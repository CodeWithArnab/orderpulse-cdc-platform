# Architecture decisions

## ADR-001: Capture changes from the database log

**Decision:** use PostgreSQL logical replication through Debezium instead of polling `updated_at`.

**Why:** log-based CDC captures deletes, avoids repeated table scans, preserves source ordering metadata, and reduces load on the operational database. Polling is simpler, but it can miss same-timestamp updates and deletes and becomes expensive at scale.

## ADR-002: Preserve an immutable Bronze layer

**Decision:** store the original Kafka payload plus broker metadata before applying business rules.

**Why:** bad records must be debuggable and transformations must be replayable. Silver is not a sufficient audit log because validation and deduplication intentionally discard information.

## ADR-003: Separate transport identity from business identity

**Decision:** derive `event_id` from topic, partition, and offset; keep `order_id` as the business key.

**Why:** multiple valid updates can share an `order_id`. Deduplicating only on the business key would silently lose state transitions. Broker coordinates provide a deterministic identity for replayed Kafka records.

## ADR-004: Use event time and a bounded watermark

**Decision:** aggregate on `event_time` with a ten-minute watermark.

**Why:** processing time produces misleading business metrics during backlogs. A bounded watermark controls streaming state while accepting ordinary network delay. Events beyond the bound are counted and quarantined for an explicit reconciliation path.

## ADR-005: Fail incompatible contracts closed

**Decision:** unsupported contract versions and invalid business fields go to quarantine.

**Why:** silently coercing unknown schemas can corrupt trusted datasets. Quarantine preserves the payload and reason while keeping the main pipeline available.

## Delivery semantics

Kafka and Spark checkpoints provide at-least-once transport. End-to-end effective exactly-once behavior requires:

1. a stable event identity;
2. deterministic transformations;
3. checkpointed offsets;
4. an idempotent or transactional sink; and
5. retention long enough to replay from Bronze.

The local Parquet sink is intentionally educational. A production deployment should use Delta Lake, Iceberg, BigQuery Storage Write API, or another sink with transactional/idempotent writes.

## Cloud mapping

| Local component | GCP deployment | AWS deployment |
|---|---|---|
| PostgreSQL | Cloud SQL | RDS/Aurora |
| Redpanda/Kafka | Managed Kafka or Pub/Sub adapter | MSK |
| Spark | Dataproc | EMR/Glue |
| Bronze/Silver/Gold | GCS + BigQuery/BigLake | S3 + Iceberg/Redshift |
| Metrics | Cloud Monitoring | CloudWatch |
| Secrets | Secret Manager | Secrets Manager |
