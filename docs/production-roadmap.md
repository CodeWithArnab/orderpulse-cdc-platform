# Production-hardening roadmap

The portfolio should grow in evidence-backed stages. A large tool list is less convincing than a smaller system with measured reliability.

## Stage 1 - Reproducible local system

Already present:

- pinned container versions;
- log-based CDC with PostgreSQL `pgoutput`;
- immutable Bronze payloads;
- versioned data contract and quarantine;
- event-time watermark and deterministic deduplication;
- unit tests, CI, architecture decisions, and a visual demo.

Exit criteria:

- the full Docker path starts from a fresh clone;
- one documented command produces CDC records;
- connector restart resumes from its stored WAL position;
- screenshots and a three-minute demo video are published.

## Stage 2 - Transactional lakehouse

Replace local Silver/Gold Parquet sinks with Delta Lake or Apache Iceberg.

- MERGE CDC changes by `order_id` and source offset;
- preserve delete semantics;
- make micro-batch retries idempotent;
- support point-in-time queries and rollback;
- run reconciliation between source counts and curated tables.

Exit criteria:

- rerunning a micro-batch produces no duplicates;
- an out-of-order update cannot overwrite newer state;
- source-to-Silver reconciliation is automated.

## Stage 3 - Data contracts and observability

- publish schemas in a registry using Avro or Protobuf;
- add compatibility checks to CI;
- export Kafka lag and Spark progress metrics to Prometheus;
- visualize freshness, throughput, invalid-rate, and p95 latency in Grafana;
- emit OpenLineage events to Marquez or OpenMetadata;
- add actionable alerts and a short incident runbook.

Exit criteria:

- a breaking schema change is rejected before deployment;
- a stopped consumer triggers a freshness alert;
- lineage connects source table, Kafka topic, Spark job, and Gold table.

## Stage 4 - Cloud and infrastructure as code

Recommended for Arnab's résumé: deploy on GCP first.

- Cloud SQL PostgreSQL as the source;
- Kafka-compatible service or Pub/Sub adaptation;
- Dataproc Serverless for PySpark;
- GCS for Bronze and BigQuery for serving;
- Secret Manager for credentials;
- Terraform modules for dev infrastructure;
- GitHub Actions with workload identity federation—no long-lived cloud keys.

Exit criteria:

- a clean environment is created and destroyed through Terraform;
- least-privilege service accounts are documented;
- deployment is automated and secrets never enter Git.

## Stage 5 - Benchmark and proof

Create a repeatable workload generator and record:

- events per second and total events;
- p50/p95/p99 end-to-end latency;
- duplicate and invalid-record rates;
- recovery time after connector and Spark restarts;
- Spark batch duration, shuffle volume, and state-store size;
- cloud cost per million events.

Publish the command, machine/cloud configuration, raw result file, and methodology. Only then add numerical claims to the résumé.
