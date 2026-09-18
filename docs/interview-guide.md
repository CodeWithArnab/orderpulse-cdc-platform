# OrderPulse interview guide

This guide explains OrderPulse from first principles. Learn the story and reasoning rather than memorizing tool definitions.

## 1. The thirty-second explanation

> OrderPulse is a real-time data engineering project for an e-commerce company. When an order is inserted or updated in PostgreSQL, Debezium captures that database change and publishes it to Kafka. PySpark Structured Streaming reads those events, keeps the original data in Bronze, validates and deduplicates it into Silver, and creates business summaries in Gold. Invalid or very late events go to quarantine. The project also reports freshness, validity, and duplicate-rate SLOs and includes automated tests and CI.

If an interviewer understands that answer, the rest of the conversation is about individual design decisions.

## 2. What business problem does it solve?

Imagine an e-commerce application. Its PostgreSQL `orders` table is designed for the application to create and update orders. Analysts and dashboards should not run large analytical queries directly against this operational database because those queries can slow down customer-facing transactions.

The business still wants quick answers:

- How many orders are being created?
- What is the order value by minute?
- How many orders are paid, shipped, delivered, or cancelled?
- Is the analytics data fresh and trustworthy?
- What happened when a record was invalid or arrived twice?

OrderPulse moves changes from the operational system into an analytical pipeline without repeatedly scanning the whole table.

## 3. Architecture in plain English

```text
Application
    |
    | INSERT / UPDATE / DELETE
    v
PostgreSQL orders table
    |
    | transaction log (WAL)
    v
Debezium connector
    |
    | CDC event
    v
Kafka topic
    |
    | continuous consumption
    v
PySpark Structured Streaming
    |              |                |
    v              v                v
Bronze          Silver           Quarantine
raw events      valid data       rejected data
                   |
                   v
                 Gold
             business metrics
```

### PostgreSQL

PostgreSQL is the source database used by the order application. It contains the current operational state of each order.

### WAL

WAL means Write-Ahead Log. Before PostgreSQL changes a database page, it records the change in its transaction log. PostgreSQL uses this log for durability and recovery. CDC tools can also read it to discover committed changes.

### Debezium

Debezium is the CDC connector. CDC means Change Data Capture. Instead of asking PostgreSQL, "Which rows changed?" every few seconds, Debezium reads the database log and emits an event for each insert, update, and delete.

### Kafka

Kafka is the durable event transport. Producers write events to topics, and consumers read them at their own speed. A Kafka topic is split into partitions. Every record within a partition receives an increasing offset.

### PySpark Structured Streaming

Spark reads Kafka continuously in small micro-batches. It parses the Debezium JSON, checks the data contract, removes duplicate deliveries, handles event time, and writes analytical datasets.

### Bronze, Silver, and Gold

- **Bronze:** original events and Kafka metadata. This layer is replayable and useful for debugging.
- **Silver:** contract-valid, typed, deduplicated data suitable for engineering and analysis.
- **Gold:** business-facing aggregates such as order counts and observed value by minute.
- **Quarantine:** invalid or excessively late records, together with the rejection reason.

This arrangement is commonly called the medallion architecture.

## 4. Follow one order through the pipeline

Suppose the application inserts this order:

```text
order_id: order-101
customer_id: customer-8
status: CREATED
amount: 1499.00
currency: INR
```

1. PostgreSQL commits the insert and records it in WAL.
2. Debezium reads the WAL entry.
3. Debezium publishes a create event with operation `c` to `orderpulse.public.orders`.
4. Kafka stores it at a particular topic, partition, and offset.
5. Spark creates a stable `event_id` from those broker coordinates.
6. Bronze stores the original payload before business validation.
7. Spark checks required fields, status, amount, currency, operation, timestamp, and contract version.
8. If valid and not a duplicate, it reaches Silver.
9. Spark groups valid events into one-minute windows to produce Gold metrics.
10. The SLO report calculates validity, duplicate rate, and freshness.

Later, when the order becomes `PAID`, the same path processes an update event with operation `u`.

## 5. Important data-engineering concepts

### CDC versus polling

**Polling approach:** run a query such as `WHERE updated_at > last_run_time` every minute.

Problems with polling:

- it repeatedly queries the source database;
- deletes are difficult to capture;
- timestamp boundaries can miss or duplicate changes;
- latency depends on the polling interval.

CDC reads committed changes from the log. It captures inserts, updates, and deletes with source ordering metadata. Its trade-off is additional operational complexity: replication slots, connector health, WAL retention, and schema evolution must be monitored.

### Event time versus processing time

- **Event time:** when the business event happened.
- **Processing time:** when Spark processed it.

If the pipeline is temporarily delayed, processing time can be much later. Business windows should therefore use event time.

### Watermark

A watermark tells Spark how long it should wait for late events while maintaining streaming state. OrderPulse uses ten minutes.

An event arriving three minutes late is accepted. An event arriving one hour late is sent to quarantine/reconciliation in the local model. The correct watermark depends on real delay patterns and business requirements.

### Duplicate delivery and idempotency

Distributed systems can deliver a record more than once during retries. OrderPulse derives a deterministic event identity from Kafka topic, partition, and offset and removes duplicate event identities.

Idempotent means that processing the same event again does not change the final result after it has already been applied.

### At-least-once and exactly-once

Kafka and Spark checkpoints help track progress, but saying "exactly once" requires care. End-to-end effective exactly-once behavior also needs a stable event identity and an idempotent or transactional sink.

The local Parquet output demonstrates transformations but is not a complete transactional lakehouse. The production roadmap replaces it with Delta Lake, Iceberg, or BigQuery merge semantics. Say this honestly in interviews.

### Checkpoint

A Spark checkpoint stores processed offsets and state. If the job restarts, it can continue rather than reading everything from the beginning. Ordinary restarts should preserve checkpoints. Deleting them deliberately causes a clean replay.

### Data contract

A data contract defines what a valid event looks like. OrderPulse checks:

- required identifiers are present;
- status belongs to an allowed set;
- amount is non-negative;
- currency contains three uppercase letters;
- operation is a supported CDC operation;
- event time includes a timezone;
- contract version is supported.

Failing records are preserved in quarantine instead of silently disappearing.

### Schema evolution

An additive change, such as a nullable `promotion_code`, can be handled compatibly. A breaking change, such as changing `amount` from decimal to arbitrary text, should fail compatibility checks and require a coordinated version change.

In an advanced version, Avro or Protobuf schemas would be stored in Schema Registry and checked in CI.

### SLO

SLO means Service Level Objective. OrderPulse demonstrates:

- validity rate at least 99%;
- duplicate rate no more than 1%;
- freshness no more than five minutes.

An SLO turns "the pipeline seems fine" into something measurable. A real alert should also include a time window and an actionable response.

## 6. Repository walkthrough

### `src/orderpulse/contracts.py`

Defines the versioned order-event contract and validation. Start here when explaining what data is allowed into trusted layers.

### `src/orderpulse/pipeline.py`

Contains the deterministic local implementation of validation, deduplication, watermark behavior, latest-order selection, and aggregation. It is intentionally free of infrastructure dependencies, which makes its behavior easy to unit test.

### `src/orderpulse/observability.py`

Calculates record counts, freshness, validity rate, duplicate rate, SLO results, and overall health.

### `src/orderpulse/demo.py`

Generates a small scenario containing valid, duplicate, late, and invalid events. It writes the Bronze, Silver, Gold, quarantine, and report files used by the dashboard.

### `jobs/stream_orders.py`

Maps the concepts to PySpark Structured Streaming. It reads Kafka, preserves Bronze, parses the Debezium envelope, validates fields, applies an event-time watermark, deduplicates, and writes the output streams.

### `infra/docker-compose.yml`

Defines the local PostgreSQL, Redpanda, Kafka Connect/Debezium, and Redpanda Console services.

Redpanda is Kafka API-compatible and keeps the local environment lighter. The design still uses Kafka concepts and Spark's Kafka connector.

### `infra/debezium-orders.json`

Configures the PostgreSQL connector, source table, topic prefix, `pgoutput` logical decoding, replication slot, initial snapshot, and JSON serialization.

### `sql/init.sql`

Creates the source `orders` table, constraints, indexes, and full replica identity needed to represent updates/deletes completely.

### `tests/test_pipeline.py`

Tests negative amounts, incompatible contract versions, broker redelivery, out-of-order updates, late events, Gold aggregation, and unhealthy SLO behavior.

### `.github/workflows/ci.yml`

Runs tests and the demo for pushes and pull requests. This is evidence that the repository is reproducible and not merely a notebook.

## 7. How to demonstrate it

### Beginner demo

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

Show these dashboard sections:

1. the source-to-serving architecture;
2. six raw Bronze records;
3. two current valid Silver orders;
4. the Gold business output;
5. two quarantined records;
6. failed duplicate and validity SLOs.

Explain that the unhealthy result is intentional because the demo injects failure.

### Full-system demo

After installing Docker, follow `docs/runbook.md`:

1. start PostgreSQL, Redpanda, Connect, and Console;
2. register Debezium;
3. create and update orders;
4. inspect CDC envelopes in the Kafka console;
5. run the Spark consumer;
6. show output directories and checkpoints;
7. restart the connector or Spark job and discuss recovery.

## 8. Interview presentation scripts

### Three-minute version

> I built OrderPulse to demonstrate production data-engineering concerns beyond a normal batch ETL project. The source is an operational PostgreSQL orders table. Debezium reads committed changes from PostgreSQL WAL and publishes insert, update, and delete events to Kafka. PySpark Structured Streaming consumes those events using event time. I preserve the raw payload in Bronze, validate and deduplicate it for Silver, and produce minute-level business metrics in Gold. Invalid or excessively late events go to quarantine with a reason, so nothing disappears silently. I added measurable SLOs for validity, duplicate rate, and freshness, plus unit tests and CI. The local Parquet implementation demonstrates behavior; for production I would use Delta Lake, Iceberg, or BigQuery MERGE for transactional current-state updates, Schema Registry for compatibility, and Prometheus/Grafana for operational metrics.

### Ten-minute version

1. State the business problem.
2. Draw the source-to-Gold architecture.
3. Follow one create event and one update event.
4. Explain Bronze, Silver, Gold, and quarantine.
5. Demonstrate a duplicate, late event, and invalid record.
6. Explain event time, watermark, checkpoints, and idempotency.
7. Show tests and CI.
8. Describe one alternative—polling—and why CDC was selected.
9. Acknowledge the Parquet limitation.
10. Finish with the transactional/cloud roadmap and benchmark plan.

## 9. Common interviewer questions and answers

### Why did you choose CDC?

CDC captures inserts, updates, and deletes without repeatedly scanning the source table. It provides lower-latency change events and source ordering information. The cost is more operational complexity around replication slots, WAL growth, connector monitoring, and schema changes.

### Why use Kafka between Debezium and Spark?

Kafka decouples source capture from processing. Debezium can continue publishing while Spark is temporarily unavailable, within retention limits. Multiple consumers can independently read the same event stream, and offsets support replay.

### Why not send changes directly to Spark?

Direct coupling makes outages harder to absorb and replay harder to manage. Kafka acts as a durable buffer and allows independent scaling and recovery.

### Why preserve Bronze if Kafka already stores the events?

Kafka retention is finite and optimized for streaming. Bronze provides longer-lived, queryable, source-aligned history for audit, replay, and backfills. Its retention policy can differ from Kafka's.

### How do you handle duplicates?

I generate a stable identity using Kafka topic, partition, and offset. Spark deduplicates that identity within the watermark boundary. A production sink must also apply changes idempotently so a retried micro-batch cannot create duplicate state.

### How do you handle late data?

Aggregations use event time with a ten-minute watermark. Events within the bound update the relevant window. Events beyond the accepted bound require a quarantine or reconciliation path. I would choose the real watermark using observed lateness percentiles and business tolerance.

### How do you handle out-of-order updates?

The local current-state model compares event time and source offset so an older update cannot replace a newer state. In production, the Silver `MERGE` condition would update an order only when the incoming source position is newer.

### What happens when Spark crashes?

Spark restarts from its checkpoint, which contains offsets and streaming state. Kafka retains unprocessed messages. The sink must be transactional or idempotent because the last micro-batch may be retried.

### What happens when Debezium crashes?

Kafka Connect stores connector offsets. On restart, Debezium continues from its recorded WAL position. The replication slot prevents PostgreSQL from discarding WAL still needed by the connector. That also means connector downtime can cause WAL disk growth and must be monitored.

### How do you handle deletes?

Debezium emits operation `d`. Bronze preserves it. A production Silver merge would mark the entity deleted or remove it, depending on retention and audit requirements. Analytical facts are normally not physically deleted without a governance decision.

### How would you handle schema changes?

I would use Avro or Protobuf with Schema Registry, enforce backward-compatible changes, test consumer compatibility in CI, and route unsupported contract versions to quarantine. Database DDL and downstream data-contract deployment should be coordinated.

### What partition key would you use?

For order changes, `order_id` is a reasonable Kafka key because all changes for one order then remain ordered in the same partition. A poor key can create hot partitions, so I would inspect key distribution and throughput.

### Can Kafka guarantee global ordering?

No. Kafka guarantees ordering within a partition, not across all partitions. Business entities that require ordered changes should use a consistent partition key.

### How would you scale the pipeline?

Increase Kafka partitions, give Spark enough executors/cores, tune batch interval and shuffle partitions, avoid unnecessary shuffles, partition storage by useful low-cardinality fields and dates, compact small files, and monitor processing time versus input rate. Scaling must be based on measurements.

### How would you prevent small files?

Control output partitioning, compact files periodically, and use a table format with optimization/compaction capabilities. Partitioning by a high-cardinality identifier such as `order_id` would be a mistake.

### How would you secure it?

Use TLS and authentication for Kafka, encrypted database connections, least-privilege service accounts, secrets from a secret manager, encrypted storage, private networking, topic/table access controls, masked sensitive fields, audit logs, and retention policies. No credentials should be committed to Git.

### How would you test it?

- unit tests for contract and transformation rules;
- integration tests with containers and real CDC events;
- schema compatibility tests;
- reconciliation tests between source and Silver;
- restart and replay tests;
- performance and soak tests;
- data-quality and freshness assertions after deployment.

### Why PySpark instead of Flink?

PySpark matches my professional experience and supports both batch and streaming transformations with the same DataFrame model. Flink can be a better choice for very low latency and complex stateful streaming. The correct choice depends on latency, state, team expertise, and operational environment.

### Why is the demo deliberately unhealthy?

Healthy-only demos prove little about reliability. The poisoned events demonstrate that duplicate, freshness, and validation controls work and that bad records are explainable instead of silently lost.

### What is the biggest current limitation?

The local curated sink uses Parquet and does not provide a transactional CDC merge. The next priority is Delta Lake, Iceberg, or BigQuery current-state merge semantics, followed by end-to-end integration tests and measured benchmarks.

### Did this process ten million production events?

No. Do not claim a scale that has not been benchmarked. The current project demonstrates correctness and architecture. I would publish throughput only after recording a repeatable workload, environment, raw result, and methodology.

## 10. How to showcase it professionally

### GitHub repository

Use the repository name `orderpulse-cdc-platform` and description:

> Production-style CDC data platform using PostgreSQL, Debezium, Kafka, PySpark, data contracts, quarantine, SLOs, tests, and CI.

Before sharing:

1. replace any placeholder profile links;
2. run the visual and full-system demos;
3. keep the README architecture diagram near the top;
4. add one dashboard screenshot and one Kafka CDC screenshot;
5. ensure GitHub Actions is green;
6. tag a release such as `v0.1.0`;
7. add a short demo video;
8. keep limitations and future work honest.

### LinkedIn Featured section

Use this text:

> I built OrderPulse to explore the reliability problems behind real-time data platforms—not just the happy-path ETL. It captures PostgreSQL changes using Debezium and Kafka, processes them with PySpark Structured Streaming, and produces Bronze, Silver, Gold, and quarantine datasets. The project demonstrates event-time watermarks, deduplication, versioned data contracts, replayable checkpoints, measurable data-quality SLOs, automated tests, and CI.

Attach the GitHub URL, dashboard screenshot, and a 60–90 second demo video.

### Resume entry

> **OrderPulse - CDC Commerce Data Platform** | Python, PySpark, Kafka, Debezium, PostgreSQL, Docker  
> Built a CDC-driven data platform that captures transactional changes through Debezium and Kafka and processes them into Bronze, Silver, Gold, and quarantine datasets using PySpark Structured Streaming. Implemented event-time watermarks, deterministic deduplication, versioned data contracts, checkpoint-based recovery, measurable data-quality SLOs, automated tests, and CI.

Add benchmark numbers only after completing the benchmark stage.

## 11. A practical learning order

Do not try to memorize everything at once.

### Day 1: understand the story

- business problem;
- source, CDC, Kafka, Spark, and outputs;
- Bronze/Silver/Gold;
- run the visual demo.

### Day 2: understand reliability

- duplicate delivery;
- idempotency;
- event time and watermarks;
- checkpoints and replay;
- quarantine.

### Day 3: understand infrastructure

- PostgreSQL WAL and replication slots;
- Kafka topics, partitions, offsets, and keys;
- Debezium create/update/delete envelopes;
- Spark micro-batches.

### Day 4: read the code

- contract;
- pipeline;
- tests;
- Spark job;
- connector configuration.

### Day 5: practice communication

- record the three-minute explanation;
- draw the architecture without notes;
- answer the common questions aloud;
- be explicit about limitations and next steps.

## 12. Final rule for interviews

Do not pretend every component is already production-deployed. A strong answer distinguishes:

- what is implemented and tested;
- what is configured but not yet run on this computer;
- what would change in production; and
- which measurements have not yet been collected.

That honesty, combined with clear trade-off reasoning, makes the project more credible—not less.
