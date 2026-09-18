# Three-minute recruiter demo

## 0:00-0:30 - State the problem

"OrderPulse turns changes in an operational order database into trustworthy, near-real-time business metrics. I focused on the failure cases that production data pipelines face: duplicates, late data, incompatible schemas, bad records, and replay."

## 0:30-1:15 - Show the architecture

Open the README diagram. Follow one order from PostgreSQL WAL to Debezium, Kafka, Spark, and the three data layers. Explain that Bronze is immutable, Silver is contract-valid and deduplicated, and Gold is business-facing.

## 1:15-2:10 - Run the proof

```bash
python -m orderpulse.demo --output build/demo
python -m unittest discover -s tests -v
```

Open `build/demo/reports/pipeline_report.json`. Point out the duplicate, late event, invalid amount, freshness calculation, and failing SLOs. Explain that unhealthy is the correct result for deliberately poisoned demo data.

## 2:10-2:40 - Explain one trade-off

"I used log-based CDC instead of polling because it captures deletes, preserves offsets, and avoids repeatedly scanning the source table. The trade-off is more operational complexity around replication slots and connector lag."

## 2:40-3:00 - Close with ownership

"The next production step is deploying the same job on Dataproc, storing immutable data in GCS, publishing curated tables to BigQuery, and recording throughput, p95 end-to-end latency, and cost per million events."

## Do not claim yet

- Do not say "exactly once" without explaining the sink and idempotency boundary.
- Do not state events-per-day, latency, or cost savings until a repeatable benchmark exists.
- Do not call the project production-deployed until it is running outside a laptop.
