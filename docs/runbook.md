# OrderPulse runbook

## Mode A: visual demo (recommended first)

Requirements: Windows PowerShell and Python 3.11+.

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

The command:

1. runs all correctness tests;
2. generates raw, trusted, aggregated, and quarantined datasets;
3. starts a local web server; and
4. opens `http://localhost:8000/dashboard/`.

Press `Ctrl+C` in PowerShell to stop it. If port 8000 is occupied:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1 -Port 8088
```

## Mode B: full CDC platform

### Prerequisites

- Docker Desktop using the WSL 2 backend
- Python 3.11+
- Java 17 and Apache Spark 3.5.x for the Spark consumer
- At least 8 GB of memory available to Docker

### Start infrastructure

```powershell
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

Wait until PostgreSQL and Redpanda report healthy, then register the CDC connector:

```powershell
python scripts/register_connector.py
curl http://localhost:8083/connectors/orderpulse-postgres-orders/status
```

Open Redpanda Console at `http://localhost:8080`. The connector creates the topic `orderpulse.public.orders` after the initial source snapshot.

### Generate database changes

```powershell
python -m pip install -e ".[source]"
python scripts/seed_orders.py --count 1000
```

The seed script creates 1,000 orders and then performs 2,000 updates. In the console, inspect the topic and compare create (`c`) and update (`u`) envelopes.

### Run Spark

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 `
  jobs/stream_orders.py
```

Spark writes local outputs under `data/` and checkpoints under `build/checkpoints/`. Keep checkpoints between ordinary restarts; delete them only when deliberately performing a clean replay.

### Stop cleanly

```powershell
docker compose -f infra/docker-compose.yml down
```

This keeps the PostgreSQL volume. To remove it too, use `down --volumes` only when you intentionally want a clean database.

## Operational checks

```powershell
curl http://localhost:8083/connectors/orderpulse-postgres-orders/status
docker compose -f infra/docker-compose.yml logs --tail 100 connect
docker compose -f infra/docker-compose.yml logs --tail 100 redpanda
```

Production alerts should cover connector failure, replication-slot/WAL growth, consumer lag, Spark batch duration, checkpoint failures, invalid-record rate, and data freshness.
