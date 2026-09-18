from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from .observability import build_report
from .pipeline import process_events


def sample_events(now: datetime) -> list[dict[str, Any]]:
    def iso(delta: timedelta) -> str:
        return (now + delta).isoformat().replace("+00:00", "Z")

    base = {
        "customer_id": "customer-101",
        "currency": "INR",
        "operation": "c",
        "contract_version": 1,
    }
    created = {
        **base,
        "event_id": "evt-001",
        "order_id": "order-001",
        "status": "CREATED",
        "amount": "1499.00",
        "event_time": iso(timedelta(minutes=-2)),
        "source_offset": 10,
    }
    return [
        created,
        dict(created),  # broker redelivery
        {
            **created,
            "event_id": "evt-002",
            "status": "PAID",
            "operation": "u",
            "event_time": iso(timedelta(minutes=-1)),
            "source_offset": 11,
        },
        {
            **base,
            "event_id": "evt-003",
            "order_id": "order-002",
            "status": "PAID",
            "amount": "799.00",
            "event_time": iso(timedelta(seconds=-30)),
            "source_offset": 12,
        },
        {
            **base,
            "event_id": "evt-late",
            "order_id": "order-003",
            "status": "CREATED",
            "amount": "500.00",
            "event_time": iso(timedelta(hours=-1)),
            "source_offset": 7,
        },
        {
            **base,
            "event_id": "evt-bad",
            "order_id": "order-004",
            "status": "CREATED",
            "amount": "-25.00",
            "event_time": iso(timedelta(seconds=-5)),
            "source_offset": 13,
        },
    ]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def run(output: Path) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    result = process_events(sample_events(now), observed_at=now)
    report = build_report(result, generated_at=now)
    write_jsonl(output / "bronze/events.jsonl", result.bronze)
    write_jsonl(output / "silver/orders.jsonl", [event.as_dict() for event in result.silver])
    write_jsonl(output / "gold/orders_by_minute.jsonl", result.gold)
    write_jsonl(output / "quarantine/rejected_events.jsonl", result.quarantine)
    report_path = output / "reports/pipeline_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the deterministic OrderPulse CDC demo")
    parser.add_argument("--output", type=Path, default=Path("build/demo"))
    args = parser.parse_args()
    report = run(args.output)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
