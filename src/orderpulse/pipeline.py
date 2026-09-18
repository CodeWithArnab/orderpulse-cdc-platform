from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

from .contracts import ContractViolation, OrderEvent


@dataclass(slots=True)
class PipelineResult:
    bronze: list[dict[str, Any]]
    silver: list[OrderEvent]
    gold: list[dict[str, Any]]
    quarantine: list[dict[str, Any]]
    invalid_count: int
    duplicate_count: int
    late_count: int


def process_events(
    raw_events: Iterable[dict[str, Any]],
    *,
    observed_at: datetime | None = None,
    watermark: timedelta = timedelta(minutes=10),
) -> PipelineResult:
    """Apply the same contract, dedupe, watermark, and aggregation semantics as the Spark job."""
    now = observed_at or datetime.now(timezone.utc)
    bronze: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    candidates: list[OrderEvent] = []
    invalid_count = 0

    for raw in raw_events:
        bronze.append(dict(raw))
        try:
            candidates.append(OrderEvent.from_mapping(raw))
        except (ContractViolation, ValueError) as exc:
            invalid_count += 1
            quarantine.append({"event": dict(raw), "reason": str(exc)})

    by_event_id: dict[str, OrderEvent] = {}
    duplicate_count = 0
    for event in candidates:
        existing = by_event_id.get(event.event_id)
        if existing is not None:
            duplicate_count += 1
        if existing is None or (event.event_time, event.source_offset) > (
            existing.event_time,
            existing.source_offset,
        ):
            by_event_id[event.event_id] = event

    cutoff = now - watermark
    on_time: list[OrderEvent] = []
    late_count = 0
    for event in by_event_id.values():
        if event.event_time < cutoff:
            late_count += 1
            quarantine.append({"event": event.as_dict(), "reason": "event exceeded watermark"})
        else:
            on_time.append(event)

    latest_by_order: dict[str, OrderEvent] = {}
    for event in on_time:
        current = latest_by_order.get(event.order_id)
        if current is None or (event.event_time, event.source_offset) > (
            current.event_time,
            current.source_offset,
        ):
            latest_by_order[event.order_id] = event

    silver = sorted(latest_by_order.values(), key=lambda item: item.order_id)
    aggregates: dict[tuple[datetime, str, str], dict[str, Any]] = defaultdict(
        lambda: {"order_count": 0, "gmv": Decimal("0")}
    )
    for event in silver:
        if event.operation == "d":
            continue
        minute = event.event_time.replace(second=0, microsecond=0)
        key = (minute, event.status, event.currency)
        aggregates[key]["order_count"] += 1
        aggregates[key]["gmv"] += event.amount

    gold = [
        {
            "minute": minute.isoformat().replace("+00:00", "Z"),
            "status": status,
            "currency": currency,
            "order_count": values["order_count"],
            "gmv": str(values["gmv"]),
        }
        for (minute, status, currency), values in sorted(aggregates.items())
    ]
    return PipelineResult(
        bronze=bronze,
        silver=silver,
        gold=gold,
        quarantine=quarantine,
        invalid_count=invalid_count,
        duplicate_count=duplicate_count,
        late_count=late_count,
    )
