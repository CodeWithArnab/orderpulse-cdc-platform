from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .pipeline import PipelineResult


def build_report(result: PipelineResult, *, generated_at: datetime | None = None) -> dict[str, Any]:
    timestamp = generated_at or datetime.now(timezone.utc)
    total = len(result.bronze)
    quarantined = len(result.quarantine)
    validity_rate = (total - result.invalid_count) / total if total else 1.0
    duplicate_rate = result.duplicate_count / total if total else 0.0
    newest = max((event.event_time for event in result.silver), default=timestamp)
    freshness_seconds = max(0.0, (timestamp - newest).total_seconds())

    slos = {
        "validity_rate_gte_99_percent": validity_rate >= 0.99,
        "duplicate_rate_lte_1_percent": duplicate_rate <= 0.01,
        "freshness_lte_300_seconds": freshness_seconds <= 300,
    }
    return {
        "generated_at": timestamp.isoformat().replace("+00:00", "Z"),
        "records": {
            "bronze": total,
            "silver": len(result.silver),
            "gold": len(result.gold),
            "quarantine": quarantined,
        },
        "metrics": {
            "validity_rate": round(validity_rate, 6),
            "duplicate_rate": round(duplicate_rate, 6),
            "freshness_seconds": round(freshness_seconds, 3),
            "late_event_count": result.late_count,
        },
        "slos": slos,
        "healthy": all(slos.values()),
    }
