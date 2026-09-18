from datetime import datetime, timedelta, timezone
import unittest

from orderpulse.contracts import ContractViolation, OrderEvent
from orderpulse.observability import build_report
from orderpulse.pipeline import process_events


NOW = datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc)


def event(**overrides):
    raw = {
        "event_id": "evt-1",
        "order_id": "order-1",
        "customer_id": "customer-1",
        "status": "CREATED",
        "amount": "100.00",
        "currency": "INR",
        "event_time": "2026-09-18T09:59:00Z",
        "operation": "c",
        "source_offset": 1,
        "contract_version": 1,
    }
    raw.update(overrides)
    return raw


class ContractTests(unittest.TestCase):
    def test_rejects_negative_amount(self):
        with self.assertRaisesRegex(ContractViolation, "non-negative"):
            OrderEvent.from_mapping(event(amount="-0.01"))

    def test_rejects_unknown_schema_version(self):
        with self.assertRaisesRegex(ContractViolation, "contract_version"):
            OrderEvent.from_mapping(event(contract_version=2))


class PipelineTests(unittest.TestCase):
    def test_duplicate_event_is_processed_once(self):
        raw = event()
        result = process_events([raw, dict(raw)], observed_at=NOW)
        self.assertEqual(result.duplicate_count, 1)
        self.assertEqual(len(result.silver), 1)

    def test_latest_order_state_wins_even_if_input_is_out_of_order(self):
        paid = event(
            event_id="evt-2",
            status="PAID",
            operation="u",
            event_time="2026-09-18T09:59:30Z",
            source_offset=2,
        )
        result = process_events([paid, event()], observed_at=NOW)
        self.assertEqual(result.silver[0].status, "PAID")

    def test_late_and_invalid_events_are_quarantined(self):
        late = event(event_id="evt-late", event_time="2026-09-18T09:00:00Z")
        invalid = event(event_id="evt-invalid", currency="inr")
        result = process_events([late, invalid], observed_at=NOW, watermark=timedelta(minutes=10))
        self.assertEqual(result.late_count, 1)
        self.assertEqual(result.invalid_count, 1)
        self.assertEqual(len(result.quarantine), 2)
        self.assertEqual(result.silver, [])

    def test_gold_aggregates_gmv(self):
        second = event(event_id="evt-2", order_id="order-2", amount="50.00", source_offset=2)
        result = process_events([event(), second], observed_at=NOW)
        self.assertEqual(result.gold[0]["order_count"], 2)
        self.assertEqual(result.gold[0]["gmv"], "150.00")

    def test_report_marks_unhealthy_quality(self):
        result = process_events([event(amount="-1.00")], observed_at=NOW)
        report = build_report(result, generated_at=NOW)
        self.assertFalse(report["healthy"])
        self.assertEqual(report["metrics"]["validity_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
