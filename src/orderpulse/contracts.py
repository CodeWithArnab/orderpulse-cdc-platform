from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any


ALLOWED_STATUSES = {"CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"}
ALLOWED_OPERATIONS = {"c", "u", "d", "r"}
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


class ContractViolation(ValueError):
    """Raised when a CDC event does not satisfy the versioned data contract."""


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ContractViolation("event_time must include a timezone")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class OrderEvent:
    event_id: str
    order_id: str
    customer_id: str
    status: str
    amount: Decimal
    currency: str
    event_time: datetime
    operation: str
    source_offset: int
    contract_version: int = 1

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "OrderEvent":
        required = {
            "event_id",
            "order_id",
            "customer_id",
            "status",
            "amount",
            "currency",
            "event_time",
            "operation",
            "source_offset",
        }
        missing = sorted(required - raw.keys())
        if missing:
            raise ContractViolation(f"missing required fields: {', '.join(missing)}")

        try:
            amount = Decimal(str(raw["amount"]))
            source_offset = int(raw["source_offset"])
            contract_version = int(raw.get("contract_version", 1))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ContractViolation("amount, source_offset, or contract_version has an invalid type") from exc

        for name in ("event_id", "order_id", "customer_id"):
            if not isinstance(raw[name], str) or not raw[name].strip():
                raise ContractViolation(f"{name} must be a non-empty string")
        if raw["status"] not in ALLOWED_STATUSES:
            raise ContractViolation(f"unsupported status: {raw['status']}")
        if amount < 0:
            raise ContractViolation("amount must be non-negative")
        if not CURRENCY_PATTERN.fullmatch(str(raw["currency"])):
            raise ContractViolation("currency must be three uppercase letters")
        if raw["operation"] not in ALLOWED_OPERATIONS:
            raise ContractViolation(f"unsupported CDC operation: {raw['operation']}")
        if contract_version != 1:
            raise ContractViolation(f"unsupported contract_version: {contract_version}")

        return cls(
            event_id=raw["event_id"],
            order_id=raw["order_id"],
            customer_id=raw["customer_id"],
            status=raw["status"],
            amount=amount,
            currency=raw["currency"],
            event_time=parse_utc(str(raw["event_time"])),
            operation=raw["operation"],
            source_offset=source_offset,
            contract_version=contract_version,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "status": self.status,
            "amount": str(self.amount),
            "currency": self.currency,
            "event_time": self.event_time.isoformat().replace("+00:00", "Z"),
            "operation": self.operation,
            "source_offset": self.source_offset,
            "contract_version": self.contract_version,
        }
