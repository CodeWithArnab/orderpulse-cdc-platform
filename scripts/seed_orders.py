from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import random
import uuid


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate order inserts and updates for CDC")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--dsn", default="postgresql://orderpulse:orderpulse@localhost:5432/commerce")
    args = parser.parse_args()

    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("Install the source driver first: pip install 'psycopg[binary]>=3.2'") from exc

    rng = random.Random(42)
    currencies = ["INR", "USD", "EUR"]
    order_ids: list[uuid.UUID] = []
    with psycopg.connect(args.dsn) as connection:
        for _ in range(args.count):
            order_id = uuid.uuid4()
            order_ids.append(order_id)
            connection.execute(
                """
                INSERT INTO orders (order_id, customer_id, status, amount, currency)
                VALUES (%s, %s, 'CREATED', %s, %s)
                """,
                (
                    order_id,
                    uuid.uuid4(),
                    Decimal(rng.randrange(10000, 250000)) / 100,
                    rng.choice(currencies),
                ),
            )
        connection.commit()

        for order_id in order_ids:
            for status in ("PAID", "SHIPPED"):
                connection.execute(
                    "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                    (status, datetime.now(timezone.utc), order_id),
                )
        connection.commit()
    print(f"Created {args.count} orders and {args.count * 2} CDC updates.")


if __name__ == "__main__":
    main()
