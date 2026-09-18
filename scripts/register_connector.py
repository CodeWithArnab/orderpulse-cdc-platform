from __future__ import annotations

import json
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CONNECT_URL = "http://localhost:8083/connectors"


def main() -> None:
    config_path = Path(__file__).parents[1] / "infra" / "debezium-orders.json"
    payload = config_path.read_bytes()
    request = Request(
        CONNECT_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            print(json.dumps(json.load(response), indent=2))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 409:
            print("Connector already exists; no change made.")
            return
        print(f"Kafka Connect returned HTTP {exc.code}: {body}", file=sys.stderr)
        raise SystemExit(1) from exc
    except URLError as exc:
        print("Cannot reach Kafka Connect. Is docker compose healthy?", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
