from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .engine import BasisArbitrageEngine
from .models import MarketQuote


@dataclass(frozen=True)
class ReplayRow:
    timestamp: datetime
    pair: str
    spot_bid: float
    spot_ask: float
    futures_bid: float
    futures_ask: float


def load_replay_rows(path: str | Path) -> list[ReplayRow]:
    rows: list[ReplayRow] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                ReplayRow(
                    timestamp=_parse_timestamp(row["timestamp"]),
                    pair=row["pair"],
                    spot_bid=float(row["spot_bid"]),
                    spot_ask=float(row["spot_ask"]),
                    futures_bid=float(row["futures_bid"]),
                    futures_ask=float(row["futures_ask"]),
                )
            )
    rows.sort(key=lambda item: item.timestamp)
    return rows


def replay_rows(rows: list[ReplayRow], engine: BasisArbitrageEngine) -> None:
    for row in rows:
        engine.on_futures_quote(
            MarketQuote(row.pair, f"{row.pair}_FUT", row.futures_bid, row.futures_ask, row.timestamp, "replay-futures")
        )
        engine.on_spot_quote(
            MarketQuote(row.pair, f"{row.pair}_SPOT", row.spot_bid, row.spot_ask, row.timestamp, "replay-spot")
        )


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
