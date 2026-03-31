from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from .dxy import DxySnapshot

REQUIRED_COLUMNS = {
    "timestamp",
    "dxy",
    "eurusd",
    "usdjpy",
    "gbpusd",
    "usdcad",
    "usdsek",
    "usdchf",
    "eurusd_future",
    "usd_rate",
    "eur_rate",
    "days_to_expiry",
}


@dataclass(frozen=True)
class MarketRow(DxySnapshot):
    eurusd_future: float
    usd_rate: float
    eur_rate: float
    days_to_expiry: float


def load_market_rows(path: str | Path) -> list[MarketRow]:
    csv_path = Path(path)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - fieldnames)
        if missing:
            missing_display = ", ".join(missing)
            raise ValueError(f"missing required columns: {missing_display}")

        rows: list[MarketRow] = []
        for raw_row in reader:
            rows.append(
                MarketRow(
                    timestamp=raw_row["timestamp"],
                    dxy=float(raw_row["dxy"]),
                    eurusd=float(raw_row["eurusd"]),
                    usdjpy=float(raw_row["usdjpy"]),
                    gbpusd=float(raw_row["gbpusd"]),
                    usdcad=float(raw_row["usdcad"]),
                    usdsek=float(raw_row["usdsek"]),
                    usdchf=float(raw_row["usdchf"]),
                    eurusd_future=float(raw_row["eurusd_future"]),
                    usd_rate=float(raw_row["usd_rate"]),
                    eur_rate=float(raw_row["eur_rate"]),
                    days_to_expiry=float(raw_row["days_to_expiry"]),
                )
            )
    return rows


def export_enriched_rows(path: str | Path, rows: list[dict[str, str | float]]) -> None:
    csv_path = Path(path)
    if not rows:
        raise ValueError("no rows to export")

    fieldnames = list(rows[0].keys())
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
