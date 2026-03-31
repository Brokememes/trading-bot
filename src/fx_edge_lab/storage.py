from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from .models import AlertEvent, BasisSnapshot


class SQLiteStorage:
    def __init__(self, database_path: str) -> None:
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS gap_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    raw_futures_bid REAL NOT NULL,
                    raw_futures_ask REAL NOT NULL,
                    normalized_futures_bid REAL NOT NULL,
                    normalized_futures_ask REAL NOT NULL,
                    spot_bid REAL NOT NULL,
                    spot_ask REAL NOT NULL,
                    futures_mid REAL NOT NULL,
                    normalized_futures_mid REAL NOT NULL,
                    spot_mid REAL NOT NULL,
                    gap_price REAL NOT NULL,
                    gap_pips REAL NOT NULL,
                    threshold_pips REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    raw_futures_price REAL NOT NULL,
                    normalized_futures_price REAL NOT NULL,
                    spot_price REAL NOT NULL,
                    gap_pips REAL NOT NULL,
                    lot REAL NOT NULL,
                    sl_price REAL NOT NULL,
                    tp_price REAL NOT NULL,
                    execution_status TEXT NOT NULL DEFAULT 'NOT_SENT',
                    execution_order_id TEXT,
                    status TEXT NOT NULL DEFAULT 'OPEN',
                    closed_at TEXT,
                    close_gap_pips REAL,
                    max_gap_abs_pips REAL NOT NULL DEFAULT 0.0
                );
                """
            )
            self._connection.commit()

    def log_gap(self, snapshot: BasisSnapshot) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO gap_ticks (
                    timestamp, pair, direction, raw_futures_bid, raw_futures_ask,
                    normalized_futures_bid, normalized_futures_ask, spot_bid, spot_ask,
                    futures_mid, normalized_futures_mid, spot_mid, gap_price, gap_pips, threshold_pips
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp.isoformat(),
                    snapshot.pair,
                    snapshot.direction,
                    snapshot.raw_futures_bid,
                    snapshot.raw_futures_ask,
                    snapshot.normalized_futures_bid,
                    snapshot.normalized_futures_ask,
                    snapshot.spot_bid,
                    snapshot.spot_ask,
                    snapshot.futures_mid,
                    snapshot.normalized_futures_mid,
                    snapshot.spot_mid,
                    snapshot.gap_price,
                    snapshot.gap_pips,
                    snapshot.threshold_pips,
                ),
            )
            self._connection.commit()

    def insert_alert(self, alert: AlertEvent) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT INTO alerts (
                    timestamp, pair, direction, raw_futures_price, normalized_futures_price,
                    spot_price, gap_pips, lot, sl_price, tp_price, max_gap_abs_pips
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert.timestamp.isoformat(),
                    alert.pair,
                    alert.direction,
                    alert.raw_futures_price,
                    alert.normalized_futures_price,
                    alert.spot_price,
                    alert.gap_pips,
                    alert.lot,
                    alert.sl_price,
                    alert.tp_price,
                    abs(alert.gap_pips),
                ),
            )
            self._connection.commit()
            return int(cursor.lastrowid)

    def update_execution(self, alert_id: int, status: str, order_id: str | None) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE alerts SET execution_status = ?, execution_order_id = ? WHERE id = ?",
                (status, order_id, alert_id),
            )
            self._connection.commit()

    def update_alert_max_gap(self, alert_id: int, max_gap_abs_pips: float) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE alerts SET max_gap_abs_pips = ? WHERE id = ?",
                (max_gap_abs_pips, alert_id),
            )
            self._connection.commit()

    def close_alert(self, alert_id: int, closed_at: str, close_gap_pips: float) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE alerts
                SET status = 'GAP_CLOSED', closed_at = ?, close_gap_pips = ?
                WHERE id = ?
                """,
                (closed_at, close_gap_pips, alert_id),
            )
            self._connection.commit()

    def summary(self) -> dict[str, int]:
        with self._lock:
            gap_count = self._connection.execute("SELECT COUNT(*) FROM gap_ticks").fetchone()[0]
            alert_count = self._connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
            closed_count = self._connection.execute(
                "SELECT COUNT(*) FROM alerts WHERE status = 'GAP_CLOSED'"
            ).fetchone()[0]
        return {
            "gap_ticks": int(gap_count),
            "alerts": int(alert_count),
            "closed_alerts": int(closed_count),
        }

    def close(self) -> None:
        with self._lock:
            self._connection.close()
