from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .crypto_models import (
    BasisObservation,
    FundingSnapshot,
    MarketTrade,
    OpenInterestSnapshot,
    OrderBookSnapshot,
    SignalEvent,
)


class CryptoSQLiteStorage:
    def __init__(self, database_path: str) -> None:
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA temp_store = MEMORY;

                CREATE TABLE IF NOT EXISTS crypto_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    bid_price REAL NOT NULL,
                    bid_size REAL NOT NULL,
                    ask_price REAL NOT NULL,
                    ask_size REAL NOT NULL,
                    mid_price REAL NOT NULL,
                    spread_bps REAL NOT NULL,
                    imbalance REAL NOT NULL,
                    bid_depth_notional REAL NOT NULL,
                    ask_depth_notional REAL NOT NULL,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS crypto_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    taker_side TEXT NOT NULL,
                    trade_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS crypto_funding (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    current_funding_rate REAL,
                    average_funding_rate REAL,
                    next_funding_time TEXT,
                    basis_rate REAL,
                    basis_value REAL
                );

                CREATE TABLE IF NOT EXISTS crypto_open_interest (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_interest REAL NOT NULL,
                    open_interest_value REAL
                );

                CREATE TABLE IF NOT EXISTS crypto_basis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    spot_venue TEXT NOT NULL,
                    spot_symbol TEXT NOT NULL,
                    perp_venue TEXT NOT NULL,
                    perp_symbol TEXT NOT NULL,
                    spot_mid REAL NOT NULL,
                    perp_mid REAL NOT NULL,
                    premium_bps REAL NOT NULL,
                    spot_imbalance REAL NOT NULL,
                    perp_imbalance REAL NOT NULL,
                    current_funding_rate REAL,
                    average_funding_rate REAL,
                    next_funding_time TEXT,
                    basis_rate REAL,
                    basis_value REAL
                );

                CREATE TABLE IF NOT EXISTS crypto_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    signal_source TEXT NOT NULL,
                    entry_bid REAL NOT NULL,
                    entry_ask REAL NOT NULL,
                    entry_mid REAL NOT NULL,
                    spread_bps REAL NOT NULL,
                    imbalance REAL NOT NULL,
                    threshold REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS crypto_signal_markouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER NOT NULL,
                    horizon_ms INTEGER NOT NULL,
                    marked_at TEXT NOT NULL,
                    markout_bps REAL NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES crypto_signals(id)
                );

                CREATE TABLE IF NOT EXISTS crypto_paper_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    limit_price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    status TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    filled_at TEXT,
                    fill_price REAL,
                    fill_trade_id TEXT,
                    fill_latency_ms REAL,
                    FOREIGN KEY(signal_id) REFERENCES crypto_signals(id)
                );

                CREATE TABLE IF NOT EXISTS crypto_paper_markouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    horizon_ms INTEGER NOT NULL,
                    marked_at TEXT NOT NULL,
                    markout_bps REAL NOT NULL,
                    FOREIGN KEY(order_id) REFERENCES crypto_paper_orders(id)
                );

                CREATE TABLE IF NOT EXISTS crypto_spread_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    status TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_spot_price REAL NOT NULL,
                    entry_perp_price REAL NOT NULL,
                    entry_basis_bps REAL NOT NULL,
                    basis_threshold_bps REAL NOT NULL,
                    entry_funding_rate REAL,
                    signal_quality_score REAL,
                    signal_quality_band TEXT NOT NULL,
                    spot_imbalance REAL NOT NULL,
                    perp_imbalance REAL NOT NULL,
                    exit_timestamp TEXT,
                    exit_reason TEXT,
                    exit_spot_price REAL,
                    exit_perp_price REAL,
                    exit_basis_bps REAL,
                    exit_funding_rate REAL,
                    gross_edge_pct REAL,
                    gross_pnl_quote REAL,
                    net_pnl_quote REAL,
                    hold_ms REAL,
                    FOREIGN KEY(signal_id) REFERENCES crypto_signals(id)
                );

                CREATE INDEX IF NOT EXISTS idx_crypto_basis_pair_timestamp
                ON crypto_basis(pair, timestamp);

                CREATE INDEX IF NOT EXISTS idx_crypto_basis_timestamp
                ON crypto_basis(timestamp);

                CREATE INDEX IF NOT EXISTS idx_crypto_signals_pair_source_timestamp
                ON crypto_signals(pair, signal_source, timestamp);

                CREATE INDEX IF NOT EXISTS idx_crypto_spread_positions_signal_id
                ON crypto_spread_positions(signal_id);

                CREATE INDEX IF NOT EXISTS idx_crypto_funding_pair_timestamp
                ON crypto_funding(pair, timestamp);

                CREATE INDEX IF NOT EXISTS idx_crypto_open_interest_pair_timestamp
                ON crypto_open_interest(pair, timestamp);
                """
            )
            self._connection.commit()

    def log_quote(self, snapshot: OrderBookSnapshot, imbalance: float, storage_levels: int) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_quotes (
                    timestamp, pair, venue, market_type, symbol,
                    bid_price, bid_size, ask_price, ask_size, mid_price, spread_bps,
                    imbalance, bid_depth_notional, ask_depth_notional, bids_json, asks_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp.isoformat(),
                    snapshot.pair,
                    snapshot.venue,
                    snapshot.market_type,
                    snapshot.symbol,
                    snapshot.bid_price,
                    snapshot.bid_size,
                    snapshot.ask_price,
                    snapshot.ask_size,
                    snapshot.mid_price,
                    snapshot.spread_bps,
                    imbalance,
                    snapshot.depth_notional(storage_levels, "bid"),
                    snapshot.depth_notional(storage_levels, "ask"),
                    json.dumps(snapshot.bids[:storage_levels]),
                    json.dumps(snapshot.asks[:storage_levels]),
                ),
            )
            self._connection.commit()

    def log_trade(self, trade: MarketTrade) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_trades (
                    timestamp, pair, venue, market_type, symbol,
                    price, size, taker_side, trade_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.timestamp.isoformat(),
                    trade.pair,
                    trade.venue,
                    trade.market_type,
                    trade.symbol,
                    trade.price,
                    trade.size,
                    trade.taker_side,
                    trade.trade_id,
                ),
            )
            self._connection.commit()

    def log_funding(self, snapshot: FundingSnapshot) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_funding (
                    timestamp, pair, venue, symbol, current_funding_rate, average_funding_rate,
                    next_funding_time, basis_rate, basis_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp.isoformat(),
                    snapshot.pair,
                    snapshot.venue,
                    snapshot.symbol,
                    snapshot.current_funding_rate,
                    snapshot.average_funding_rate,
                    None if snapshot.next_funding_time is None else snapshot.next_funding_time.isoformat(),
                    snapshot.basis_rate,
                    snapshot.basis_value,
                ),
            )
            self._connection.commit()

    def log_open_interest(self, snapshot: OpenInterestSnapshot) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_open_interest (
                    timestamp, pair, venue, symbol, interval, open_interest, open_interest_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp.isoformat(),
                    snapshot.pair,
                    snapshot.venue,
                    snapshot.symbol,
                    snapshot.interval,
                    snapshot.open_interest,
                    snapshot.open_interest_value,
                ),
            )
            self._connection.commit()

    def log_basis(self, observation: BasisObservation) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_basis (
                    timestamp, pair, spot_venue, spot_symbol, perp_venue, perp_symbol,
                    spot_mid, perp_mid, premium_bps, spot_imbalance, perp_imbalance,
                    current_funding_rate, average_funding_rate, next_funding_time, basis_rate, basis_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation.timestamp.isoformat(),
                    observation.pair,
                    observation.spot_venue,
                    observation.spot_symbol,
                    observation.perp_venue,
                    observation.perp_symbol,
                    observation.spot_mid,
                    observation.perp_mid,
                    observation.premium_bps,
                    observation.spot_imbalance,
                    observation.perp_imbalance,
                    observation.current_funding_rate,
                    observation.average_funding_rate,
                    None
                    if observation.next_funding_time is None
                    else observation.next_funding_time.isoformat(),
                    observation.basis_rate,
                    observation.basis_value,
                ),
            )
            self._connection.commit()

    def insert_signal(self, signal: SignalEvent) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT INTO crypto_signals (
                    timestamp, pair, venue, market_type, symbol, side,
                    signal_source, entry_bid, entry_ask, entry_mid, spread_bps, imbalance, threshold
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.timestamp.isoformat(),
                    signal.pair,
                    signal.venue,
                    signal.market_type,
                    signal.symbol,
                    signal.side,
                    signal.signal_source,
                    signal.entry_bid,
                    signal.entry_ask,
                    signal.entry_mid,
                    signal.spread_bps,
                    signal.imbalance,
                    signal.threshold,
                ),
            )
            self._connection.commit()
            return int(cursor.lastrowid)

    def insert_signal_markout(
        self,
        signal_id: int,
        horizon_ms: int,
        marked_at: str,
        markout_bps: float,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_signal_markouts (signal_id, horizon_ms, marked_at, markout_bps)
                VALUES (?, ?, ?, ?)
                """,
                (signal_id, horizon_ms, marked_at, markout_bps),
            )
            self._connection.commit()

    def insert_paper_order(
        self,
        *,
        signal_id: int,
        timestamp: str,
        pair: str,
        venue: str,
        market_type: str,
        symbol: str,
        side: str,
        limit_price: float,
        quantity: float,
        expires_at: str,
    ) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT INTO crypto_paper_orders (
                    signal_id, timestamp, pair, venue, market_type, symbol, side,
                    limit_price, quantity, status, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
                """,
                (
                    signal_id,
                    timestamp,
                    pair,
                    venue,
                    market_type,
                    symbol,
                    side,
                    limit_price,
                    quantity,
                    expires_at,
                ),
            )
            self._connection.commit()
            return int(cursor.lastrowid)

    def mark_order_expired(self, order_id: int) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE crypto_paper_orders SET status = 'EXPIRED' WHERE id = ?",
                (order_id,),
            )
            self._connection.commit()

    def fill_order(
        self,
        order_id: int,
        *,
        filled_at: str,
        fill_price: float,
        fill_trade_id: str,
        fill_latency_ms: float,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE crypto_paper_orders
                SET status = 'FILLED', filled_at = ?, fill_price = ?, fill_trade_id = ?, fill_latency_ms = ?
                WHERE id = ?
                """,
                (filled_at, fill_price, fill_trade_id, fill_latency_ms, order_id),
            )
            self._connection.commit()

    def insert_paper_markout(
        self,
        order_id: int,
        horizon_ms: int,
        marked_at: str,
        markout_bps: float,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO crypto_paper_markouts (order_id, horizon_ms, marked_at, markout_bps)
                VALUES (?, ?, ?, ?)
                """,
                (order_id, horizon_ms, marked_at, markout_bps),
            )
            self._connection.commit()

    def insert_spread_position(
        self,
        *,
        signal_id: int,
        timestamp: str,
        pair: str,
        quantity: float,
        entry_spot_price: float,
        entry_perp_price: float,
        entry_basis_bps: float,
        basis_threshold_bps: float,
        entry_funding_rate: float | None,
        signal_quality_score: float | None,
        signal_quality_band: str,
        spot_imbalance: float,
        perp_imbalance: float,
    ) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT INTO crypto_spread_positions (
                    signal_id, timestamp, pair, status, quantity,
                    entry_spot_price, entry_perp_price, entry_basis_bps, basis_threshold_bps,
                    entry_funding_rate, signal_quality_score, signal_quality_band,
                    spot_imbalance, perp_imbalance
                ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    timestamp,
                    pair,
                    quantity,
                    entry_spot_price,
                    entry_perp_price,
                    entry_basis_bps,
                    basis_threshold_bps,
                    entry_funding_rate,
                    signal_quality_score,
                    signal_quality_band,
                    spot_imbalance,
                    perp_imbalance,
                ),
            )
            self._connection.commit()
            return int(cursor.lastrowid)

    def close_spread_position(
        self,
        position_id: int,
        *,
        exit_timestamp: str,
        exit_reason: str,
        exit_spot_price: float,
        exit_perp_price: float,
        exit_basis_bps: float,
        exit_funding_rate: float | None,
        gross_edge_pct: float,
        gross_pnl_quote: float,
        net_pnl_quote: float,
        hold_ms: float,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE crypto_spread_positions
                SET status = 'CLOSED',
                    exit_timestamp = ?,
                    exit_reason = ?,
                    exit_spot_price = ?,
                    exit_perp_price = ?,
                    exit_basis_bps = ?,
                    exit_funding_rate = ?,
                    gross_edge_pct = ?,
                    gross_pnl_quote = ?,
                    net_pnl_quote = ?,
                    hold_ms = ?
                WHERE id = ?
                """,
                (
                    exit_timestamp,
                    exit_reason,
                    exit_spot_price,
                    exit_perp_price,
                    exit_basis_bps,
                    exit_funding_rate,
                    gross_edge_pct,
                    gross_pnl_quote,
                    net_pnl_quote,
                    hold_ms,
                    position_id,
                ),
            )
            self._connection.commit()

    def fetch_all(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._connection.execute(query, params).fetchall())

    def close(self) -> None:
        with self._lock:
            self._connection.close()
