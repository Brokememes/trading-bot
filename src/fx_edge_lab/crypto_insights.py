from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from itertools import groupby

from .crypto_models import CryptoPairSettings, CryptoResearchSettings
from .crypto_pnl import (
    LONG_SPOT_SHORT_PERP,
    SHORT_SPOT_LONG_PERP,
    borrow_cost_quote,
    signal_quality_band,
    signal_quality_score,
    spread_gross_pnl_quote,
    spread_net_pnl_quote,
    spread_target_edge_pct,
    target_exit_basis_bps,
)
from .crypto_storage import CryptoSQLiteStorage

FUNDING_HOURS_UTC = (0, 8, 16)


def count_strategy_signals(storage: CryptoSQLiteStorage) -> int:
    return int(
        storage.fetch_all(
            """
            SELECT COUNT(*) AS n
            FROM crypto_signals
            WHERE signal_source LIKE 'basis_tier%'
            """
        )[0]["n"]
    )


def fetch_latest_basis(storage: CryptoSQLiteStorage) -> list[dict]:
    return [
        dict(row)
        for row in storage.fetch_all(
            """
            SELECT b.*
            FROM crypto_basis b
            JOIN (
                SELECT pair, MAX(timestamp) AS max_ts
                FROM crypto_basis
                GROUP BY pair
            ) latest
              ON latest.pair = b.pair AND latest.max_ts = b.timestamp
            ORDER BY b.pair
            """
        )
    ]


def find_pair(settings: CryptoResearchSettings, pair_name: str) -> CryptoPairSettings | None:
    for pair in settings.pairs:
        if pair.name == pair_name:
            return pair
    return None


def signals_today(storage: CryptoSQLiteStorage, pair_name: str, timestamp: datetime) -> int:
    day_start = timestamp.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    row = storage.fetch_all(
        """
        SELECT COUNT(*) AS n
        FROM crypto_signals
        WHERE pair = ? AND signal_source LIKE 'basis_tier%' AND timestamp >= ? AND timestamp < ?
        """,
        (pair_name, day_start.isoformat(), day_end.isoformat()),
    )[0]
    return int(row["n"])


def adaptive_threshold(pair: CryptoPairSettings, signals_today_count: int) -> float:
    if signals_today_count < 2:
        return pair.basis_entry_threshold_low_bps
    if signals_today_count > 10:
        return pair.basis_entry_threshold_high_bps
    return pair.basis_entry_threshold_bps


def next_funding_window(timestamp: datetime) -> datetime:
    current = timestamp.astimezone(timezone.utc)
    base = current.replace(minute=0, second=0, microsecond=0)
    for hour in FUNDING_HOURS_UTC:
        candidate = base.replace(hour=hour)
        if candidate >= current:
            return candidate
    return (base + timedelta(days=1)).replace(hour=0)


def pre_funding_state(timestamp: datetime, settings: CryptoResearchSettings) -> dict:
    next_funding = next_funding_window(timestamp)
    countdown_ms = (next_funding - timestamp.astimezone(timezone.utc)).total_seconds() * 1000.0
    alert_active = 0.0 <= countdown_ms <= settings.pre_funding_window_ms
    return {
        "next_funding_time": next_funding.isoformat(),
        "countdown_ms": countdown_ms,
        "alert_active": alert_active,
    }


def tier2_threshold_bps(
    pair: CryptoPairSettings,
    timestamp: datetime,
    settings: CryptoResearchSettings,
) -> float:
    funding_state = pre_funding_state(timestamp, settings)
    if funding_state["alert_active"]:
        return min(pair.basis_entry_threshold_bps, settings.pre_funding_basis_threshold_bps)
    return pair.basis_entry_threshold_bps


def basis_momentum_bps(
    storage: CryptoSQLiteStorage,
    pair_name: str,
    timestamp: datetime,
    window_ms: int,
    tolerance_ms: int = 2000,
) -> float | None:
    window_start = timestamp - timedelta(milliseconds=window_ms)
    rows = storage.fetch_all(
        """
        SELECT timestamp, premium_bps
        FROM crypto_basis
        WHERE pair = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
        """,
        (pair_name, window_start.isoformat(), timestamp.isoformat()),
    )
    if len(rows) < 2:
        return None
    first_time = parse_iso(str(rows[0]["timestamp"]))
    last_time = parse_iso(str(rows[-1]["timestamp"]))
    elapsed_ms = (last_time - first_time).total_seconds() * 1000.0
    if elapsed_ms + tolerance_ms < window_ms:
        return None
    return float(rows[-1]["premium_bps"]) - float(rows[0]["premium_bps"])


def consecutive_basis_samples(
    storage: CryptoSQLiteStorage,
    pair_name: str,
    timestamp: datetime,
    threshold_bps: float,
    side: str,
    required_samples: int,
) -> int:
    sample_limit = max(required_samples * 5, required_samples + 2, 10)
    rows = storage.fetch_all(
        """
        SELECT premium_bps
        FROM crypto_basis
        WHERE pair = ? AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (pair_name, timestamp.isoformat(), sample_limit),
    )
    count = 0
    for row in rows:
        premium_bps = float(row["premium_bps"])
        if side == LONG_SPOT_SHORT_PERP and premium_bps >= threshold_bps:
            count += 1
            continue
        if side == SHORT_SPOT_LONG_PERP and premium_bps <= -threshold_bps:
            count += 1
            continue
        break
    return count


def regime_from_avg_basis(avg_basis_bps: float | None, settings: CryptoResearchSettings) -> str:
    if avg_basis_bps is None:
        return "NEUTRAL"
    if avg_basis_bps > settings.regime_contango_bps:
        return "CONTANGO"
    if avg_basis_bps < settings.regime_backwardation_bps:
        return "BACKWARDATION"
    return "NEUTRAL"


def regime_snapshot(
    storage: CryptoSQLiteStorage,
    pair_name: str,
    timestamp: datetime,
    settings: CryptoResearchSettings,
    tolerance_ms: int = 2000,
) -> dict:
    window_start = timestamp - timedelta(milliseconds=settings.regime_window_ms)
    rows = storage.fetch_all(
        """
        SELECT timestamp, premium_bps
        FROM crypto_basis
        WHERE pair = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
        """,
        (pair_name, window_start.isoformat(), timestamp.isoformat()),
    )
    avg_basis_bps = None
    if rows:
        first_time = parse_iso(str(rows[0]["timestamp"]))
        last_time = parse_iso(str(rows[-1]["timestamp"]))
        elapsed_ms = (last_time - first_time).total_seconds() * 1000.0
        if len(rows) >= 2 and elapsed_ms + tolerance_ms >= settings.regime_window_ms:
            avg_basis_bps = sum(float(row["premium_bps"]) for row in rows) / len(rows)
    regime = regime_from_avg_basis(avg_basis_bps, settings)
    duration_ms, changes_today, last_change = regime_duration_and_changes(storage, pair_name, timestamp, settings)
    return {
        "regime": regime,
        "avg_basis_bps": avg_basis_bps,
        "regime_duration_ms": duration_ms,
        "regime_changes_today": changes_today,
        "last_regime_change_at": None if last_change is None else last_change.isoformat(),
    }


def regime_duration_and_changes(
    storage: CryptoSQLiteStorage,
    pair_name: str,
    timestamp: datetime,
    settings: CryptoResearchSettings,
) -> tuple[float | None, int, datetime | None]:
    day_start = timestamp.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = hourly_regime_rows(storage, settings, pair_name, day_start)
    if not rows:
        return None, 0, None
    latest_regime = rows[-1]["regime"]
    changes_today = 0
    last_change_bucket: datetime | None = None
    previous_regime = rows[0]["regime"]
    for row in rows[1:]:
        if row["regime"] != previous_regime:
            changes_today += 1
            last_change_bucket = parse_iso(str(row["bucket_start"]))
        previous_regime = row["regime"]
    regime_since = parse_iso(str(rows[0]["bucket_start"]))
    for row in reversed(rows):
        if row["regime"] != latest_regime:
            regime_since = parse_iso(str(row["bucket_start"])) + timedelta(hours=1)
            break
        regime_since = parse_iso(str(row["bucket_start"]))
    duration_ms = (timestamp - regime_since).total_seconds() * 1000.0
    return duration_ms, changes_today, last_change_bucket


def load_spread_positions(
    storage: CryptoSQLiteStorage,
    latest_basis: list[dict],
    settings: CryptoResearchSettings,
) -> list[dict]:
    basis_by_pair = {str(row["pair"]): row for row in latest_basis}
    rows = [
        dict(row)
        for row in storage.fetch_all(
            """
            SELECT p.id, p.signal_id, p.timestamp, p.pair, p.status, p.quantity,
                   p.entry_spot_price, p.entry_perp_price, p.entry_basis_bps,
                   p.basis_threshold_bps, p.entry_funding_rate, p.signal_quality_score,
                   p.signal_quality_band, p.spot_imbalance, p.perp_imbalance,
                   p.exit_timestamp, p.exit_reason, p.exit_spot_price, p.exit_perp_price,
                   p.exit_basis_bps, p.exit_funding_rate, p.gross_edge_pct,
                   p.gross_pnl_quote, p.net_pnl_quote, p.hold_ms,
                   s.side, s.signal_source
            FROM crypto_spread_positions p
            JOIN crypto_signals s ON s.id = p.signal_id
            ORDER BY p.timestamp DESC
            """
        )
    ]
    decorated: list[dict] = []
    for row in rows:
        item = dict(row)
        timestamp = parse_iso(str(item["timestamp"]))
        side = str(item["side"])
        live_basis = basis_by_pair.get(str(item["pair"]))
        if item["status"] == "OPEN" and live_basis is not None:
            current_basis_bps = float(live_basis["premium_bps"])
            current_spot_price = float(live_basis["spot_mid"])
            current_funding_rate = (
                None if live_basis["current_funding_rate"] is None else float(live_basis["current_funding_rate"])
            )
            hold_ms = (parse_iso(str(live_basis["timestamp"])) - timestamp).total_seconds() * 1000.0
            item["live_basis_bps"] = current_basis_bps
            item["live_funding_rate"] = current_funding_rate
            item["hold_ms_effective"] = hold_ms
            item["live_gross_pnl_quote"] = spread_gross_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                float(item["entry_basis_bps"]),
                current_basis_bps,
            )
            item["live_net_without_borrow_quote"] = spread_net_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                current_spot_price,
                float(item["entry_basis_bps"]),
                current_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=False,
            )
            item["live_net_with_borrow_quote"] = spread_net_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                current_spot_price,
                float(item["entry_basis_bps"]),
                current_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=True,
            )
            item["borrow_cost_quote"] = borrow_cost_quote(
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                hold_ms,
                settings.reverse_spot_borrow_apy if side == SHORT_SPOT_LONG_PERP else 0.0,
            )
            item["age_minutes"] = hold_ms / 60000.0
        else:
            hold_ms = float(item["hold_ms"] or 0.0)
            exit_spot_price = float(item["exit_spot_price"] or item["entry_spot_price"])
            exit_basis_bps = float(item["exit_basis_bps"] or item["entry_basis_bps"])
            item["live_basis_bps"] = exit_basis_bps
            item["live_funding_rate"] = item["exit_funding_rate"]
            item["hold_ms_effective"] = hold_ms
            item["live_gross_pnl_quote"] = spread_gross_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                float(item["entry_basis_bps"]),
                exit_basis_bps,
            )
            item["live_net_without_borrow_quote"] = spread_net_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                exit_spot_price,
                float(item["entry_basis_bps"]),
                exit_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=False,
            )
            item["live_net_with_borrow_quote"] = spread_net_pnl_quote(
                side,
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                exit_spot_price,
                float(item["entry_basis_bps"]),
                exit_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=True,
            )
            item["borrow_cost_quote"] = borrow_cost_quote(
                float(item["quantity"]),
                float(item["entry_spot_price"]),
                hold_ms,
                settings.reverse_spot_borrow_apy if side == SHORT_SPOT_LONG_PERP else 0.0,
            )
            item["age_minutes"] = hold_ms / 60000.0 if hold_ms > 0 else None
        signal_source = str(item.get("signal_source") or "")
        if signal_source == "basis_tier1_confirmed":
            item["entry_mode"] = "TIER1_FULL"
        elif signal_source == "basis_tier2_extreme":
            item["entry_mode"] = "TIER2_HALF"
        else:
            item["entry_mode"] = "ORIGINAL" if side == LONG_SPOT_SHORT_PERP else "REVERSE_PAPER"
        decorated.append(item)
    return decorated


def build_live_signal_rows(
    storage: CryptoSQLiteStorage,
    settings: CryptoResearchSettings,
    latest_basis: list[dict],
    open_positions: list[dict],
) -> list[dict]:
    open_by_pair = {str(row["pair"]): row for row in open_positions}
    rows = []
    for basis_row in latest_basis:
        pair = find_pair(settings, str(basis_row["pair"]))
        if pair is None:
            continue
        timestamp = parse_iso(str(basis_row["timestamp"]))
        regime_info = regime_snapshot(storage, pair.name, timestamp, settings)
        signals_today_count = signals_today(storage, pair.name, timestamp)
        active_threshold_bps = adaptive_threshold(pair, signals_today_count)
        basis_only_threshold_bps = tier2_threshold_bps(pair, timestamp, settings)
        momentum_bps = basis_momentum_bps(storage, pair.name, timestamp, pair.basis_momentum_window_ms)
        trend_10m_bps = basis_momentum_bps(storage, pair.name, timestamp, settings.pre_funding_trend_window_ms)
        current_funding_rate = (
            None if basis_row["current_funding_rate"] is None else float(basis_row["current_funding_rate"])
        )
        funding_state = pre_funding_state(timestamp, settings)
        regime = regime_info["regime"]
        premium_bps = float(basis_row["premium_bps"])
        if premium_bps > 0:
            side = LONG_SPOT_SHORT_PERP
            gross_edge_pct = spread_target_edge_pct(side, premium_bps, pair.basis_exit_threshold_bps)
        elif premium_bps < 0:
            side = SHORT_SPOT_LONG_PERP
            gross_edge_pct = spread_target_edge_pct(side, premium_bps, pair.basis_exit_threshold_bps)
        else:
            side = None
            gross_edge_pct = 0.0
        tier1_duration_count = 0
        tier2_duration_count = 0
        if side is not None:
            tier1_duration_count = consecutive_basis_samples(
                storage,
                pair.name,
                timestamp,
                active_threshold_bps,
                side,
                settings.basis_consecutive_samples_required,
            )
            tier2_duration_count = consecutive_basis_samples(
                storage,
                pair.name,
                timestamp,
                basis_only_threshold_bps,
                side,
                settings.basis_consecutive_samples_required,
            )
        quality_score = signal_quality_score(gross_edge_pct, settings)
        status = signal_status(
            premium_bps=premium_bps,
            strong_threshold_bps=active_threshold_bps,
            basis_only_threshold_bps=basis_only_threshold_bps,
            funding_rate=current_funding_rate,
            min_funding_rate=pair.funding_entry_min_rate,
            momentum_bps=momentum_bps,
            tier1_duration_count=tier1_duration_count,
            tier2_duration_count=tier2_duration_count,
            required_duration_samples=settings.basis_consecutive_samples_required,
            quality_score=quality_score,
            has_open_position=pair.name in open_by_pair,
        )
        rows.append(
            {
                "pair": pair.name,
                "timestamp": basis_row["timestamp"],
                "premium_bps": premium_bps,
                "active_threshold_bps": active_threshold_bps,
                "basis_only_threshold_bps": basis_only_threshold_bps,
                "basis_exit_threshold_bps": pair.basis_exit_threshold_bps,
                "momentum_bps": momentum_bps,
                "basis_trend_10m_bps": trend_10m_bps,
                "current_funding_rate": current_funding_rate,
                "signals_today": signals_today_count,
                "gross_edge_pct": gross_edge_pct,
                "signal_quality_score": quality_score,
                "signal_quality_band": signal_quality_band(quality_score),
                "tier1_duration_count": tier1_duration_count,
                "tier2_duration_count": tier2_duration_count,
                "required_duration_samples": settings.basis_consecutive_samples_required,
                "pre_funding_alert_active": funding_state["alert_active"],
                "time_to_next_funding_ms": funding_state["countdown_ms"],
                "next_funding_time": funding_state["next_funding_time"],
                "status": status,
                "regime": regime,
                "regime_avg_basis_bps": regime_info["avg_basis_bps"],
                "regime_duration_ms": regime_info["regime_duration_ms"],
                "regime_changes_today": regime_info["regime_changes_today"],
                "mode": "ORIGINAL" if side == LONG_SPOT_SHORT_PERP else ("REVERSE_PAPER" if side == SHORT_SPOT_LONG_PERP else "MONITOR_ONLY"),
            }
        )
    return sorted(rows, key=lambda item: item["pair"])


def signal_status(
    *,
    premium_bps: float,
    strong_threshold_bps: float,
    basis_only_threshold_bps: float,
    funding_rate: float | None,
    min_funding_rate: float,
    momentum_bps: float | None,
    tier1_duration_count: int,
    tier2_duration_count: int,
    required_duration_samples: int,
    quality_score: float | None,
    has_open_position: bool,
) -> str:
    if has_open_position:
        return "IN_POSITION"
    if premium_bps == 0:
        return "NEUTRAL_MONITOR"
    is_long = premium_bps > 0
    abs_basis_bps = abs(premium_bps)
    funding_ready = funding_rate is not None and (
        funding_rate > min_funding_rate if is_long else funding_rate < -min_funding_rate
    )
    momentum_ready = momentum_bps is not None and (momentum_bps > 0 if is_long else momentum_bps < 0)
    if abs_basis_bps >= strong_threshold_bps and funding_ready and momentum_ready:
        if tier1_duration_count < required_duration_samples:
            return "WAIT_DURATION"
        if quality_score is not None and quality_score < 0:
            return "DO_NOT_TRADE"
        return "READY_TIER1"
    if abs_basis_bps >= basis_only_threshold_bps:
        if tier2_duration_count < required_duration_samples:
            return "WAIT_DURATION"
        if quality_score is not None and quality_score < 0:
            return "DO_NOT_TRADE"
        return "READY_TIER2"
    if abs_basis_bps < strong_threshold_bps:
        return "WAIT_BASIS"
    if not funding_ready and not momentum_ready:
        return "WAIT_FUNDING_MOMENTUM"
    if not funding_ready:
        return "WAIT_FUNDING"
    if not momentum_ready:
        return "WAIT_MOMENTUM"
    if quality_score is not None and quality_score < 0:
        return "DO_NOT_TRADE"
    return "WATCH_EXTREME"


def hourly_regime_rows(
    storage: CryptoSQLiteStorage,
    settings: CryptoResearchSettings,
    pair_name: str | None,
    start_time: datetime,
) -> list[dict]:
    params: tuple = (start_time.isoformat(),)
    pair_filter = ""
    if pair_name is not None:
        pair_filter = "AND pair = ?"
        params = (start_time.isoformat(), pair_name)
    rows = storage.fetch_all(
        f"""
        SELECT pair,
               substr(timestamp, 1, 13) || ':00:00+00:00' AS bucket_start,
               AVG(premium_bps) AS avg_basis_bps,
               MIN(premium_bps) AS min_basis_bps,
               MAX(premium_bps) AS max_basis_bps,
               AVG(current_funding_rate) AS avg_funding_rate,
               AVG(spot_mid) AS avg_spot_mid,
               COUNT(*) AS samples
        FROM crypto_basis
        WHERE timestamp >= ? {pair_filter}
        GROUP BY pair, bucket_start
        ORDER BY pair, bucket_start
        """,
        params,
    )
    return [
        {
            "pair": str(row["pair"]),
            "bucket_start": str(row["bucket_start"]),
            "avg_basis_bps": float(row["avg_basis_bps"]) if row["avg_basis_bps"] is not None else None,
            "min_basis_bps": float(row["min_basis_bps"]) if row["min_basis_bps"] is not None else None,
            "max_basis_bps": float(row["max_basis_bps"]) if row["max_basis_bps"] is not None else None,
            "avg_funding_rate": float(row["avg_funding_rate"]) if row["avg_funding_rate"] is not None else None,
            "avg_spot_mid": float(row["avg_spot_mid"]) if row["avg_spot_mid"] is not None else None,
            "samples": int(row["samples"]),
            "regime": regime_from_avg_basis(
                None if row["avg_basis_bps"] is None else float(row["avg_basis_bps"]),
                settings,
            ),
        }
        for row in rows
    ]


def spike_forensics_rows(
    storage: CryptoSQLiteStorage,
    settings: CryptoResearchSettings,
    lookback_days: int = 7,
    threshold_bps: float = 60.0,
) -> list[dict]:
    start_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rows = storage.fetch_all(
        """
        SELECT pair, timestamp, premium_bps, current_funding_rate
        FROM crypto_basis
        WHERE timestamp >= ? AND ABS(premium_bps) >= ?
        ORDER BY pair, timestamp
        """,
        (start_time.isoformat(), threshold_bps),
    )
    split_gap_ms = max(settings.basis_sample_interval_ms * 2.5, 2500.0)
    episodes: list[tuple[str, list[dict]]] = []
    for pair_name, pair_rows_iter in groupby(rows, key=lambda row: str(row["pair"])):
        pair_rows = [
            {
                "timestamp": parse_iso(str(row["timestamp"])),
                "premium_bps": float(row["premium_bps"]),
                "current_funding_rate": None
                if row["current_funding_rate"] is None
                else float(row["current_funding_rate"]),
            }
            for row in pair_rows_iter
        ]
        current_episode: list[dict] = []
        previous: dict | None = None
        for row in pair_rows:
            if previous is None:
                current_episode = [row]
            else:
                gap_ms = (row["timestamp"] - previous["timestamp"]).total_seconds() * 1000.0
                same_direction = (row["premium_bps"] >= 0) == (previous["premium_bps"] >= 0)
                if gap_ms <= split_gap_ms and same_direction:
                    current_episode.append(row)
                else:
                    episodes.append((pair_name, current_episode))
                    current_episode = [row]
            previous = row
        if current_episode:
            episodes.append((pair_name, current_episode))

    results: list[dict] = []
    for pair_name, episode in episodes:
        peak = max(episode, key=lambda item: abs(item["premium_bps"]))
        first_time = episode[0]["timestamp"]
        last_time = episode[-1]["timestamp"]
        peak_time = peak["timestamp"]
        peak_basis_bps = peak["premium_bps"]
        peak_funding_bps = None if peak["current_funding_rate"] is None else peak["current_funding_rate"] * 10_000.0
        funding_state = pre_funding_state(peak_time, settings)

        def later_basis(minutes: int) -> float | None:
            target = peak_time + timedelta(minutes=minutes)
            row = storage.fetch_all(
                """
                SELECT premium_bps
                FROM crypto_basis
                WHERE pair = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (pair_name, target.isoformat()),
            )
            if not row:
                return None
            return float(row[0]["premium_bps"])

        basis_5m = later_basis(5)
        basis_15m = later_basis(15)
        basis_30m = later_basis(30)
        results.append(
            {
                "pair": pair_name,
                "peak_timestamp": peak_time.isoformat(),
                "peak_basis_bps": peak_basis_bps,
                "direction": "CONTANGO" if peak_basis_bps > 0 else "BACKWARDATION",
                "duration_ms": (last_time - first_time).total_seconds() * 1000.0,
                "samples": len(episode),
                "funding_peak_bps": peak_funding_bps,
                "basis_5m_bps": basis_5m,
                "basis_15m_bps": basis_15m,
                "basis_30m_bps": basis_30m,
                "basis_5m_abs_change_bps": None if basis_5m is None else abs(peak_basis_bps) - abs(basis_5m),
                "basis_15m_abs_change_bps": None if basis_15m is None else abs(peak_basis_bps) - abs(basis_15m),
                "basis_30m_abs_change_bps": None if basis_30m is None else abs(peak_basis_bps) - abs(basis_30m),
                "pre_funding_alert_active": funding_state["alert_active"],
                "time_to_next_funding_ms": funding_state["countdown_ms"],
                "next_funding_time": funding_state["next_funding_time"],
            }
        )
    return sorted(results, key=lambda item: item["peak_timestamp"], reverse=True)


def simulate_strategy_history(
    storage: CryptoSQLiteStorage,
    settings: CryptoResearchSettings,
    lookback_days: int = 7,
) -> dict:
    start_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rows = [
        {
            "pair": str(row["pair"]),
            "timestamp": parse_iso(str(row["bucket_start"])),
            "premium_bps": float(row["premium_bps"]),
            "min_basis_bps": float(row["min_basis_bps"]),
            "max_basis_bps": float(row["max_basis_bps"]),
            "spot_mid": float(row["spot_mid"]),
            "current_funding_rate": None
            if row["current_funding_rate"] is None
            else float(row["current_funding_rate"]),
        }
        for row in storage.fetch_all(
            """
            SELECT pair,
                   substr(timestamp, 1, 16) || ':00+00:00' AS bucket_start,
                   AVG(premium_bps) AS premium_bps,
                   MIN(premium_bps) AS min_basis_bps,
                   MAX(premium_bps) AS max_basis_bps,
                   AVG(spot_mid) AS spot_mid,
                   AVG(current_funding_rate) AS current_funding_rate
            FROM crypto_basis
            WHERE timestamp >= ?
            GROUP BY pair, bucket_start
            ORDER BY pair, bucket_start
            """,
            (start_time.isoformat(),),
        )
    ]

    summary_rows: list[dict] = []
    equity_points: list[dict] = []
    trade_rows: list[dict] = []
    for pair_name, pair_rows_iter in groupby(rows, key=lambda row: row["pair"]):
        pair = find_pair(settings, pair_name)
        if pair is None:
            continue
        pair_rows = list(pair_rows_iter)
        momentum_window: deque[dict] = deque()
        open_position: dict | None = None
        long_hits = 0
        reverse_hits = 0
        long_tier1_run = 0
        short_tier1_run = 0
        long_tier2_run = 0
        short_tier2_run = 0
        trades = 0
        gross_total = 0.0
        net_without_borrow_total = 0.0
        net_with_borrow_total = 0.0
        for row in pair_rows:
            momentum_window.append(row)
            momentum_cutoff = row["timestamp"] - timedelta(milliseconds=pair.basis_momentum_window_ms)
            while len(momentum_window) > 1 and momentum_window[1]["timestamp"] <= momentum_cutoff:
                momentum_window.popleft()

            momentum_bps = _momentum_from_minute_window(momentum_window, row["timestamp"], pair.basis_momentum_window_ms)
            strong_threshold_bps = pair.basis_entry_threshold_low_bps
            basis_only_threshold_bps = tier2_threshold_bps(pair, row["timestamp"], settings)
            funding_rate = row["current_funding_rate"]

            if row["max_basis_bps"] >= strong_threshold_bps:
                long_hits += 1
            if row["min_basis_bps"] <= -strong_threshold_bps:
                reverse_hits += 1

            long_tier1_run = long_tier1_run + 1 if row["max_basis_bps"] >= strong_threshold_bps else 0
            short_tier1_run = short_tier1_run + 1 if row["min_basis_bps"] <= -strong_threshold_bps else 0
            long_tier2_run = long_tier2_run + 1 if row["max_basis_bps"] >= basis_only_threshold_bps else 0
            short_tier2_run = short_tier2_run + 1 if row["min_basis_bps"] <= -basis_only_threshold_bps else 0

            if open_position is None:
                if (
                    row["max_basis_bps"] >= strong_threshold_bps
                    and funding_rate is not None
                    and funding_rate > pair.funding_entry_min_rate
                    and momentum_bps is not None
                    and momentum_bps > 0
                    and long_tier1_run >= settings.basis_consecutive_samples_required
                ):
                    open_position = {
                        "side": LONG_SPOT_SHORT_PERP,
                        "entry_time": row["timestamp"],
                        "entry_basis_bps": max(row["max_basis_bps"], strong_threshold_bps),
                        "entry_spot_price": row["spot_mid"],
                        "signal_source": "basis_tier1_confirmed",
                    }
                elif (
                    row["min_basis_bps"] <= -strong_threshold_bps
                    and funding_rate is not None
                    and funding_rate < -pair.funding_entry_min_rate
                    and momentum_bps is not None
                    and momentum_bps < 0
                    and short_tier1_run >= settings.basis_consecutive_samples_required
                ):
                    open_position = {
                        "side": SHORT_SPOT_LONG_PERP,
                        "entry_time": row["timestamp"],
                        "entry_basis_bps": min(row["min_basis_bps"], -strong_threshold_bps),
                        "entry_spot_price": row["spot_mid"],
                        "signal_source": "basis_tier1_confirmed",
                    }
                elif (
                    row["max_basis_bps"] >= basis_only_threshold_bps
                    and long_tier2_run >= settings.basis_consecutive_samples_required
                ):
                    open_position = {
                        "side": LONG_SPOT_SHORT_PERP,
                        "entry_time": row["timestamp"],
                        "entry_basis_bps": max(row["max_basis_bps"], basis_only_threshold_bps),
                        "entry_spot_price": row["spot_mid"],
                        "signal_source": "basis_tier2_extreme",
                    }
                elif (
                    row["min_basis_bps"] <= -basis_only_threshold_bps
                    and short_tier2_run >= settings.basis_consecutive_samples_required
                ):
                    open_position = {
                        "side": SHORT_SPOT_LONG_PERP,
                        "entry_time": row["timestamp"],
                        "entry_basis_bps": min(row["min_basis_bps"], -basis_only_threshold_bps),
                        "entry_spot_price": row["spot_mid"],
                        "signal_source": "basis_tier2_extreme",
                    }
                continue

            side = str(open_position["side"])
            exit_basis = target_exit_basis_bps(side, pair.basis_exit_threshold_bps)
            exit_now = False
            exit_reason = ""
            live_basis_bps = row["premium_bps"]
            if side == LONG_SPOT_SHORT_PERP and live_basis_bps <= exit_basis:
                exit_now = True
                exit_reason = "BASIS_CONVERGED"
            elif side == SHORT_SPOT_LONG_PERP and live_basis_bps >= exit_basis:
                exit_now = True
                exit_reason = "BASIS_CONVERGED"
            elif (
                side == LONG_SPOT_SHORT_PERP
                and funding_rate is not None
                and funding_rate < pair.funding_exit_rate
            ):
                exit_now = True
                exit_reason = "FUNDING_FLIPPED"
            elif (
                side == SHORT_SPOT_LONG_PERP
                and funding_rate is not None
                and funding_rate > -pair.funding_exit_rate
            ):
                exit_now = True
                exit_reason = "FUNDING_FLIPPED"
            elif row["timestamp"] >= open_position["entry_time"] + timedelta(milliseconds=pair.max_hold_ms):
                exit_now = True
                exit_reason = "TIME_STOP"

            if not exit_now:
                continue

            hold_ms = (row["timestamp"] - open_position["entry_time"]).total_seconds() * 1000.0
            size_fraction = (
                pair.extreme_basis_size_fraction
                if open_position.get("signal_source") == "basis_tier2_extreme"
                else 1.0
            )
            quantity = pair.order_size * size_fraction
            gross = spread_gross_pnl_quote(
                side,
                quantity,
                float(open_position["entry_spot_price"]),
                float(open_position["entry_basis_bps"]),
                live_basis_bps,
            )
            net_without = spread_net_pnl_quote(
                side,
                quantity,
                float(open_position["entry_spot_price"]),
                row["spot_mid"],
                float(open_position["entry_basis_bps"]),
                live_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=False,
            )
            net_with = spread_net_pnl_quote(
                side,
                quantity,
                float(open_position["entry_spot_price"]),
                row["spot_mid"],
                float(open_position["entry_basis_bps"]),
                live_basis_bps,
                hold_ms,
                settings,
                include_borrow_cost=True,
            )
            borrow_quote = borrow_cost_quote(
                quantity,
                float(open_position["entry_spot_price"]),
                hold_ms,
                settings.reverse_spot_borrow_apy if side == SHORT_SPOT_LONG_PERP else 0.0,
            )
            trades += 1
            gross_total += gross
            net_without_borrow_total += net_without
            net_with_borrow_total += net_with
            trade_rows.append(
                {
                    "pair": pair.name,
                    "exit_timestamp": row["timestamp"].isoformat(),
                    "side": side,
                    "entry_basis_bps": float(open_position["entry_basis_bps"]),
                    "exit_basis_bps": live_basis_bps,
                    "gross_pnl_quote": gross,
                    "net_without_borrow_quote": net_without,
                    "net_with_borrow_quote": net_with,
                    "borrow_cost_quote": borrow_quote,
                    "exit_reason": exit_reason,
                }
            )
            open_position = None

        summary_rows.append(
            {
                "pair": pair.name,
                "long_hits_60bps": long_hits,
                "reverse_hits_60bps": reverse_hits,
                "what_if_trades": trades,
                "what_if_gross_pnl_quote": gross_total,
                "what_if_net_without_borrow_quote": net_without_borrow_total,
                "what_if_net_with_borrow_quote": net_with_borrow_total,
            }
        )

    cumulative_without = 0.0
    cumulative_with = 0.0
    for row in sorted(trade_rows, key=lambda item: item["exit_timestamp"]):
        cumulative_without += float(row["net_without_borrow_quote"])
        cumulative_with += float(row["net_with_borrow_quote"])
        equity_points.append(
            {
                "timestamp": row["exit_timestamp"],
                "pair": row["pair"],
                "cumulative_net_without_borrow_quote": cumulative_without,
                "cumulative_net_with_borrow_quote": cumulative_with,
            }
        )

    return {
        "summary_rows": sorted(summary_rows, key=lambda item: item["pair"]),
        "trade_rows": sorted(trade_rows, key=lambda item: item["exit_timestamp"], reverse=True),
        "equity_points": equity_points,
        "lookback_days": lookback_days,
    }


def _regime_from_minute_windows(
    history: deque[dict],
    regime_sum: float,
    timestamp: datetime,
    settings: CryptoResearchSettings,
) -> tuple[str, float | None]:
    if len(history) < 2:
        return "NEUTRAL", None
    elapsed_ms = (history[-1]["timestamp"] - history[0]["timestamp"]).total_seconds() * 1000.0
    if elapsed_ms + 60000 < settings.regime_window_ms:
        return "NEUTRAL", None
    avg_basis_bps = regime_sum / len(history)
    return regime_from_avg_basis(avg_basis_bps, settings), avg_basis_bps


def _momentum_from_minute_window(history: deque[dict], timestamp: datetime, window_ms: int) -> float | None:
    if len(history) < 2:
        return None
    elapsed_ms = (history[-1]["timestamp"] - history[0]["timestamp"]).total_seconds() * 1000.0
    if elapsed_ms + 60000 < window_ms:
        return None
    return float(history[-1]["premium_bps"]) - float(history[0]["premium_bps"])


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)
