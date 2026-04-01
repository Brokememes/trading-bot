from __future__ import annotations

from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from .crypto_insights import (
    build_live_signal_rows,
    fetch_latest_basis,
    find_pair,
    load_spread_positions,
    parse_iso,
    regime_from_avg_basis,
    simulate_strategy_history,
)
from .crypto_pnl import (
    LONG_SPOT_SHORT_PERP,
    SHORT_SPOT_LONG_PERP,
    estimated_net_live_pnl_quote,
    gross_live_pnl_quote,
    spread_gross_pnl_quote,
    spread_net_pnl_quote,
)
from .crypto_storage import CryptoSQLiteStorage

STRATEGY_BASIS = "basis_regime_carry"
STRATEGY_FUNDING_FLIP = "funding_flip_momentum"
STRATEGY_FUNDING_DIVERGENCE = "exchange_funding_divergence"
STRATEGY_LIQUIDATION = "liquidation_cascade_snapback"

STRATEGY_META = {
    STRATEGY_BASIS: {
        "label": "Basis Regime Carry",
        "category": "Regime-dependent",
    },
    STRATEGY_FUNDING_FLIP: {
        "label": "Funding Flip Momentum",
        "category": "Behavioral + Structural",
    },
    STRATEGY_FUNDING_DIVERGENCE: {
        "label": "Funding Divergence Spread",
        "category": "Capacity-limited",
    },
    STRATEGY_LIQUIDATION: {
        "label": "Liquidation Cascade Snap-back",
        "category": "Behavioral",
    },
}


def build_strategy_lab(
    storage: CryptoSQLiteStorage,
    settings,
    latest_basis: list[dict] | None = None,
    lookback_days: int | None = None,
) -> dict:
    lookback_days = settings.strategy_lookback_days if lookback_days is None else lookback_days
    latest_basis = latest_basis if latest_basis is not None else fetch_latest_basis(storage)
    position_rows = load_spread_positions(storage, latest_basis, settings)
    open_positions = [row for row in position_rows if row["status"] == "OPEN"]
    live_basis_rows = build_live_signal_rows(storage, settings, latest_basis, open_positions)
    start_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    basis_map = _load_minute_basis_map(storage, start_time)

    summaries: list[dict] = []
    live_rows: list[dict] = []
    trade_rows: list[dict] = []

    basis_result = _evaluate_basis_strategy(live_basis_rows, storage, settings, lookback_days)
    summaries.append(basis_result["summary"])
    live_rows.extend(basis_result["live_rows"])
    trade_rows.extend(basis_result["trade_rows"])

    funding_flip_result = _evaluate_funding_flip_strategy(storage, settings, basis_map, start_time)
    summaries.append(funding_flip_result["summary"])
    live_rows.extend(funding_flip_result["live_rows"])
    trade_rows.extend(funding_flip_result["trade_rows"])

    funding_divergence_result = _evaluate_funding_divergence_strategy(storage, settings, basis_map, start_time)
    summaries.append(funding_divergence_result["summary"])
    live_rows.extend(funding_divergence_result["live_rows"])
    trade_rows.extend(funding_divergence_result["trade_rows"])

    liquidation_result = _evaluate_liquidation_strategy(storage, settings, basis_map, start_time)
    summaries.append(liquidation_result["summary"])
    live_rows.extend(liquidation_result["live_rows"])
    trade_rows.extend(liquidation_result["trade_rows"])

    primary = _pick_primary_strategy(summaries)
    for row in summaries:
        row["is_primary"] = row["strategy_id"] == primary["strategy_id"] if primary is not None else False

    return {
        "lookback_days": lookback_days,
        "primary_strategy": primary,
        "summary_rows": sorted(summaries, key=lambda item: item["label"]),
        "live_rows": sorted(live_rows, key=lambda item: (item["strategy_label"], item["pair"])),
        "trade_rows": sorted(
            trade_rows,
            key=lambda item: item["exit_timestamp"] or item["entry_timestamp"],
            reverse=True,
        )[:100],
    }


def _evaluate_basis_strategy(live_basis_rows: list[dict], storage: CryptoSQLiteStorage, settings, lookback_days: int) -> dict:
    what_if = simulate_strategy_history(storage, settings, lookback_days=lookback_days)
    trade_rows = [
        {
            "strategy_id": STRATEGY_BASIS,
            "strategy_label": STRATEGY_META[STRATEGY_BASIS]["label"],
            "pair": row["pair"],
            "entry_timestamp": None,
            "exit_timestamp": row["exit_timestamp"],
            "side": row["side"],
            "regime": _regime_from_spread_side(row["side"]),
            "gross_pnl_quote": float(row["gross_pnl_quote"]),
            "net_pnl_quote": float(row["net_with_borrow_quote"]),
            "exit_reason": row["exit_reason"],
        }
        for row in what_if["trade_rows"]
    ]
    live_rows = [
        {
            "strategy_id": STRATEGY_BASIS,
            "strategy_label": STRATEGY_META[STRATEGY_BASIS]["label"],
            "pair": row["pair"],
            "status": row["status"],
            "regime": row["regime"],
            "signal_value": row["premium_bps"],
            "edge_value": row["signal_quality_score"],
            "notes": row["mode"],
        }
        for row in live_basis_rows
    ]
    summary = _build_summary(
        strategy_id=STRATEGY_BASIS,
        trade_rows=trade_rows,
        live_rows=live_rows,
    )
    return {"summary": summary, "live_rows": live_rows, "trade_rows": trade_rows}


def _evaluate_funding_flip_strategy(storage: CryptoSQLiteStorage, settings, basis_map: dict, start_time: datetime) -> dict:
    funding_rows = storage.fetch_all(
        """
        SELECT pair, timestamp, current_funding_rate
        FROM crypto_funding
        WHERE venue = 'bybit' AND timestamp >= ? AND current_funding_rate IS NOT NULL
        ORDER BY pair, timestamp
        """,
        (start_time.isoformat(),),
    )
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in funding_rows:
        grouped[str(row["pair"])].append(
            {
                "timestamp": parse_iso(str(row["timestamp"])),
                "rate": float(row["current_funding_rate"]),
            }
        )

    trade_rows: list[dict] = []
    live_rows: list[dict] = []
    for pair_name, rows in grouped.items():
        pair = find_pair(settings, pair_name)
        series = basis_map.get(pair_name)
        if pair is None or series is None or not series["rows"]:
            continue
        previous_rate: float | None = None
        for row in rows:
            if previous_rate is None:
                previous_rate = row["rate"]
                continue
            side = None
            if previous_rate > 0 and row["rate"] <= 0:
                side = "SELL"
            elif previous_rate < 0 and row["rate"] >= 0:
                side = "BUY"
            previous_rate = row["rate"]
            if side is None:
                continue
            entry_basis = _basis_row_at_or_after(series, row["timestamp"])
            exit_basis = _basis_row_at_or_after(series, row["timestamp"] + timedelta(milliseconds=settings.funding_flip_hold_ms))
            if entry_basis is None or exit_basis is None:
                continue
            gross = gross_live_pnl_quote(side, pair.order_size, entry_basis["perp_mid"], exit_basis["perp_mid"])
            net = estimated_net_live_pnl_quote(side, pair.order_size, entry_basis["perp_mid"], exit_basis["perp_mid"], settings)
            trade_rows.append(
                {
                    "strategy_id": STRATEGY_FUNDING_FLIP,
                    "strategy_label": STRATEGY_META[STRATEGY_FUNDING_FLIP]["label"],
                    "pair": pair_name,
                    "entry_timestamp": entry_basis["timestamp"].isoformat(),
                    "exit_timestamp": exit_basis["timestamp"].isoformat(),
                    "side": side,
                    "regime": regime_from_avg_basis(entry_basis["premium_bps"], settings),
                    "gross_pnl_quote": gross,
                    "net_pnl_quote": net,
                    "exit_reason": "TIME_STOP",
                }
            )

        latest = rows[-1]
        previous = rows[-2] if len(rows) >= 2 else None
        status = "MONITOR"
        notes = "No recent funding flip."
        if previous is not None and previous["rate"] > 0 >= latest["rate"]:
            status = "READY_SHORT"
            notes = "Funding crossed from positive to non-positive."
        elif previous is not None and previous["rate"] < 0 <= latest["rate"]:
            status = "READY_LONG"
            notes = "Funding crossed from negative to non-negative."
        elif abs(latest["rate"]) <= settings.funding_divergence_exit_rate:
            status = "WATCH_ZERO_CROSS"
            notes = "Funding is near zero."
        live_rows.append(
            {
                "strategy_id": STRATEGY_FUNDING_FLIP,
                "strategy_label": STRATEGY_META[STRATEGY_FUNDING_FLIP]["label"],
                "pair": pair_name,
                "status": status,
                "regime": regime_from_avg_basis(series["rows"][-1]["premium_bps"], settings),
                "signal_value": latest["rate"] * 10_000.0,
                "edge_value": None,
                "notes": notes,
            }
        )

    summary = _build_summary(
        strategy_id=STRATEGY_FUNDING_FLIP,
        trade_rows=trade_rows,
        live_rows=live_rows,
    )
    return {"summary": summary, "live_rows": live_rows, "trade_rows": trade_rows}


def _evaluate_funding_divergence_strategy(storage: CryptoSQLiteStorage, settings, basis_map: dict, start_time: datetime) -> dict:
    funding_rows = storage.fetch_all(
        """
        SELECT pair, venue, timestamp, current_funding_rate
        FROM crypto_funding
        WHERE timestamp >= ? AND venue IN ('binance', 'bybit') AND current_funding_rate IS NOT NULL
        ORDER BY pair, timestamp
        """,
        (start_time.isoformat(),),
    )
    by_pair: dict[str, dict[str, dict]] = defaultdict(dict)
    latest_snapshot: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in funding_rows:
        pair_name = str(row["pair"])
        timestamp = parse_iso(str(row["timestamp"]))
        bucket = timestamp.replace(second=0, microsecond=0).isoformat()
        bucket_row = by_pair[pair_name].setdefault(bucket, {"timestamp": timestamp})
        bucket_row[str(row["venue"])] = float(row["current_funding_rate"])
        latest_snapshot[pair_name][str(row["venue"])] = {
            "timestamp": timestamp,
            "rate": float(row["current_funding_rate"]),
        }

    trade_rows: list[dict] = []
    live_rows: list[dict] = []
    for pair_name, bucket_map in by_pair.items():
        pair = find_pair(settings, pair_name)
        series = basis_map.get(pair_name)
        if pair is None or series is None or not series["rows"]:
            continue
        open_position: dict | None = None
        for bucket in sorted(bucket_map):
            row = bucket_map[bucket]
            if "binance" not in row or "bybit" not in row:
                continue
            timestamp = row["timestamp"]
            divergence = float(row["bybit"] - row["binance"])
            if open_position is None:
                if abs(divergence) < settings.funding_divergence_entry_rate:
                    continue
                entry_basis = _basis_row_at_or_after(series, timestamp)
                if entry_basis is None:
                    continue
                open_position = {
                    "timestamp": timestamp,
                    "entry_basis": entry_basis,
                    "entry_divergence": divergence,
                    "side": LONG_SPOT_SHORT_PERP if divergence > 0 else SHORT_SPOT_LONG_PERP,
                }
                continue

            should_exit = abs(divergence) <= settings.funding_divergence_exit_rate
            timed_out = timestamp >= open_position["timestamp"] + timedelta(milliseconds=settings.funding_flip_hold_ms)
            if not should_exit and not timed_out:
                continue
            exit_basis = _basis_row_at_or_after(series, timestamp)
            if exit_basis is None:
                continue
            side = str(open_position["side"])
            entry_basis = open_position["entry_basis"]
            hold_ms = (exit_basis["timestamp"] - entry_basis["timestamp"]).total_seconds() * 1000.0
            gross = spread_gross_pnl_quote(
                side,
                pair.order_size,
                entry_basis["spot_mid"],
                entry_basis["premium_bps"],
                exit_basis["premium_bps"],
            )
            net = spread_net_pnl_quote(
                side,
                pair.order_size,
                entry_basis["spot_mid"],
                exit_basis["spot_mid"],
                entry_basis["premium_bps"],
                exit_basis["premium_bps"],
                hold_ms,
                settings,
            )
            trade_rows.append(
                {
                    "strategy_id": STRATEGY_FUNDING_DIVERGENCE,
                    "strategy_label": STRATEGY_META[STRATEGY_FUNDING_DIVERGENCE]["label"],
                    "pair": pair_name,
                    "entry_timestamp": entry_basis["timestamp"].isoformat(),
                    "exit_timestamp": exit_basis["timestamp"].isoformat(),
                    "side": side,
                    "regime": regime_from_avg_basis(entry_basis["premium_bps"], settings),
                    "gross_pnl_quote": gross,
                    "net_pnl_quote": net,
                    "exit_reason": "DIVERGENCE_COMPRESSED" if should_exit else "TIME_STOP",
                }
            )
            open_position = None

        latest = latest_snapshot.get(pair_name, {})
        if "binance" not in latest or "bybit" not in latest:
            continue
        latest_divergence = latest["bybit"]["rate"] - latest["binance"]["rate"]
        status = "MONITOR"
        notes = "Funding spread is inside threshold."
        if latest_divergence >= settings.funding_divergence_entry_rate:
            status = "READY_LONG_SPOT_SHORT_PERP"
            notes = "Bybit funding is richer than Binance."
        elif latest_divergence <= -settings.funding_divergence_entry_rate:
            status = "READY_SHORT_SPOT_LONG_PERP"
            notes = "Binance funding is richer than Bybit."
        live_rows.append(
            {
                "strategy_id": STRATEGY_FUNDING_DIVERGENCE,
                "strategy_label": STRATEGY_META[STRATEGY_FUNDING_DIVERGENCE]["label"],
                "pair": pair_name,
                "status": status,
                "regime": regime_from_avg_basis(series["rows"][-1]["premium_bps"], settings),
                "signal_value": latest_divergence * 10_000.0,
                "edge_value": None,
                "notes": notes,
            }
        )

    summary = _build_summary(
        strategy_id=STRATEGY_FUNDING_DIVERGENCE,
        trade_rows=trade_rows,
        live_rows=live_rows,
    )
    return {"summary": summary, "live_rows": live_rows, "trade_rows": trade_rows}


def _evaluate_liquidation_strategy(storage: CryptoSQLiteStorage, settings, basis_map: dict, start_time: datetime) -> dict:
    oi_rows = storage.fetch_all(
        """
        SELECT pair, timestamp, open_interest, open_interest_value
        FROM crypto_open_interest
        WHERE timestamp >= ?
        ORDER BY pair, timestamp
        """,
        (start_time.isoformat(),),
    )
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in oi_rows:
        pair_name = str(row["pair"])
        grouped[pair_name].append(
            {
                "timestamp": parse_iso(str(row["timestamp"])),
                "open_interest": float(row["open_interest"]),
                "open_interest_value": None
                if row["open_interest_value"] is None
                else float(row["open_interest_value"]),
            }
        )

    trade_rows: list[dict] = []
    live_rows: list[dict] = []
    for pair_name, rows in grouped.items():
        pair = find_pair(settings, pair_name)
        series = basis_map.get(pair_name)
        if pair is None or series is None or not series["rows"]:
            continue
        deduped = _dedupe_rows(rows)
        for previous, current in zip(deduped, deduped[1:]):
            if previous["open_interest"] <= 0:
                continue
            oi_change_pct = (current["open_interest"] - previous["open_interest"]) / previous["open_interest"]
            if oi_change_pct > -settings.liquidation_oi_drop_pct:
                continue
            previous_basis = _basis_row_at_or_after(series, previous["timestamp"])
            entry_basis = _basis_row_at_or_after(series, current["timestamp"])
            exit_basis = _basis_row_at_or_after(
                series,
                current["timestamp"] + timedelta(milliseconds=settings.liquidation_snapback_hold_ms),
            )
            if previous_basis is None or entry_basis is None or exit_basis is None:
                continue
            price_change_pct = (entry_basis["spot_mid"] / previous_basis["spot_mid"]) - 1.0
            if abs(price_change_pct) < settings.liquidation_price_move_pct_min:
                continue
            side = "BUY" if price_change_pct < 0 else "SELL"
            gross = gross_live_pnl_quote(side, pair.order_size, entry_basis["spot_mid"], exit_basis["spot_mid"])
            net = estimated_net_live_pnl_quote(side, pair.order_size, entry_basis["spot_mid"], exit_basis["spot_mid"], settings)
            trade_rows.append(
                {
                    "strategy_id": STRATEGY_LIQUIDATION,
                    "strategy_label": STRATEGY_META[STRATEGY_LIQUIDATION]["label"],
                    "pair": pair_name,
                    "entry_timestamp": entry_basis["timestamp"].isoformat(),
                    "exit_timestamp": exit_basis["timestamp"].isoformat(),
                    "side": side,
                    "regime": regime_from_avg_basis(entry_basis["premium_bps"], settings),
                    "gross_pnl_quote": gross,
                    "net_pnl_quote": net,
                    "exit_reason": "SNAPBACK_TIME_EXIT",
                }
            )

        latest = deduped[-1] if deduped else None
        previous = deduped[-2] if len(deduped) >= 2 else None
        if latest is None or previous is None:
            continue
        oi_change_pct = 0.0 if previous["open_interest"] <= 0 else (latest["open_interest"] - previous["open_interest"]) / previous["open_interest"]
        latest_basis = _basis_row_at_or_after(series, latest["timestamp"])
        previous_basis = _basis_row_at_or_after(series, previous["timestamp"])
        status = "MONITOR"
        notes = "No fresh open-interest cascade."
        signal_value = oi_change_pct * 100.0
        if latest_basis is not None and previous_basis is not None:
            price_change_pct = (latest_basis["spot_mid"] / previous_basis["spot_mid"]) - 1.0
            if oi_change_pct <= -settings.liquidation_oi_drop_pct and abs(price_change_pct) >= settings.liquidation_price_move_pct_min:
                status = "READY_LONG" if price_change_pct < 0 else "READY_SHORT"
                notes = "Open interest dropped sharply over the last 5-minute bar."
            live_rows.append(
                {
                    "strategy_id": STRATEGY_LIQUIDATION,
                    "strategy_label": STRATEGY_META[STRATEGY_LIQUIDATION]["label"],
                    "pair": pair_name,
                    "status": status,
                    "regime": regime_from_avg_basis(latest_basis["premium_bps"], settings),
                    "signal_value": signal_value,
                    "edge_value": price_change_pct * 100.0,
                    "notes": notes,
                }
            )

    summary = _build_summary(
        strategy_id=STRATEGY_LIQUIDATION,
        trade_rows=trade_rows,
        live_rows=live_rows,
    )
    return {"summary": summary, "live_rows": live_rows, "trade_rows": trade_rows}


def _build_summary(strategy_id: str, trade_rows: list[dict], live_rows: list[dict]) -> dict:
    wins = sum(1 for row in trade_rows if float(row["net_pnl_quote"]) > 0)
    trades = len(trade_rows)
    gross_total = sum(float(row["gross_pnl_quote"]) for row in trade_rows)
    net_total = sum(float(row["net_pnl_quote"]) for row in trade_rows)
    live_candidates = sum(1 for row in live_rows if str(row["status"]).startswith("READY"))
    regimes = [str(row["regime"]) for row in trade_rows if row.get("regime")]
    return {
        "strategy_id": strategy_id,
        "label": STRATEGY_META[strategy_id]["label"],
        "category": STRATEGY_META[strategy_id]["category"],
        "trades": trades,
        "wins": wins,
        "win_rate": (wins / trades) if trades else None,
        "gross_pnl_quote": gross_total,
        "net_pnl_quote": net_total,
        "ev_per_trade_quote": (net_total / trades) if trades else None,
        "live_candidates": live_candidates,
        "dominant_regime": _dominant_regime(regimes),
        "status": _summary_status(trades, live_candidates),
    }


def _summary_status(trades: int, live_candidates: int) -> str:
    if live_candidates > 0:
        return "ACTIVE"
    if trades > 0:
        return "TRACKING"
    return "NO_DATA"


def _pick_primary_strategy(summary_rows: list[dict]) -> dict | None:
    eligible = [
        row
        for row in summary_rows
        if row["trades"] > 0 and row["ev_per_trade_quote"] is not None and float(row["ev_per_trade_quote"]) > 0.0
    ]
    if eligible:
        return max(
            eligible,
            key=lambda item: (
                float(item["ev_per_trade_quote"]),
                float(item["net_pnl_quote"]),
                int(item["trades"]),
            ),
        )
    watchlist = [row for row in summary_rows if int(row["live_candidates"]) > 0]
    if watchlist:
        return max(watchlist, key=lambda item: (int(item["live_candidates"]), int(item["trades"])))
    return None


def _load_minute_basis_map(storage: CryptoSQLiteStorage, start_time: datetime) -> dict[str, dict]:
    rows = storage.fetch_all(
        """
        SELECT pair,
               substr(timestamp, 1, 16) || ':00+00:00' AS bucket_start,
               AVG(spot_mid) AS spot_mid,
               AVG(perp_mid) AS perp_mid,
               AVG(premium_bps) AS premium_bps
        FROM crypto_basis
        WHERE timestamp >= ?
        GROUP BY pair, bucket_start
        ORDER BY pair, bucket_start
        """,
        (start_time.isoformat(),),
    )
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row["pair"])].append(
            {
                "timestamp": parse_iso(str(row["bucket_start"])),
                "spot_mid": float(row["spot_mid"]),
                "perp_mid": float(row["perp_mid"]),
                "premium_bps": float(row["premium_bps"]),
            }
        )
    return {
        pair_name: {
            "rows": pair_rows,
            "timestamps": [row["timestamp"] for row in pair_rows],
        }
        for pair_name, pair_rows in grouped.items()
    }


def _basis_row_at_or_after(series: dict, timestamp: datetime) -> dict | None:
    timestamps = series["timestamps"]
    rows = series["rows"]
    index = bisect_left(timestamps, timestamp)
    if index >= len(rows):
        return None
    return rows[index]


def _dedupe_rows(rows: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    for row in rows:
        if deduped and deduped[-1]["timestamp"] == row["timestamp"]:
            deduped[-1] = row
        else:
            deduped.append(row)
    return deduped


def _dominant_regime(regimes: list[str]) -> str:
    if not regimes:
        return "n/a"
    counts = Counter(regimes)
    if len(counts) > 1:
        top = counts.most_common(2)
        if len(top) >= 2 and top[0][1] == top[1][1]:
            return "MIXED"
    return counts.most_common(1)[0][0]


def _regime_from_spread_side(side: str) -> str:
    if side == LONG_SPOT_SHORT_PERP:
        return "CONTANGO"
    if side == SHORT_SPOT_LONG_PERP:
        return "BACKWARDATION"
    return "NEUTRAL"
