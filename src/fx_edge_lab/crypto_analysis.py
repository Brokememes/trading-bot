from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .crypto_insights import (
    build_live_signal_rows,
    count_strategy_signals,
    fetch_latest_basis,
    load_spread_positions,
    pre_funding_state,
    simulate_strategy_history,
    spike_forensics_rows,
)
from .crypto_pnl import cost_assumptions
from .crypto_settings import default_crypto_settings
from .crypto_storage import CryptoSQLiteStorage
from .crypto_strategy_lab import build_strategy_lab


def summarize_crypto_database(
    database_path: str | Path,
    settings=None,
) -> str:
    settings = settings or default_crypto_settings()
    storage = CryptoSQLiteStorage(str(database_path))
    try:
        latest_basis = fetch_latest_basis(storage)
        positions = load_spread_positions(storage, latest_basis, settings)
        open_positions = [row for row in positions if row["status"] == "OPEN"]
        closed_positions = [row for row in positions if row["status"] == "CLOSED"]
        live_signals = build_live_signal_rows(storage, settings, latest_basis, open_positions)
        what_if = simulate_strategy_history(storage, settings, lookback_days=7)
        strategy_lab = build_strategy_lab(
            storage,
            settings,
            latest_basis=latest_basis,
            lookback_days=settings.strategy_lookback_days,
        )
        reference_time = (
            max(datetime.fromisoformat(str(row["timestamp"])) for row in latest_basis)
            if latest_basis
            else None
        )
        funding_clock = None if reference_time is None else pre_funding_state(reference_time, settings)
        spike_rows = spike_forensics_rows(storage, settings, lookback_days=7, threshold_bps=60.0)[:10]
        counts = {
            "quotes": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_quotes")[0]["n"]),
            "trades": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_trades")[0]["n"]),
            "funding": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_funding")[0]["n"]),
            "open_interest": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_open_interest")[0]["n"]),
            "basis": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_basis")[0]["n"]),
            "signals": count_strategy_signals(storage),
            "spreads": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_spread_positions")[0]["n"]),
        }
    finally:
        storage.close()

    assumptions = cost_assumptions(settings)
    lines = [
        "Crypto Research Summary",
        f"quotes={counts['quotes']} trades={counts['trades']} funding={counts['funding']} "
        f"open_interest={counts['open_interest']} basis={counts['basis']} signals={counts['signals']} spreads={counts['spreads']} "
        f"open_spreads={len(open_positions)} closed_spreads={len(closed_positions)}",
        "",
        "Cost Assumptions",
        f"preset={assumptions['fee_preset']} exit_mode={assumptions['exit_mode']} "
        f"entry={_fmt(assumptions['maker_entry_fee_bps'])}bps "
        f"exit_fee={_fmt(assumptions['exit_fee_bps'])}bps "
        f"exit_slippage={_fmt(assumptions['exit_slippage_bps'])}bps "
        f"borrow_apy={_fmt_pct(assumptions['reverse_spot_borrow_apy'])}",
        "",
        "Strategy Leadership",
    ]

    primary = strategy_lab["primary_strategy"]
    if primary is None:
        lines.append("No primary strategy yet.")
    else:
        lines.append(
            f"primary={primary['label']} category={primary['category']} status={primary['status']} "
            f"trades={primary['trades']} winRate={_fmt_ratio(primary['win_rate'])} "
            f"evPerTrade={_fmt_quote(primary['ev_per_trade_quote'])}"
        )

    lines.extend([
        "",
        "Strategy Scoreboard",
    ])

    if strategy_lab["summary_rows"]:
        for row in strategy_lab["summary_rows"]:
            lines.append(
                f"{row['label']} category={row['category']} status={row['status']} "
                f"trades={row['trades']} wins={row['wins']} winRate={_fmt_ratio(row['win_rate'])} "
                f"gross={_fmt_quote(row['gross_pnl_quote'])} net={_fmt_quote(row['net_pnl_quote'])} "
                f"evPerTrade={_fmt_quote(row['ev_per_trade_quote'])} liveCandidates={row['live_candidates']} "
                f"dominantRegime={row['dominant_regime']}"
            )
    else:
        lines.append("No strategy rows yet.")

    lines.extend([
        "",
        "Strategy Live Board",
    ])

    if strategy_lab["live_rows"]:
        for row in strategy_lab["live_rows"]:
            lines.append(
                f"{row['strategy_label']} {row['pair']} status={row['status']} regime={row['regime']} "
                f"signal={_fmt(row['signal_value'])} edge={_fmt(row['edge_value'])} notes={row['notes']}"
            )
    else:
        lines.append("No live strategy rows yet.")

    lines.extend([
        "",
        "Pre-Funding Alert",
    ])

    if funding_clock is None:
        lines.append("No funding clock available yet.")
    else:
        lines.append(
            f"nextFunding={funding_clock['next_funding_time']} countdownMin={_fmt((funding_clock['countdown_ms'] or 0.0) / 60000.0)} "
            f"alert={'ON' if funding_clock['alert_active'] else 'OFF'} "
            f"prefundingTier2={_fmt_bps(settings.pre_funding_basis_threshold_bps)} "
            f"durationSamples={settings.basis_consecutive_samples_required}"
        )

    lines.extend([
        "",
        "Live Signal Board",
    ])

    if live_signals:
        for row in live_signals:
            lines.append(
                f"{row['pair']} regime={row['regime']} mode={row['mode']} status={row['status']} "
                f"basis={_fmt_bps(row['premium_bps'])} 1hAvg={_fmt_bps(row['regime_avg_basis_bps'])} "
                f"10mTrend={_fmt_bps(row['basis_trend_10m_bps'])} 3mMom={_fmt_bps(row['momentum_bps'])} "
                f"funding={_fmt_rate(row['current_funding_rate'])} "
                f"tier1={_fmt_bps(row['active_threshold_bps'])} tier2={_fmt_bps(row['basis_only_threshold_bps'])} "
                f"tier1Run={row['tier1_duration_count']} tier2Run={row['tier2_duration_count']} "
                f"prefunding={'ON' if row['pre_funding_alert_active'] else 'OFF'} "
                f"changesToday={row['regime_changes_today']} "
                f"quality={_fmt(row['signal_quality_score'])} band={row['signal_quality_band']}"
            )
    else:
        lines.append("No live basis rows recorded yet.")

    lines.extend(["", "Open Spread Positions"])
    if open_positions:
        for row in open_positions:
            lines.append(
                f"{row['pair']} side={row['side']} mode={row['entry_mode']} "
                f"entryBasis={_fmt_bps(row['entry_basis_bps'])} liveBasis={_fmt_bps(row['live_basis_bps'])} "
                f"gross={_fmt_quote(row['live_gross_pnl_quote'])} "
                f"netNoBorrow={_fmt_quote(row['live_net_without_borrow_quote'])} "
                f"netWithBorrow={_fmt_quote(row['live_net_with_borrow_quote'])} "
                f"borrow={_fmt_quote(row['borrow_cost_quote'])}"
            )
    else:
        lines.append("No open spread positions.")

    lines.extend(["", "Closed Spread Performance"])
    if closed_positions:
        grouped: dict[str, dict[str, float]] = {}
        for row in closed_positions:
            pair = str(row["pair"])
            bucket = grouped.setdefault(
                pair,
                {"trades": 0.0, "gross": 0.0, "net_with": 0.0},
            )
            bucket["trades"] += 1
            bucket["gross"] += float(row["live_gross_pnl_quote"] or 0.0)
            bucket["net_with"] += float(row["live_net_with_borrow_quote"] or 0.0)
        for pair in sorted(grouped):
            bucket = grouped[pair]
            lines.append(
                f"{pair} trades={int(bucket['trades'])} gross={_fmt_quote(bucket['gross'])} "
                f"netWithBorrow={_fmt_quote(bucket['net_with'])}"
            )
    else:
        lines.append("No closed spread positions yet.")

    lines.extend(["", "Latest Basis"])
    if latest_basis:
        for row in latest_basis:
            lines.append(
                f"{row['pair']} premium={_fmt_bps(row['premium_bps'])} "
                f"funding={_fmt_rate(row['current_funding_rate'])} avgFunding={_fmt_rate(row['average_funding_rate'])}"
            )
    else:
        lines.append("No basis snapshots recorded yet.")

    lines.extend(["", "7-Day What-If"])
    if what_if["summary_rows"]:
        for row in what_if["summary_rows"]:
            lines.append(
                f"{row['pair']} longHits60={row['long_hits_60bps']} reverseHits60={row['reverse_hits_60bps']} "
                f"trades={row['what_if_trades']} gross={_fmt_quote(row['what_if_gross_pnl_quote'])} "
                f"netNoBorrow={_fmt_quote(row['what_if_net_without_borrow_quote'])} "
                f"netWithBorrow={_fmt_quote(row['what_if_net_with_borrow_quote'])}"
            )
    else:
        lines.append("No 7-day what-if trades yet.")

    lines.extend(["", "Spike Forensics"])
    if spike_rows:
        for row in spike_rows:
            lines.append(
                f"{row['pair']} peak={row['peak_timestamp']} basis={_fmt_bps(row['peak_basis_bps'])} "
                f"dir={row['direction']} samples={row['samples']} durationMs={_fmt(row['duration_ms'])} "
                f"fundingPeak={_fmt_bps(row['funding_peak_bps'])} basis5m={_fmt_bps(row['basis_5m_bps'])} "
                f"basis15m={_fmt_bps(row['basis_15m_bps'])} basis30m={_fmt_bps(row['basis_30m_bps'])}"
            )
    else:
        lines.append("No spike episodes >= 60bps found.")

    return "\n".join(lines)


def _fmt(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100.0:.2f}%"


def _fmt_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 10_000.0:.2f}bps"


def _fmt_quote(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def _fmt_bps(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}bps"


def _fmt_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100.0:.2f}%"
