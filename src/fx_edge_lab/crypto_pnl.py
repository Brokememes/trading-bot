from __future__ import annotations

from .crypto_models import CryptoResearchSettings


LONG_SPOT_SHORT_PERP = "LONG_SPOT_SHORT_PERP"
SHORT_SPOT_LONG_PERP = "SHORT_SPOT_LONG_PERP"


def entry_cost_quote(quantity: float, fill_price: float, settings: CryptoResearchSettings) -> float:
    return quantity * fill_price * settings.maker_entry_fee_bps / 10_000.0


def exit_cost_quote(quantity: float, exit_price: float, settings: CryptoResearchSettings) -> float:
    if settings.exit_mode == "mid":
        return 0.0
    cost_bps = settings.exit_fee_bps + settings.exit_slippage_bps
    return quantity * exit_price * cost_bps / 10_000.0


def borrow_cost_quote(
    quantity: float,
    spot_reference_price: float,
    hold_ms: float,
    borrow_apy: float,
) -> float:
    if hold_ms <= 0 or borrow_apy <= 0:
        return 0.0
    hold_days = hold_ms / 1000.0 / 60.0 / 60.0 / 24.0
    return quantity * spot_reference_price * borrow_apy * (hold_days / 365.0)


def cost_assumptions(settings: CryptoResearchSettings) -> dict[str, float | str | bool]:
    total_fee_pct = (settings.maker_entry_fee_bps + settings.exit_fee_bps + settings.exit_slippage_bps) / 10_000.0
    return {
        "fee_preset": settings.fee_preset,
        "exit_mode": settings.exit_mode,
        "maker_entry_fee_bps": settings.maker_entry_fee_bps,
        "exit_fee_bps": settings.exit_fee_bps,
        "exit_slippage_bps": settings.exit_slippage_bps,
        "total_fee_pct": total_fee_pct,
        "basis_consecutive_samples_required": settings.basis_consecutive_samples_required,
        "pre_funding_window_ms": settings.pre_funding_window_ms,
        "pre_funding_basis_threshold_bps": settings.pre_funding_basis_threshold_bps,
        "pre_funding_trend_window_ms": settings.pre_funding_trend_window_ms,
        "reverse_spot_borrow_apy": settings.reverse_spot_borrow_apy,
        "include_funding_in_pnl": settings.include_funding_in_pnl,
    }


def gross_live_pnl_quote(side: str, quantity: float, fill_price: float, current_mid: float) -> float:
    if side == "BUY":
        return quantity * (current_mid - fill_price)
    return quantity * (fill_price - current_mid)


def estimated_net_live_pnl_quote(
    side: str,
    quantity: float,
    fill_price: float,
    current_mid: float,
    settings: CryptoResearchSettings,
) -> float:
    gross = gross_live_pnl_quote(side, quantity, fill_price, current_mid)
    return gross - entry_cost_quote(quantity, fill_price, settings) - exit_cost_quote(
        quantity,
        current_mid,
        settings,
    )


def markout_to_exit_price(side: str, reference_price: float, markout_bps: float) -> float:
    if side == "BUY":
        return reference_price * (1.0 + (markout_bps / 10_000.0))
    denominator = 1.0 + (markout_bps / 10_000.0)
    if denominator == 0:
        return reference_price
    return reference_price / denominator


def gross_markout_pnl_quote(quantity: float, reference_price: float, markout_bps: float) -> float:
    return quantity * reference_price * markout_bps / 10_000.0


def estimated_net_markout_pnl_quote(
    side: str,
    quantity: float,
    reference_price: float,
    markout_bps: float,
    settings: CryptoResearchSettings,
) -> float:
    exit_price = markout_to_exit_price(side, reference_price, markout_bps)
    gross = gross_markout_pnl_quote(quantity, reference_price, markout_bps)
    return gross - entry_cost_quote(quantity, reference_price, settings) - exit_cost_quote(
        quantity,
        exit_price,
        settings,
    )


def spread_direction_multiplier(side: str) -> float:
    if side == LONG_SPOT_SHORT_PERP:
        return 1.0
    if side == SHORT_SPOT_LONG_PERP:
        return -1.0
    raise ValueError(f"Unsupported spread side '{side}'")


def target_exit_basis_bps(side: str, basis_exit_threshold_bps: float) -> float:
    if side == LONG_SPOT_SHORT_PERP:
        return basis_exit_threshold_bps
    if side == SHORT_SPOT_LONG_PERP:
        return -basis_exit_threshold_bps
    raise ValueError(f"Unsupported spread side '{side}'")


def spread_edge_pct(side: str, entry_basis_bps: float, exit_basis_bps: float) -> float:
    return spread_direction_multiplier(side) * (entry_basis_bps - exit_basis_bps) / 10_000.0


def spread_target_edge_pct(side: str, entry_basis_bps: float, basis_exit_threshold_bps: float) -> float:
    return max(spread_edge_pct(side, entry_basis_bps, target_exit_basis_bps(side, basis_exit_threshold_bps)), 0.0)


def spread_gross_pnl_quote(
    side: str,
    quantity: float,
    spot_reference_price: float,
    entry_basis_bps: float,
    exit_basis_bps: float,
) -> float:
    return quantity * spot_reference_price * spread_edge_pct(side, entry_basis_bps, exit_basis_bps)


def spread_net_pnl_quote(
    side: str,
    quantity: float,
    spot_reference_price: float,
    exit_reference_price: float,
    entry_basis_bps: float,
    exit_basis_bps: float,
    hold_ms: float,
    settings: CryptoResearchSettings,
    include_borrow_cost: bool = True,
) -> float:
    gross = spread_gross_pnl_quote(
        side,
        quantity,
        spot_reference_price,
        entry_basis_bps,
        exit_basis_bps,
    )
    borrow_quote = 0.0
    if include_borrow_cost and side == SHORT_SPOT_LONG_PERP:
        borrow_quote = borrow_cost_quote(
            quantity,
            spot_reference_price,
            hold_ms,
            settings.reverse_spot_borrow_apy,
        )
    return (
        gross
        - entry_cost_quote(quantity, spot_reference_price, settings)
        - exit_cost_quote(quantity, exit_reference_price, settings)
        - borrow_quote
    )


def signal_quality_score(gross_edge_pct: float, settings: CryptoResearchSettings) -> float | None:
    total_fees_pct = (settings.maker_entry_fee_bps + settings.exit_fee_bps + settings.exit_slippage_bps) / 10_000.0
    if total_fees_pct <= 0:
        return None
    return (gross_edge_pct - total_fees_pct) / total_fees_pct


def signal_quality_band(score: float | None) -> str:
    if score is None:
        return "UNBOUNDED"
    if score < 0:
        return "DO_NOT_TRADE"
    if score < 0.5:
        return "PAPER_ONLY"
    if score < 1.0:
        return "SMALL_LIVE"
    return "SCALE_UP"
