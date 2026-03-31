from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SignalViews:
    residual_view: str
    basis_view: str
    aligned_trade: str


def classify_residual(residual_pct_value: float, threshold_pct: float) -> str:
    if residual_pct_value >= threshold_pct:
        return "EUR_RICH"
    if residual_pct_value <= -threshold_pct:
        return "EUR_CHEAP"
    return "NEUTRAL"


def classify_basis(gap_pct_value: float, threshold_pct: float) -> str:
    if gap_pct_value >= threshold_pct:
        return "FUTURES_RICH"
    if gap_pct_value <= -threshold_pct:
        return "FUTURES_CHEAP"
    return "NEUTRAL"


def combine_views(
    residual_pct_value: float,
    gap_pct_value: float,
    residual_threshold_bp: float,
    gap_threshold_bp: float,
) -> SignalViews:
    residual_view = classify_residual(
        residual_pct_value=residual_pct_value,
        threshold_pct=residual_threshold_bp / 10_000.0,
    )
    basis_view = classify_basis(
        gap_pct_value=gap_pct_value,
        threshold_pct=gap_threshold_bp / 10_000.0,
    )

    if residual_view == "EUR_RICH" and basis_view == "FUTURES_RICH":
        aligned_trade = "SHORT_EUR"
    elif residual_view == "EUR_CHEAP" and basis_view == "FUTURES_CHEAP":
        aligned_trade = "LONG_EUR"
    else:
        aligned_trade = "MIXED"

    return SignalViews(
        residual_view=residual_view,
        basis_view=basis_view,
        aligned_trade=aligned_trade,
    )
