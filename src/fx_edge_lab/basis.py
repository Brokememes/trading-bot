from __future__ import annotations

from dataclasses import dataclass
from math import exp, log


@dataclass(frozen=True)
class BasisMetrics:
    fair_future: float
    observed_basis: float
    fair_basis: float
    gap: float
    gap_pct: float
    implied_annualized_carry: float | None
    model_annualized_carry: float | None
    carry_gap: float | None


def fair_future_price(
    spot: float,
    quote_rate_pct: float,
    base_rate_pct: float,
    days_to_expiry: float,
    day_count: float = 365.0,
) -> float:
    if days_to_expiry <= 0:
        return spot

    time_fraction = days_to_expiry / day_count
    quote_rate = quote_rate_pct / 100.0
    base_rate = base_rate_pct / 100.0
    return spot * exp((quote_rate - base_rate) * time_fraction)


def basis_metrics(
    spot: float,
    future: float,
    quote_rate_pct: float,
    base_rate_pct: float,
    days_to_expiry: float,
    day_count: float = 365.0,
) -> BasisMetrics:
    fair_future = fair_future_price(
        spot=spot,
        quote_rate_pct=quote_rate_pct,
        base_rate_pct=base_rate_pct,
        days_to_expiry=days_to_expiry,
        day_count=day_count,
    )
    observed_basis = future - spot
    fair_basis = fair_future - spot
    gap = future - fair_future
    gap_pct = (future / fair_future) - 1.0

    if days_to_expiry <= 0:
        return BasisMetrics(
            fair_future=fair_future,
            observed_basis=observed_basis,
            fair_basis=fair_basis,
            gap=gap,
            gap_pct=gap_pct,
            implied_annualized_carry=None,
            model_annualized_carry=None,
            carry_gap=None,
        )

    time_fraction = days_to_expiry / day_count
    implied_carry = log(future / spot) / time_fraction
    model_carry = (quote_rate_pct - base_rate_pct) / 100.0
    return BasisMetrics(
        fair_future=fair_future,
        observed_basis=observed_basis,
        fair_basis=fair_basis,
        gap=gap,
        gap_pct=gap_pct,
        implied_annualized_carry=implied_carry,
        model_annualized_carry=model_carry,
        carry_gap=implied_carry - model_carry,
    )
