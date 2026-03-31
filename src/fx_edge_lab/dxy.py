from __future__ import annotations

from dataclasses import dataclass
from math import log
from statistics import fmean, pstdev
from typing import Sequence

DXY_COEFFICIENT = 50.14348112


@dataclass(frozen=True)
class DxySnapshot:
    timestamp: str
    dxy: float
    eurusd: float
    usdjpy: float
    gbpusd: float
    usdcad: float
    usdsek: float
    usdchf: float


def dxy_from_components(
    eurusd: float,
    usdjpy: float,
    gbpusd: float,
    usdcad: float,
    usdsek: float,
    usdchf: float,
) -> float:
    return DXY_COEFFICIENT * (
        (eurusd ** -0.576)
        * (usdjpy ** 0.136)
        * (gbpusd ** -0.119)
        * (usdcad ** 0.091)
        * (usdsek ** 0.042)
        * (usdchf ** 0.036)
    )


def implied_eurusd(
    dxy: float,
    usdjpy: float,
    gbpusd: float,
    usdcad: float,
    usdsek: float,
    usdchf: float,
) -> float:
    other_legs = (
        (usdjpy ** 0.136)
        * (gbpusd ** -0.119)
        * (usdcad ** 0.091)
        * (usdsek ** 0.042)
        * (usdchf ** 0.036)
    )
    return ((DXY_COEFFICIENT * other_legs) / dxy) ** (1 / 0.576)


def residual_pct(actual_eurusd: float, implied_eurusd_value: float) -> float:
    return (actual_eurusd / implied_eurusd_value) - 1.0


def residual_log(actual_eurusd: float, implied_eurusd_value: float) -> float:
    return log(actual_eurusd / implied_eurusd_value)


def rolling_zscores(values: Sequence[float], window: int) -> list[float | None]:
    if window < 2:
        raise ValueError("window must be at least 2")

    zscores: list[float | None] = []
    for index, value in enumerate(values):
        history = list(values[max(0, index - window) : index])
        if len(history) < window:
            zscores.append(None)
            continue

        sigma = pstdev(history)
        if sigma == 0:
            zscores.append(0.0)
            continue

        zscores.append((value - fmean(history)) / sigma)
    return zscores
