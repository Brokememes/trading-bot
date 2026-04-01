from __future__ import annotations

import json
from pathlib import Path

from .crypto_models import CryptoPairSettings, CryptoResearchSettings

FEE_PRESETS = {
    "custom": {
        "label": "Custom Manual Fees",
        "maker_fee_bps": None,
        "taker_fee_bps": None,
    },
    "binance_spot_regular": {
        "label": "Binance Spot Regular User",
        "maker_fee_bps": 10.0,
        "taker_fee_bps": 10.0,
    },
    "binance_spot_regular_bnb": {
        "label": "Binance Spot Regular User (BNB Discount)",
        "maker_fee_bps": 7.5,
        "taker_fee_bps": 7.5,
    },
    "bybit_spot_vip0": {
        "label": "Bybit Spot VIP 0",
        "maker_fee_bps": 10.0,
        "taker_fee_bps": 10.0,
    },
    "bybit_linear_vip0": {
        "label": "Bybit Perpetual & Futures VIP 0",
        "maker_fee_bps": 2.0,
        "taker_fee_bps": 5.5,
    },
}


DEFAULT_CRYPTO_PAIRS = (
    CryptoPairSettings(
        name="BTCUSDT",
        binance_spot_symbol="btcusdt",
        bybit_linear_symbol="BTCUSDT",
        order_size=0.001,
    ),
    CryptoPairSettings(
        name="ETHUSDT",
        binance_spot_symbol="ethusdt",
        bybit_linear_symbol="ETHUSDT",
        order_size=0.01,
    ),
    CryptoPairSettings(
        name="SOLUSDT",
        binance_spot_symbol="solusdt",
        bybit_linear_symbol="SOLUSDT",
        order_size=0.1,
    ),
    CryptoPairSettings(
        name="BNBUSDT",
        binance_spot_symbol="bnbusdt",
        bybit_linear_symbol="BNBUSDT",
        order_size=0.01,
    ),
    CryptoPairSettings(
        name="DOGEUSDT",
        binance_spot_symbol="dogeusdt",
        bybit_linear_symbol="DOGEUSDT",
        order_size=10.0,
    ),
    CryptoPairSettings(
        name="AVAXUSDT",
        binance_spot_symbol="avaxusdt",
        bybit_linear_symbol="AVAXUSDT",
        order_size=0.2,
    ),
    CryptoPairSettings(
        name="TRXUSDT",
        binance_spot_symbol="trxusdt",
        bybit_linear_symbol="TRXUSDT",
        order_size=50.0,
    ),
)


def default_crypto_settings() -> CryptoResearchSettings:
    return CryptoResearchSettings(pairs=DEFAULT_CRYPTO_PAIRS)


def load_crypto_settings(config_path: str | Path | None) -> CryptoResearchSettings:
    settings = default_crypto_settings()
    if config_path is None:
        return settings

    data = json.loads(Path(config_path).read_text(encoding="utf-8"))
    pairs_data = data.get("pairs")
    pairs = (
        tuple(_pair_from_dict(item) for item in pairs_data)
        if pairs_data is not None
        else settings.pairs
    )
    horizons = tuple(int(item) for item in data.get("analysis_horizons_ms", settings.analysis_horizons_ms))
    fee_preset = str(data.get("fee_preset", settings.fee_preset)).strip().lower()
    exit_mode = str(data.get("exit_mode", settings.exit_mode)).strip().lower()
    if fee_preset not in FEE_PRESETS:
        supported = ", ".join(sorted(FEE_PRESETS))
        raise ValueError(f"Unsupported crypto fee preset '{fee_preset}'. Supported: {supported}")
    if exit_mode not in {"mid", "maker", "taker"}:
        raise ValueError("crypto exit_mode must be one of: mid, maker, taker")

    resolved_entry_fee_bps, resolved_exit_fee_bps, resolved_exit_slippage_bps = _resolve_fee_settings(
        fee_preset=fee_preset,
        exit_mode=exit_mode,
        maker_entry_fee_bps=data.get("maker_entry_fee_bps"),
        exit_fee_bps=data.get("exit_fee_bps"),
        exit_slippage_bps=data.get("exit_slippage_bps"),
        defaults=settings,
    )

    return CryptoResearchSettings(
        database_path=str(data.get("database_path", settings.database_path)),
        binance_stream_url=str(data.get("binance_stream_url", settings.binance_stream_url)),
        bybit_linear_url=str(data.get("bybit_linear_url", settings.bybit_linear_url)),
        quote_throttle_ms=int(data.get("quote_throttle_ms", settings.quote_throttle_ms)),
        imbalance_levels=int(data.get("imbalance_levels", settings.imbalance_levels)),
        storage_depth_levels=int(data.get("storage_depth_levels", settings.storage_depth_levels)),
        basis_sample_interval_ms=int(
            data.get("basis_sample_interval_ms", settings.basis_sample_interval_ms)
        ),
        funding_poll_interval_ms=int(
            data.get("funding_poll_interval_ms", settings.funding_poll_interval_ms)
        ),
        funding_history_limit=int(data.get("funding_history_limit", settings.funding_history_limit)),
        analysis_horizons_ms=horizons,
        fee_preset=fee_preset,
        exit_mode=exit_mode,
        maker_entry_fee_bps=resolved_entry_fee_bps,
        exit_fee_bps=resolved_exit_fee_bps,
        exit_slippage_bps=resolved_exit_slippage_bps,
        equity_curve_horizon_ms=int(data.get("equity_curve_horizon_ms", settings.equity_curve_horizon_ms)),
        regime_window_ms=int(data.get("regime_window_ms", settings.regime_window_ms)),
        regime_contango_bps=float(data.get("regime_contango_bps", settings.regime_contango_bps)),
        regime_backwardation_bps=float(data.get("regime_backwardation_bps", settings.regime_backwardation_bps)),
        basis_consecutive_samples_required=int(
            data.get("basis_consecutive_samples_required", settings.basis_consecutive_samples_required)
        ),
        pre_funding_window_ms=int(data.get("pre_funding_window_ms", settings.pre_funding_window_ms)),
        pre_funding_basis_threshold_bps=float(
            data.get("pre_funding_basis_threshold_bps", settings.pre_funding_basis_threshold_bps)
        ),
        pre_funding_trend_window_ms=int(
            data.get("pre_funding_trend_window_ms", settings.pre_funding_trend_window_ms)
        ),
        reverse_spot_borrow_apy=float(data.get("reverse_spot_borrow_apy", settings.reverse_spot_borrow_apy)),
        include_funding_in_pnl=bool(data.get("include_funding_in_pnl", settings.include_funding_in_pnl)),
        strategy_lookback_days=int(data.get("strategy_lookback_days", settings.strategy_lookback_days)),
        funding_divergence_entry_rate=float(
            data.get("funding_divergence_entry_rate", settings.funding_divergence_entry_rate)
        ),
        funding_divergence_exit_rate=float(
            data.get("funding_divergence_exit_rate", settings.funding_divergence_exit_rate)
        ),
        funding_flip_hold_ms=int(data.get("funding_flip_hold_ms", settings.funding_flip_hold_ms)),
        liquidation_oi_drop_pct=float(data.get("liquidation_oi_drop_pct", settings.liquidation_oi_drop_pct)),
        liquidation_price_move_pct_min=float(
            data.get("liquidation_price_move_pct_min", settings.liquidation_price_move_pct_min)
        ),
        liquidation_snapback_hold_ms=int(
            data.get("liquidation_snapback_hold_ms", settings.liquidation_snapback_hold_ms)
        ),
        pairs=pairs,
    )


def _pair_from_dict(data: dict) -> CryptoPairSettings:
    return CryptoPairSettings(
        name=str(data["name"]),
        binance_spot_symbol=str(data["binance_spot_symbol"]).lower(),
        bybit_linear_symbol=str(data["bybit_linear_symbol"]).upper(),
        maker_venue=str(data.get("maker_venue", "binance")),
        maker_market_type=str(data.get("maker_market_type", "spot")),
        signal_source=str(data.get("signal_source", "binance_spot")),
        imbalance_threshold=float(data.get("imbalance_threshold", 0.35)),
        max_spread_bps=float(data.get("max_spread_bps", 3.0)),
        maker_order_ttl_ms=int(data.get("maker_order_ttl_ms", 2000)),
        signal_cooldown_ms=int(data.get("signal_cooldown_ms", 1000)),
        order_size=float(data.get("order_size", 0.001)),
        extreme_basis_size_fraction=float(data.get("extreme_basis_size_fraction", 0.5)),
        basis_entry_threshold_bps=float(data.get("basis_entry_threshold_bps", 80.0)),
        basis_entry_threshold_low_bps=float(data.get("basis_entry_threshold_low_bps", 60.0)),
        basis_entry_threshold_high_bps=float(data.get("basis_entry_threshold_high_bps", 120.0)),
        basis_exit_threshold_bps=float(data.get("basis_exit_threshold_bps", 30.0)),
        funding_entry_min_rate=float(data.get("funding_entry_min_rate", 0.0005)),
        funding_exit_rate=float(data.get("funding_exit_rate", 0.0)),
        basis_momentum_window_ms=int(data.get("basis_momentum_window_ms", 180000)),
        max_hold_ms=int(data.get("max_hold_ms", 28800000)),
    )


def _resolve_fee_settings(
    *,
    fee_preset: str,
    exit_mode: str,
    maker_entry_fee_bps,
    exit_fee_bps,
    exit_slippage_bps,
    defaults: CryptoResearchSettings,
) -> tuple[float, float, float]:
    preset = FEE_PRESETS[fee_preset]
    maker_fee = defaults.maker_entry_fee_bps if preset["maker_fee_bps"] is None else float(preset["maker_fee_bps"])
    taker_fee = defaults.exit_fee_bps if preset["taker_fee_bps"] is None else float(preset["taker_fee_bps"])

    resolved_entry_fee_bps = maker_fee if maker_entry_fee_bps is None else float(maker_entry_fee_bps)
    if exit_mode == "mid":
        return resolved_entry_fee_bps, 0.0, 0.0

    if exit_fee_bps is not None:
        resolved_exit_fee_bps = float(exit_fee_bps)
    elif exit_mode == "maker":
        resolved_exit_fee_bps = maker_fee
    elif exit_mode == "taker":
        resolved_exit_fee_bps = taker_fee
    else:
        resolved_exit_fee_bps = 0.0

    if exit_slippage_bps is not None:
        resolved_exit_slippage_bps = float(exit_slippage_bps)
    elif exit_mode in {"mid", "maker"}:
        resolved_exit_slippage_bps = 0.0
    else:
        resolved_exit_slippage_bps = defaults.exit_slippage_bps

    return resolved_entry_fee_bps, resolved_exit_fee_bps, resolved_exit_slippage_bps
