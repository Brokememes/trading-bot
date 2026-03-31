from __future__ import annotations

import json
from pathlib import Path

from .models import AppSettings, FuturesProviderSettings, Mt5Settings, PairSettings, TelegramSettings


DEFAULT_PAIRS = (
    PairSettings(
        name="EURUSD",
        spot_symbol="EURUSD",
        futures_symbol="6EM26",
        pip_size=0.0001,
        threshold_pips=1.5,
        normalization="identity",
        ib_contract={
            "symbol": "EUR",
            "exchange": "CME",
            "currency": "USD",
            "lastTradeDateOrContractMonth": "202606",
            "localSymbol": "6EM6",
        },
    ),
    PairSettings(
        name="GBPUSD",
        spot_symbol="GBPUSD",
        futures_symbol="6BM26",
        pip_size=0.0001,
        threshold_pips=1.5,
        normalization="identity",
        ib_contract={
            "symbol": "GBP",
            "exchange": "CME",
            "currency": "USD",
            "lastTradeDateOrContractMonth": "202606",
            "localSymbol": "6BM6",
        },
    ),
    PairSettings(
        name="XAUUSD",
        spot_symbol="XAUUSD",
        futures_symbol="GCM26",
        pip_size=0.01,
        threshold_pips=50.0,
        normalization="identity",
        close_tolerance_pips=5.0,
        cooldown_seconds=20.0,
        ib_contract={
            "symbol": "GC",
            "exchange": "COMEX",
            "currency": "USD",
            "lastTradeDateOrContractMonth": "202606",
            "localSymbol": "GCM6",
        },
    ),
    PairSettings(
        name="USDJPY",
        spot_symbol="USDJPY",
        futures_symbol="6JM26",
        pip_size=0.01,
        threshold_pips=1.5,
        normalization="invert",
        ib_contract={
            "symbol": "JPY",
            "exchange": "CME",
            "currency": "USD",
            "lastTradeDateOrContractMonth": "202606",
            "localSymbol": "6JM6",
        },
    ),
)


def default_settings() -> AppSettings:
    return AppSettings(pairs=DEFAULT_PAIRS)


def load_settings(config_path: str | Path | None) -> AppSettings:
    settings = default_settings()
    if config_path is None:
        return settings

    data = json.loads(Path(config_path).read_text(encoding="utf-8"))
    mt5_data = data.get("mt5", {})
    telegram_data = data.get("telegram", {})
    futures_data = data.get("futures_provider", {})
    pairs_data = data.get("pairs")

    pairs = (
        tuple(_pair_from_dict(item) for item in pairs_data)
        if pairs_data is not None
        else settings.pairs
    )

    return AppSettings(
        database_path=data.get("database_path", settings.database_path),
        execute_trades=bool(data.get("execute_trades", settings.execute_trades)),
        dry_run=bool(data.get("dry_run", settings.dry_run)),
        stale_after_ms=int(data.get("stale_after_ms", settings.stale_after_ms)),
        mt5=Mt5Settings(
            path=mt5_data.get("path", settings.mt5.path),
            login=mt5_data.get("login", settings.mt5.login),
            password=mt5_data.get("password", settings.mt5.password),
            server=mt5_data.get("server", settings.mt5.server),
            initialize_timeout_ms=int(
                mt5_data.get("initialize_timeout_ms", settings.mt5.initialize_timeout_ms)
            ),
            poll_interval_ms=int(mt5_data.get("poll_interval_ms", settings.mt5.poll_interval_ms)),
            deviation_points=int(mt5_data.get("deviation_points", settings.mt5.deviation_points)),
            magic=int(mt5_data.get("magic", settings.mt5.magic)),
        ),
        telegram=TelegramSettings(
            enabled=bool(telegram_data.get("enabled", settings.telegram.enabled)),
            bot_token=telegram_data.get("bot_token", settings.telegram.bot_token),
            chat_id=telegram_data.get("chat_id", settings.telegram.chat_id),
        ),
        futures_provider=FuturesProviderSettings(
            kind=str(futures_data.get("kind", settings.futures_provider.kind)),
            api_key=futures_data.get("api_key", settings.futures_provider.api_key),
            polygon_url=str(futures_data.get("polygon_url", settings.futures_provider.polygon_url)),
            ib_host=str(futures_data.get("ib_host", settings.futures_provider.ib_host)),
            ib_port=int(futures_data.get("ib_port", settings.futures_provider.ib_port)),
            ib_client_id=int(futures_data.get("ib_client_id", settings.futures_provider.ib_client_id)),
        ),
        pairs=pairs,
    )


def _pair_from_dict(data: dict) -> PairSettings:
    return PairSettings(
        name=str(data["name"]),
        spot_symbol=str(data["spot_symbol"]),
        futures_symbol=str(data["futures_symbol"]),
        pip_size=float(data["pip_size"]),
        threshold_pips=float(data["threshold_pips"]),
        normalization=str(data.get("normalization", "identity")),
        lot=float(data.get("lot", 0.01)),
        cooldown_seconds=float(data.get("cooldown_seconds", 15.0)),
        close_tolerance_pips=float(data.get("close_tolerance_pips", 0.2)),
        enabled=bool(data.get("enabled", True)),
        ib_contract=dict(data.get("ib_contract", {})),
    )
