from __future__ import annotations

import json
import time
from dataclasses import asdict

from .connectors.ib import IbFuturesFeed
from .connectors.mt5 import Mt5Bridge, Mt5SpotPoller
from .connectors.polygon import PolygonFuturesFeed
from .engine import BasisArbitrageEngine
from .notifiers import CompositeNotifier, ConsoleNotifier, TelegramNotifier
from .settings import load_settings
from .storage import SQLiteStorage


def monitor_live(config_path: str | None, run_seconds: float | None = None) -> dict[str, int]:
    settings = load_settings(config_path)
    _validate_live_settings(settings)
    storage = SQLiteStorage(settings.database_path)
    bridge = Mt5Bridge(settings.mt5)
    bridge.initialize()

    try:
        notifier = CompositeNotifier(ConsoleNotifier(), TelegramNotifier(settings.telegram))
        engine = BasisArbitrageEngine(
            settings=settings,
            storage=storage,
            notifier=notifier,
            trade_executor=bridge.place_spot_trade,
        )
        live_pairs = [pair for pair in settings.pairs if pair.enabled]
        spot_poller = Mt5SpotPoller(
            bridge=bridge,
            pairs=live_pairs,
            callback=engine.on_spot_quote,
            poll_interval_ms=settings.mt5.poll_interval_ms,
        )

        if settings.futures_provider.kind.lower() == "polygon":
            futures_feed = PolygonFuturesFeed(settings.futures_provider, live_pairs, engine.on_futures_quote)
        elif settings.futures_provider.kind.lower() == "ib":
            futures_feed = IbFuturesFeed(settings.futures_provider, live_pairs, engine.on_futures_quote)
        else:
            raise ValueError(f"unsupported futures provider {settings.futures_provider.kind}")

        futures_feed.start()
        spot_poller.start()

        started = time.time()
        while True:
            if run_seconds is not None and (time.time() - started) >= run_seconds:
                break
            time.sleep(0.5)

        futures_feed.stop()
        spot_poller.stop()
        return storage.summary()
    finally:
        bridge.shutdown()
        storage.close()


def dump_config(config_path: str | None) -> str:
    return json.dumps(asdict(load_settings(config_path)), indent=2)


def _validate_live_settings(settings) -> None:
    provider_kind = settings.futures_provider.kind.lower()
    if provider_kind == "polygon":
        api_key = settings.futures_provider.api_key
        if not api_key or api_key == "REPLACE_ME":
            raise ValueError("Polygon API key is missing in futures_provider.api_key")
