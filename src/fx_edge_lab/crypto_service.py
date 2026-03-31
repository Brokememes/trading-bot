from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from dataclasses import asdict
from datetime import datetime, timezone

from .connectors.binance_public import BinanceSpotFeed
from .connectors.bybit_public import BybitLinearFeed
from .crypto_engine import CryptoResearchEngine
from .crypto_models import FundingSnapshot
from .crypto_settings import load_crypto_settings
from .crypto_storage import CryptoSQLiteStorage


def capture_crypto_research(config_path: str | None, run_seconds: float | None = None) -> dict[str, int]:
    settings = load_crypto_settings(config_path)
    return asyncio.run(_capture(settings, run_seconds))


def dump_crypto_config(config_path: str | None) -> str:
    return json.dumps(asdict(load_crypto_settings(config_path)), indent=2)


async def _capture(settings, run_seconds: float | None) -> dict[str, int]:
    storage = CryptoSQLiteStorage(settings.database_path)
    engine = CryptoResearchEngine(settings, storage)
    stop_event = asyncio.Event()

    binance_symbols = sorted({pair.binance_spot_symbol for pair in settings.pairs})
    bybit_symbols = sorted({pair.bybit_linear_symbol for pair in settings.pairs})
    binance_feed = BinanceSpotFeed(
        stream_url=settings.binance_stream_url,
        symbols=binance_symbols,
        orderbook_callback=engine.on_orderbook,
        trade_callback=engine.on_trade,
    )
    bybit_feed = BybitLinearFeed(
        stream_url=settings.bybit_linear_url,
        symbols=bybit_symbols,
        orderbook_callback=engine.on_orderbook,
        trade_callback=engine.on_trade,
    )

    tasks = [
        asyncio.create_task(binance_feed.run(stop_event)),
        asyncio.create_task(bybit_feed.run(stop_event)),
        asyncio.create_task(_funding_poll_loop(settings, engine, stop_event)),
    ]
    timer_task = None
    if run_seconds is not None:
        timer_task = asyncio.create_task(_sleep_then_stop(run_seconds, stop_event))

    try:
        if timer_task is not None:
            await timer_task
        else:
            await stop_event.wait()
        await asyncio.sleep(0.2)
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        if timer_task is not None:
            await asyncio.gather(timer_task, return_exceptions=True)
        summary = engine.summary()
        storage.close()
    return summary


async def _sleep_then_stop(run_seconds: float, stop_event: asyncio.Event) -> None:
    await asyncio.sleep(run_seconds)
    stop_event.set()


async def _funding_poll_loop(settings, engine: CryptoResearchEngine, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        for pair in settings.pairs:
            try:
                snapshot = await asyncio.to_thread(
                    _fetch_bybit_funding_snapshot,
                    pair.name,
                    pair.bybit_linear_symbol,
                    settings.funding_history_limit,
                )
                engine.on_funding(snapshot)
            except Exception as exc:  # pragma: no cover - live path
                print(f"[FUNDING] {pair.name} fetch failed: {exc}")
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=settings.funding_poll_interval_ms / 1000.0,
            )
        except asyncio.TimeoutError:
            continue


def _fetch_bybit_funding_snapshot(pair_name: str, symbol: str, history_limit: int) -> FundingSnapshot:
    ticker_payload = _fetch_json(
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    ticker = ticker_payload["result"]["list"][0]
    history_payload = _fetch_json(
        "https://api.bybit.com/v5/market/funding/history",
        {"category": "linear", "symbol": symbol, "limit": history_limit},
    )
    history = history_payload["result"]["list"]
    funding_values = [float(item["fundingRate"]) for item in history if item.get("fundingRate") is not None]
    average_funding = (sum(funding_values) / len(funding_values)) if funding_values else None

    next_funding_time = None
    next_funding_raw = ticker.get("nextFundingTime")
    if next_funding_raw not in {None, "", "0"}:
        next_funding_time = datetime.fromtimestamp(int(next_funding_raw) / 1000.0, tz=timezone.utc)

    return FundingSnapshot(
        pair=pair_name,
        venue="bybit",
        symbol=symbol,
        timestamp=datetime.now(tz=timezone.utc),
        current_funding_rate=_float_or_none(ticker.get("fundingRate")),
        average_funding_rate=average_funding,
        next_funding_time=next_funding_time,
        basis_rate=_float_or_none(ticker.get("basisRate")),
        basis_value=_float_or_none(ticker.get("basis")),
    )


def _fetch_json(base_url: str, params: dict[str, str | int]) -> dict:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{base_url}?{query}",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def _float_or_none(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)
