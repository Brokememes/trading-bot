from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import quote

import websockets

from ..crypto_models import MarketTrade, OrderBookSnapshot


class BinanceSpotFeed:
    def __init__(self, stream_url: str, symbols: list[str], orderbook_callback, trade_callback) -> None:
        self._stream_url = stream_url.rstrip("/")
        self._symbols = symbols
        self._orderbook_callback = orderbook_callback
        self._trade_callback = trade_callback

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self._symbols:
            return

        stream_names = []
        for symbol in self._symbols:
            stream_names.append(f"{symbol}@depth20@100ms")
            stream_names.append(f"{symbol}@trade")
        combined = "/".join(stream_names)
        url = f"{self._stream_url}?streams={quote(combined, safe='/@')}"

        while not stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    while not stop_event.is_set():
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        payload = json.loads(raw_message)
                        stream = payload["stream"]
                        data = payload["data"]
                        if "@depth" in stream:
                            self._orderbook_callback(_parse_depth_message(stream, data))
                        elif "@trade" in stream:
                            self._trade_callback(_parse_trade_message(stream, data))
            except asyncio.TimeoutError:
                continue
            except Exception as exc:  # pragma: no cover - live reconnect path
                print(f"[BINANCE] reconnecting after error: {exc}")
                await asyncio.sleep(1.0)


def _parse_depth_message(stream: str, data: dict) -> OrderBookSnapshot:
    symbol = stream.split("@", 1)[0]
    event_time = data.get("E")
    if event_time is None:
        timestamp = datetime.now(tz=timezone.utc)
    else:
        timestamp = datetime.fromtimestamp(event_time / 1000.0, tz=timezone.utc)
    bids = tuple((float(price), float(size)) for price, size in data["bids"])
    asks = tuple((float(price), float(size)) for price, size in data["asks"])
    return OrderBookSnapshot(
        pair=symbol.upper(),
        venue="binance",
        market_type="spot",
        symbol=symbol,
        timestamp=timestamp,
        bids=bids,
        asks=asks,
    )


def _parse_trade_message(stream: str, data: dict) -> MarketTrade:
    symbol = stream.split("@", 1)[0]
    timestamp = datetime.fromtimestamp(data["T"] / 1000.0, tz=timezone.utc)
    return MarketTrade(
        pair=symbol.upper(),
        venue="binance",
        market_type="spot",
        symbol=symbol,
        timestamp=timestamp,
        price=float(data["p"]),
        size=float(data["q"]),
        taker_side="Sell" if bool(data["m"]) else "Buy",
        trade_id=str(data["t"]),
    )
