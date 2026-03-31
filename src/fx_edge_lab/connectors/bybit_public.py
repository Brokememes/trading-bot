from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import websockets

from ..crypto_models import MarketTrade, OrderBookSnapshot


class BybitLinearFeed:
    def __init__(self, stream_url: str, symbols: list[str], orderbook_callback, trade_callback) -> None:
        self._stream_url = stream_url
        self._symbols = symbols
        self._orderbook_callback = orderbook_callback
        self._trade_callback = trade_callback
        self._books = {symbol: _BybitBookState(symbol) for symbol in symbols}

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self._symbols:
            return

        subscribe_args = []
        for symbol in self._symbols:
            subscribe_args.append(f"orderbook.50.{symbol}")
            subscribe_args.append(f"publicTrade.{symbol}")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._stream_url, ping_interval=None, ping_timeout=None) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": subscribe_args}))
                    heartbeat = asyncio.create_task(_heartbeat_loop(ws, stop_event))
                    try:
                        while not stop_event.is_set():
                            raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            payload = json.loads(raw_message)
                            topic = payload.get("topic", "")
                            if topic.startswith("orderbook."):
                                book = self._books[topic.split(".")[-1]]
                                snapshot = book.apply(payload)
                                if snapshot is not None:
                                    self._orderbook_callback(snapshot)
                            elif topic.startswith("publicTrade."):
                                for trade in _parse_trade_payload(payload):
                                    self._trade_callback(trade)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:  # pragma: no cover - live reconnect path
                print(f"[BYBIT] reconnecting after error: {exc}")
                await asyncio.sleep(1.0)


class _BybitBookState:
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}

    def apply(self, payload: dict) -> OrderBookSnapshot | None:
        message_type = payload.get("type")
        data = payload["data"]
        if message_type == "snapshot" or int(data.get("u", 0)) == 1:
            self._bids = {float(price): float(size) for price, size in data["b"]}
            self._asks = {float(price): float(size) for price, size in data["a"]}
        else:
            _apply_side_delta(self._bids, data["b"])
            _apply_side_delta(self._asks, data["a"])

        bids = tuple(sorted(self._bids.items(), key=lambda item: item[0], reverse=True)[:20])
        asks = tuple(sorted(self._asks.items(), key=lambda item: item[0])[:20])
        if not bids or not asks:
            return None

        timestamp = datetime.fromtimestamp(payload["ts"] / 1000.0, tz=timezone.utc)
        return OrderBookSnapshot(
            pair=self._symbol,
            venue="bybit",
            market_type="linear",
            symbol=self._symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )


def _apply_side_delta(target: dict[float, float], updates: list[list[str]]) -> None:
    for price_text, size_text in updates:
        price = float(price_text)
        size = float(size_text)
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size


def _parse_trade_payload(payload: dict) -> list[MarketTrade]:
    trades: list[MarketTrade] = []
    for item in payload["data"]:
        trades.append(
            MarketTrade(
                pair=item["s"],
                venue="bybit",
                market_type="linear",
                symbol=item["s"],
                timestamp=datetime.fromtimestamp(item["T"] / 1000.0, tz=timezone.utc),
                price=float(item["p"]),
                size=float(item["v"]),
                taker_side=str(item["S"]),
                trade_id=str(item["i"]),
            )
        )
    return trades


async def _heartbeat_loop(ws, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(20)
        await ws.send(json.dumps({"op": "ping"}))
