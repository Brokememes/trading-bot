from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timezone
from typing import Callable

from ..models import FuturesProviderSettings, MarketQuote, PairSettings

try:
    import websockets
except ImportError:  # pragma: no cover - optional dependency
    websockets = None


class PolygonFuturesFeed:
    def __init__(
        self,
        settings: FuturesProviderSettings,
        pairs: list[PairSettings],
        callback: Callable[[MarketQuote], None],
    ) -> None:
        if websockets is None:
            raise RuntimeError("websockets package is not installed")
        self._settings = settings
        self._pairs = pairs
        self._callback = callback
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run_thread, name="polygon-futures-feed", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run_thread(self) -> None:
        asyncio.run(self._run())

    async def _run(self) -> None:
        if not self._settings.api_key:
            raise RuntimeError("Polygon API key is missing")

        async with websockets.connect(self._settings.polygon_url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({"action": "auth", "params": self._settings.api_key}))
            await ws.send(
                json.dumps(
                    {"action": "subscribe", "params": ",".join(f"Q.{pair.futures_symbol}" for pair in self._pairs)}
                )
            )
            symbol_map = {pair.futures_symbol: pair for pair in self._pairs}

            while not self._stop_event.is_set():
                raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                payload = json.loads(raw_message)
                events = payload if isinstance(payload, list) else [payload]
                for event in events:
                    if event.get("ev") != "Q":
                        continue
                    pair = symbol_map.get(str(event["sym"]))
                    if pair is None:
                        continue
                    timestamp_ms = int(event.get("t", 0))
                    timestamp = (
                        datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                        if timestamp_ms
                        else datetime.now(tz=timezone.utc)
                    )
                    self._callback(
                        MarketQuote(
                            pair=pair.name,
                            symbol=str(event["sym"]),
                            bid=float(event["bp"]),
                            ask=float(event["ap"]),
                            timestamp=timestamp,
                            source="polygon",
                        )
                    )
