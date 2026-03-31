from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Callable

from ..models import AlertEvent, MarketQuote, Mt5Settings, PairSettings

try:
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    mt5 = None


class Mt5UnavailableError(RuntimeError):
    pass


class Mt5Bridge:
    def __init__(self, settings: Mt5Settings) -> None:
        if mt5 is None:
            raise Mt5UnavailableError("MetaTrader5 package is not installed")
        self._settings = settings
        self._initialized = False

    def initialize(self) -> None:
        kwargs = {}
        if self._settings.path:
            kwargs["path"] = self._settings.path
        if self._settings.login is not None:
            kwargs["login"] = self._settings.login
        if self._settings.password is not None:
            kwargs["password"] = self._settings.password
        if self._settings.server is not None:
            kwargs["server"] = self._settings.server
        kwargs["timeout"] = self._settings.initialize_timeout_ms

        if not mt5.initialize(**kwargs):
            raise Mt5UnavailableError(f"mt5.initialize failed: {mt5.last_error()}")
        self._initialized = True

    def shutdown(self) -> None:
        if self._initialized:
            mt5.shutdown()
            self._initialized = False

    def read_quote(self, pair: PairSettings) -> MarketQuote:
        if not mt5.symbol_select(pair.spot_symbol, True):
            raise Mt5UnavailableError(f"mt5.symbol_select failed for {pair.spot_symbol}")
        tick = mt5.symbol_info_tick(pair.spot_symbol)
        if tick is None:
            raise Mt5UnavailableError(f"mt5.symbol_info_tick returned None for {pair.spot_symbol}")
        return MarketQuote(
            pair=pair.name,
            symbol=pair.spot_symbol,
            bid=float(tick.bid),
            ask=float(tick.ask),
            timestamp=_mt5_tick_time(tick),
            source="mt5",
        )

    def place_spot_trade(self, pair: PairSettings, alert: AlertEvent) -> tuple[str, str | None]:
        symbol_info = mt5.symbol_info(pair.spot_symbol)
        tick = mt5.symbol_info_tick(pair.spot_symbol)
        if symbol_info is None or tick is None:
            return "QUOTE_UNAVAILABLE", None

        digits = int(getattr(symbol_info, "digits", 5))
        volume = max(float(getattr(symbol_info, "volume_min", pair.lot)), pair.lot)
        point = float(getattr(symbol_info, "point", pair.pip_size))
        stops_level = float(getattr(symbol_info, "trade_stops_level", 0.0)) * point
        price = float(tick.ask if alert.direction == "BUY" else tick.bid)
        sl, tp = _sanitize_stops(alert.direction, price, alert.sl_price, alert.tp_price, stops_level)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": pair.spot_symbol,
            "volume": volume,
            "type": mt5.ORDER_TYPE_BUY if alert.direction == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": round(price, digits),
            "sl": round(sl, digits),
            "tp": round(tp, digits),
            "deviation": self._settings.deviation_points,
            "magic": self._settings.magic,
            "comment": f"basis-arb-{pair.name}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": getattr(symbol_info, "filling_mode", mt5.ORDER_FILLING_RETURN),
        }
        result = mt5.order_send(request)
        if result is None:
            return "ORDER_SEND_NONE", None
        if getattr(result, "retcode", None) != mt5.TRADE_RETCODE_DONE:
            return f"ORDER_REJECTED:{getattr(result, 'retcode', 'UNKNOWN')}", None
        return "SENT", str(getattr(result, "order", ""))


class Mt5SpotPoller:
    def __init__(
        self,
        bridge: Mt5Bridge,
        pairs: list[PairSettings],
        callback: Callable[[MarketQuote], None],
        poll_interval_ms: int,
    ) -> None:
        self._bridge = bridge
        self._pairs = pairs
        self._callback = callback
        self._poll_interval = poll_interval_ms / 1000.0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_seen: dict[str, tuple[float, float, datetime]] = {}

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="mt5-spot-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            for pair in self._pairs:
                try:
                    quote = self._bridge.read_quote(pair)
                except Exception as exc:  # pragma: no cover - live path
                    print(f"[MT5] quote error for {pair.name}: {exc}")
                    continue

                signature = (quote.bid, quote.ask, quote.timestamp)
                if self._last_seen.get(pair.name) == signature:
                    continue
                self._last_seen[pair.name] = signature
                self._callback(quote)

            time.sleep(self._poll_interval)


def _mt5_tick_time(tick: object) -> datetime:
    time_msc = getattr(tick, "time_msc", None)
    if time_msc:
        return datetime.fromtimestamp(time_msc / 1000.0, tz=timezone.utc)
    tick_time = getattr(tick, "time", None)
    if tick_time:
        return datetime.fromtimestamp(tick_time, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _sanitize_stops(
    direction: str,
    entry_price: float,
    requested_sl: float,
    requested_tp: float,
    min_distance: float,
) -> tuple[float, float]:
    if direction == "BUY":
        sl = min(requested_sl, entry_price - min_distance) if min_distance > 0 else requested_sl
        tp = max(requested_tp, entry_price + min_distance) if min_distance > 0 else requested_tp
        return sl, tp

    sl = max(requested_sl, entry_price + min_distance) if min_distance > 0 else requested_sl
    tp = min(requested_tp, entry_price - min_distance) if min_distance > 0 else requested_tp
    return sl, tp
