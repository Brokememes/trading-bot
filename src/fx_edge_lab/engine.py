from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from .models import AlertEvent, AppSettings, BasisSnapshot, MarketQuote, PairSettings, TrackedAlert
from .notifiers import Notifier
from .pairs import build_snapshot
from .storage import SQLiteStorage


class BasisArbitrageEngine:
    def __init__(
        self,
        settings: AppSettings,
        storage: SQLiteStorage,
        notifier: Notifier,
        trade_executor=None,
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._notifier = notifier
        self._trade_executor = trade_executor
        self._lock = threading.Lock()
        self._pairs = {pair.name: pair for pair in settings.pairs if pair.enabled}
        self._spot_quotes: dict[str, MarketQuote] = {}
        self._futures_quotes: dict[str, MarketQuote] = {}
        self._last_alert_at: dict[str, datetime] = {}
        self._last_alert_direction: dict[str, str] = {}
        self._active_alerts: dict[str, list[TrackedAlert]] = {pair.name: [] for pair in settings.pairs}

    def on_spot_quote(self, quote: MarketQuote) -> None:
        with self._lock:
            self._spot_quotes[quote.pair] = quote
            self._evaluate_locked(quote.pair)

    def on_futures_quote(self, quote: MarketQuote) -> None:
        with self._lock:
            self._futures_quotes[quote.pair] = quote
            self._evaluate_locked(quote.pair)

    def _evaluate_locked(self, pair_name: str) -> None:
        pair = self._pairs.get(pair_name)
        if pair is None:
            return

        futures_quote = self._futures_quotes.get(pair_name)
        spot_quote = self._spot_quotes.get(pair_name)
        if futures_quote is None or spot_quote is None:
            return
        if self._is_stale(futures_quote) or self._is_stale(spot_quote):
            return

        snapshot = build_snapshot(pair, futures_quote, spot_quote)
        self._storage.log_gap(snapshot)
        self._update_outcomes(pair, snapshot)

        if snapshot.direction == "NONE":
            return
        if not self._can_emit_alert(pair, snapshot):
            return

        alert = self._build_alert(pair, snapshot)
        self._notifier.send(alert)
        alert_id = self._storage.insert_alert(alert)
        execution_status = "NOT_SENT"
        execution_order_id = None

        if self._settings.execute_trades and not self._settings.dry_run and self._trade_executor is not None:
            execution_status, execution_order_id = self._trade_executor(pair, alert)

        self._storage.update_execution(alert_id, execution_status, execution_order_id)
        self._last_alert_at[pair.name] = snapshot.timestamp
        self._last_alert_direction[pair.name] = snapshot.direction
        self._active_alerts[pair.name].append(
            TrackedAlert(
                alert_id=alert_id,
                pair=pair.name,
                direction=snapshot.direction,
                opened_at=snapshot.timestamp,
                entry_gap_pips=snapshot.gap_pips,
                threshold_pips=pair.threshold_pips,
                close_tolerance_pips=pair.close_tolerance_pips,
                max_gap_abs_pips=abs(snapshot.gap_pips),
            )
        )

    def _is_stale(self, quote: MarketQuote) -> bool:
        if quote.source.startswith("replay"):
            return False
        age = datetime.now(tz=timezone.utc) - quote.timestamp
        return age > timedelta(milliseconds=self._settings.stale_after_ms)

    def _can_emit_alert(self, pair: PairSettings, snapshot: BasisSnapshot) -> bool:
        last_at = self._last_alert_at.get(pair.name)
        last_direction = self._last_alert_direction.get(pair.name)
        if last_at is None:
            return True

        if (snapshot.timestamp - last_at).total_seconds() >= pair.cooldown_seconds:
            return True
        return last_direction != snapshot.direction

    def _build_alert(self, pair: PairSettings, snapshot: BasisSnapshot) -> AlertEvent:
        gap_abs_price = abs(snapshot.gap_pips) * pair.pip_size
        if snapshot.direction == "BUY":
            entry_price = snapshot.spot_ask
            sl_price = entry_price - (gap_abs_price * 1.5)
            tp_price = entry_price + gap_abs_price
        else:
            entry_price = snapshot.spot_bid
            sl_price = entry_price + (gap_abs_price * 1.5)
            tp_price = entry_price - gap_abs_price

        return AlertEvent(
            pair=pair.name,
            direction=snapshot.direction,
            timestamp=snapshot.timestamp,
            raw_futures_price=snapshot.futures_mid,
            normalized_futures_price=snapshot.normalized_futures_mid,
            spot_price=snapshot.spot_mid,
            gap_pips=snapshot.gap_pips,
            lot=pair.lot,
            sl_price=sl_price,
            tp_price=tp_price,
        )

    def _update_outcomes(self, pair: PairSettings, snapshot: BasisSnapshot) -> None:
        still_open: list[TrackedAlert] = []
        for tracked in self._active_alerts[pair.name]:
            tracked.max_gap_abs_pips = max(tracked.max_gap_abs_pips, abs(snapshot.gap_pips))
            self._storage.update_alert_max_gap(tracked.alert_id, tracked.max_gap_abs_pips)
            if _gap_closed(tracked.direction, snapshot.gap_pips, tracked.close_tolerance_pips):
                self._storage.close_alert(
                    tracked.alert_id,
                    snapshot.timestamp.isoformat(),
                    snapshot.gap_pips,
                )
                tracked.closed = True
            else:
                still_open.append(tracked)
        self._active_alerts[pair.name] = still_open


def _gap_closed(direction: str, current_gap_pips: float, tolerance_pips: float) -> bool:
    if abs(current_gap_pips) <= tolerance_pips:
        return True
    if direction == "BUY" and current_gap_pips < 0:
        return True
    if direction == "SELL" and current_gap_pips > 0:
        return True
    return False
