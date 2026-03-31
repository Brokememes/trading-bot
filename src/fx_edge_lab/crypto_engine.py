from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone

from .crypto_models import (
    BasisObservation,
    CryptoPairSettings,
    CryptoResearchSettings,
    FundingSnapshot,
    MarketTrade,
    OrderBookSnapshot,
    PaperOrder,
    PendingMarkout,
    SpreadPosition,
    SignalEvent,
)
from .crypto_pnl import (
    LONG_SPOT_SHORT_PERP,
    SHORT_SPOT_LONG_PERP,
    signal_quality_band,
    signal_quality_score,
    spread_edge_pct,
    spread_gross_pnl_quote,
    spread_net_pnl_quote,
    spread_target_edge_pct,
    target_exit_basis_bps,
)
from .crypto_storage import CryptoSQLiteStorage

TIER1_SIGNAL_SOURCE = "basis_tier1_confirmed"
TIER2_SIGNAL_SOURCE = "basis_tier2_extreme"


class CryptoResearchEngine:
    def __init__(self, settings: CryptoResearchSettings, storage: CryptoSQLiteStorage) -> None:
        self._settings = settings
        self._storage = storage
        self._pairs_by_name = {pair.name: pair for pair in settings.pairs}
        self._pair_by_binance_symbol = {pair.binance_spot_symbol: pair for pair in settings.pairs}
        self._pair_by_bybit_symbol = {pair.bybit_linear_symbol: pair for pair in settings.pairs}
        self._latest_books: dict[tuple[str, str, str], OrderBookSnapshot] = {}
        self._latest_funding: dict[str, FundingSnapshot] = {}
        self._last_quote_write: dict[tuple[str, str, str], datetime] = {}
        self._last_basis_write: dict[str, datetime] = {}
        self._last_signal_at: dict[str, datetime] = {}
        self._open_orders: list[PaperOrder] = []
        self._pending_markouts: list[PendingMarkout] = []
        self._recent_basis: dict[str, deque[BasisObservation]] = {pair.name: deque() for pair in settings.pairs}
        self._open_spread_positions: list[SpreadPosition] = []

    def on_orderbook(self, snapshot: OrderBookSnapshot) -> None:
        pair = self._resolve_pair(snapshot.venue, snapshot.market_type, snapshot.symbol)
        if pair is None:
            return

        snapshot = OrderBookSnapshot(
            pair=pair.name,
            venue=snapshot.venue,
            market_type=snapshot.market_type,
            symbol=snapshot.symbol,
            timestamp=snapshot.timestamp,
            bids=snapshot.bids,
            asks=snapshot.asks,
        )
        self._latest_books[(snapshot.venue, snapshot.market_type, snapshot.symbol)] = snapshot
        imbalance = snapshot.imbalance(self._settings.imbalance_levels)

        if self._should_write_quote(snapshot):
            self._storage.log_quote(snapshot, imbalance, self._settings.storage_depth_levels)
            self._last_quote_write[(snapshot.venue, snapshot.market_type, snapshot.symbol)] = snapshot.timestamp

        self._expire_orders(snapshot.timestamp)
        self._settle_markouts(snapshot)
        observation = self._write_basis_if_due(pair.name, snapshot.timestamp)
        if observation is not None:
            self._on_basis_observation(pair, observation)

    def on_trade(self, trade: MarketTrade) -> None:
        pair = self._resolve_pair(trade.venue, trade.market_type, trade.symbol)
        if pair is None:
            return

        trade = MarketTrade(
            pair=pair.name,
            venue=trade.venue,
            market_type=trade.market_type,
            symbol=trade.symbol,
            timestamp=trade.timestamp,
            price=trade.price,
            size=trade.size,
            taker_side=trade.taker_side,
            trade_id=trade.trade_id,
        )
        self._storage.log_trade(trade)
        self._maybe_fill_orders(trade)

    def on_funding(self, snapshot: FundingSnapshot) -> None:
        self._latest_funding[snapshot.pair] = snapshot
        self._storage.log_funding(snapshot)
        observation = self._write_basis_if_due(snapshot.pair, snapshot.timestamp, force=True)
        if observation is not None:
            self._on_basis_observation(self._pairs_by_name[snapshot.pair], observation)

    def summary(self) -> dict[str, int]:
        tables = {
            "quotes": "crypto_quotes",
            "trades": "crypto_trades",
            "funding": "crypto_funding",
            "basis": "crypto_basis",
            "signals": "crypto_signals",
            "spreads": "crypto_spread_positions",
        }
        summary: dict[str, int] = {}
        for key, table in tables.items():
            rows = self._storage.fetch_all(f"SELECT COUNT(*) AS n FROM {table}")
            summary[key] = int(rows[0]["n"])
        status_rows = self._storage.fetch_all(
            """
            SELECT status, COUNT(*) AS n
            FROM crypto_spread_positions
            GROUP BY status
            """
        )
        summary["open_spreads"] = 0
        summary["closed_spreads"] = 0
        for row in status_rows:
            status = str(row["status"]).upper()
            if status == "OPEN":
                summary["open_spreads"] = int(row["n"])
            elif status == "CLOSED":
                summary["closed_spreads"] = int(row["n"])
        return summary

    def _resolve_pair(self, venue: str, market_type: str, symbol: str) -> CryptoPairSettings | None:
        if venue == "binance" and market_type == "spot":
            return self._pair_by_binance_symbol.get(symbol.lower())
        if venue == "bybit" and market_type == "linear":
            return self._pair_by_bybit_symbol.get(symbol.upper())
        return None

    def _should_write_quote(self, snapshot: OrderBookSnapshot) -> bool:
        key = (snapshot.venue, snapshot.market_type, snapshot.symbol)
        previous = self._last_quote_write.get(key)
        if previous is None:
            return True
        elapsed_ms = (snapshot.timestamp - previous).total_seconds() * 1000.0
        return elapsed_ms >= self._settings.quote_throttle_ms

    def _write_basis_if_due(self, pair_name: str, timestamp: datetime, force: bool = False) -> BasisObservation | None:
        pair = self._pairs_by_name[pair_name]
        spot_book = self._latest_books.get(("binance", "spot", pair.binance_spot_symbol))
        perp_book = self._latest_books.get(("bybit", "linear", pair.bybit_linear_symbol))
        if spot_book is None or perp_book is None:
            return None

        previous = self._last_basis_write.get(pair_name)
        if not force and previous is not None:
            elapsed_ms = (timestamp - previous).total_seconds() * 1000.0
            if elapsed_ms < self._settings.basis_sample_interval_ms:
                return None

        funding = self._latest_funding.get(pair_name)
        observation = BasisObservation(
            pair=pair_name,
            timestamp=max(spot_book.timestamp, perp_book.timestamp, timestamp),
            spot_venue="binance",
            spot_symbol=pair.binance_spot_symbol,
            perp_venue="bybit",
            perp_symbol=pair.bybit_linear_symbol,
            spot_mid=spot_book.mid_price,
            perp_mid=perp_book.mid_price,
            premium_bps=((perp_book.mid_price / spot_book.mid_price) - 1.0) * 10_000.0,
            spot_imbalance=spot_book.imbalance(self._settings.imbalance_levels),
            perp_imbalance=perp_book.imbalance(self._settings.imbalance_levels),
            current_funding_rate=None if funding is None else funding.current_funding_rate,
            average_funding_rate=None if funding is None else funding.average_funding_rate,
            next_funding_time=None if funding is None else funding.next_funding_time,
            basis_rate=None if funding is None else funding.basis_rate,
            basis_value=None if funding is None else funding.basis_value,
        )
        self._storage.log_basis(observation)
        self._last_basis_write[pair_name] = observation.timestamp
        return observation

    def _on_basis_observation(self, pair: CryptoPairSettings, observation: BasisObservation) -> None:
        history = self._recent_basis[pair.name]
        history.append(observation)
        keep_window_ms = max(
            pair.basis_momentum_window_ms,
            self._settings.regime_window_ms,
            self._settings.pre_funding_trend_window_ms,
            self._settings.basis_sample_interval_ms * max(self._settings.basis_consecutive_samples_required + 2, 5),
        )
        cutoff = observation.timestamp - timedelta(milliseconds=keep_window_ms)
        while len(history) > 1 and history[1].timestamp <= cutoff:
            history.popleft()
        self._update_spread_positions(pair, observation)
        self._maybe_emit_basis_signal(pair, observation)

    def _maybe_emit_basis_signal(self, pair: CryptoPairSettings, observation: BasisObservation) -> None:
        if any(position.pair == pair.name for position in self._open_spread_positions):
            return
        strong_threshold_bps = self._adaptive_basis_threshold_bps(pair, observation.timestamp)
        basis_only_threshold_bps = self._basis_only_threshold_bps(pair, observation.timestamp)
        funding_rate = observation.current_funding_rate
        regime, _ = self._current_regime(pair, observation.timestamp)

        momentum_bps = self._basis_momentum_bps(pair, observation.timestamp)
        side = None
        signal_source = None
        threshold_bps = None
        quantity = pair.order_size
        if (
            observation.premium_bps >= strong_threshold_bps
            and funding_rate is not None
            and funding_rate > pair.funding_entry_min_rate
            and momentum_bps is not None
            and momentum_bps > 0
        ):
            side = LONG_SPOT_SHORT_PERP
            signal_source = TIER1_SIGNAL_SOURCE
            threshold_bps = strong_threshold_bps
        elif (
            observation.premium_bps <= -strong_threshold_bps
            and funding_rate is not None
            and funding_rate < -pair.funding_entry_min_rate
            and momentum_bps is not None
            and momentum_bps < 0
        ):
            side = SHORT_SPOT_LONG_PERP
            signal_source = TIER1_SIGNAL_SOURCE
            threshold_bps = strong_threshold_bps
        elif observation.premium_bps >= basis_only_threshold_bps:
            side = LONG_SPOT_SHORT_PERP
            signal_source = TIER2_SIGNAL_SOURCE
            threshold_bps = basis_only_threshold_bps
            quantity = pair.order_size * pair.extreme_basis_size_fraction
        elif observation.premium_bps <= -basis_only_threshold_bps:
            side = SHORT_SPOT_LONG_PERP
            signal_source = TIER2_SIGNAL_SOURCE
            threshold_bps = basis_only_threshold_bps
            quantity = pair.order_size * pair.extreme_basis_size_fraction
        if side is None or signal_source is None or threshold_bps is None or quantity <= 0:
            return
        if self._consecutive_basis_samples(pair, threshold_bps, side) < self._settings.basis_consecutive_samples_required:
            return

        previous = self._last_signal_at.get(pair.name)
        if previous is not None:
            elapsed_ms = (observation.timestamp - previous).total_seconds() * 1000.0
            if elapsed_ms < pair.signal_cooldown_ms:
                return

        gross_edge_pct = spread_target_edge_pct(side, observation.premium_bps, pair.basis_exit_threshold_bps)
        quality_score = signal_quality_score(gross_edge_pct, self._settings)
        quality_band = signal_quality_band(quality_score)
        if quality_score is not None and quality_score < 0:
            return

        spot_book = self._latest_books.get(("binance", "spot", pair.binance_spot_symbol))
        perp_book = self._latest_books.get(("bybit", "linear", pair.bybit_linear_symbol))
        if spot_book is None or perp_book is None:
            return

        signal = SignalEvent(
            pair=pair.name,
            venue="cross",
            market_type="spread",
            symbol=pair.name,
            timestamp=observation.timestamp,
            side=side,
            signal_source=signal_source,
            entry_bid=spot_book.bid_price,
            entry_ask=perp_book.ask_price,
            entry_mid=observation.premium_bps,
            spread_bps=observation.premium_bps,
            imbalance=observation.spot_imbalance,
            threshold=threshold_bps,
        )
        signal_id = self._storage.insert_signal(signal)
        position_id = self._storage.insert_spread_position(
            signal_id=signal_id,
            timestamp=observation.timestamp.isoformat(),
            pair=pair.name,
            quantity=quantity,
            entry_spot_price=spot_book.bid_price,
            entry_perp_price=perp_book.ask_price,
            entry_basis_bps=observation.premium_bps,
            basis_threshold_bps=threshold_bps,
            entry_funding_rate=funding_rate,
            signal_quality_score=quality_score,
            signal_quality_band=quality_band,
            spot_imbalance=observation.spot_imbalance,
            perp_imbalance=observation.perp_imbalance,
        )
        self._open_spread_positions.append(
            SpreadPosition(
                position_id=position_id,
                signal_id=signal_id,
                pair=pair.name,
                side=side,
                regime=regime,
                entry_time=observation.timestamp,
                quantity=quantity,
                entry_spot_price=spot_book.bid_price,
                entry_perp_price=perp_book.ask_price,
                entry_basis_bps=observation.premium_bps,
                basis_exit_threshold_bps=pair.basis_exit_threshold_bps,
                entry_funding_rate=funding_rate,
                signal_quality_score=quality_score,
                signal_quality_band=quality_band,
                spot_imbalance=observation.spot_imbalance,
                perp_imbalance=observation.perp_imbalance,
            )
        )
        self._last_signal_at[pair.name] = observation.timestamp

    def _update_spread_positions(self, pair: CryptoPairSettings, observation: BasisObservation) -> None:
        spot_book = self._latest_books.get(("binance", "spot", pair.binance_spot_symbol))
        perp_book = self._latest_books.get(("bybit", "linear", pair.bybit_linear_symbol))
        if spot_book is None or perp_book is None:
            return

        still_open: list[SpreadPosition] = []
        for position in self._open_spread_positions:
            if position.pair != pair.name:
                still_open.append(position)
                continue

            exit_reason = None
            exit_basis_bps = target_exit_basis_bps(position.side, pair.basis_exit_threshold_bps)
            if position.side == LONG_SPOT_SHORT_PERP and observation.premium_bps <= exit_basis_bps:
                exit_reason = "BASIS_CONVERGED"
            elif position.side == SHORT_SPOT_LONG_PERP and observation.premium_bps >= exit_basis_bps:
                exit_reason = "BASIS_CONVERGED"
            elif (
                position.side == LONG_SPOT_SHORT_PERP
                and observation.current_funding_rate is not None
                and observation.current_funding_rate < pair.funding_exit_rate
            ):
                exit_reason = "FUNDING_FLIPPED"
            elif (
                position.side == SHORT_SPOT_LONG_PERP
                and observation.current_funding_rate is not None
                and observation.current_funding_rate > -pair.funding_exit_rate
            ):
                exit_reason = "FUNDING_FLIPPED"
            elif observation.timestamp >= position.entry_time + timedelta(milliseconds=pair.max_hold_ms):
                exit_reason = "TIME_STOP"

            if exit_reason is None:
                still_open.append(position)
                continue

            hold_ms = (observation.timestamp - position.entry_time).total_seconds() * 1000.0
            gross_edge_pct = spread_edge_pct(position.side, position.entry_basis_bps, observation.premium_bps)
            gross_pnl_quote = spread_gross_pnl_quote(
                position.side,
                position.quantity,
                position.entry_spot_price,
                position.entry_basis_bps,
                observation.premium_bps,
            )
            net_pnl_quote = spread_net_pnl_quote(
                position.side,
                position.quantity,
                position.entry_spot_price,
                spot_book.mid_price,
                position.entry_basis_bps,
                observation.premium_bps,
                hold_ms,
                self._settings,
            )
            self._storage.close_spread_position(
                position.position_id,
                exit_timestamp=observation.timestamp.isoformat(),
                exit_reason=exit_reason,
                exit_spot_price=spot_book.mid_price,
                exit_perp_price=perp_book.mid_price,
                exit_basis_bps=observation.premium_bps,
                exit_funding_rate=observation.current_funding_rate,
                gross_edge_pct=gross_edge_pct,
                gross_pnl_quote=gross_pnl_quote,
                net_pnl_quote=net_pnl_quote,
                hold_ms=hold_ms,
            )
        self._open_spread_positions = still_open

    def _adaptive_basis_threshold_bps(self, pair: CryptoPairSettings, timestamp: datetime) -> float:
        day_start = timestamp.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        row = self._storage.fetch_all(
            """
            SELECT COUNT(*) AS n
            FROM crypto_signals
            WHERE pair = ? AND signal_source LIKE 'basis_tier%' AND timestamp >= ? AND timestamp < ?
            """,
            (
                pair.name,
                day_start.isoformat(),
                day_end.isoformat(),
            ),
        )[0]
        count_today = int(row["n"])
        if count_today < 2:
            return pair.basis_entry_threshold_low_bps
        if count_today > 10:
            return pair.basis_entry_threshold_high_bps
        return pair.basis_entry_threshold_bps

    def _basis_only_threshold_bps(self, pair: CryptoPairSettings, timestamp: datetime) -> float:
        if self._is_pre_funding_window(timestamp):
            return min(pair.basis_entry_threshold_bps, self._settings.pre_funding_basis_threshold_bps)
        return pair.basis_entry_threshold_bps

    def _consecutive_basis_samples(self, pair: CryptoPairSettings, threshold_bps: float, side: str) -> int:
        history = self._recent_basis[pair.name]
        count = 0
        for item in reversed(history):
            if side == LONG_SPOT_SHORT_PERP and item.premium_bps >= threshold_bps:
                count += 1
                continue
            if side == SHORT_SPOT_LONG_PERP and item.premium_bps <= -threshold_bps:
                count += 1
                continue
            break
        return count

    def _is_pre_funding_window(self, timestamp: datetime) -> bool:
        next_funding = self._next_funding_window(timestamp)
        countdown_ms = (next_funding - timestamp.astimezone(timezone.utc)).total_seconds() * 1000.0
        return 0.0 <= countdown_ms <= self._settings.pre_funding_window_ms

    def _next_funding_window(self, timestamp: datetime) -> datetime:
        current = timestamp.astimezone(timezone.utc)
        base = current.replace(minute=0, second=0, microsecond=0)
        for hour in (0, 8, 16):
            candidate = base.replace(hour=hour)
            if candidate >= current:
                return candidate
        next_day = (base + timedelta(days=1)).replace(hour=0)
        return next_day

    def _basis_momentum_bps(self, pair: CryptoPairSettings, timestamp: datetime) -> float | None:
        history = self._recent_basis[pair.name]
        if len(history) < 2:
            return None
        oldest = history[0]
        latest = history[-1]
        elapsed_ms = (latest.timestamp - oldest.timestamp).total_seconds() * 1000.0
        if elapsed_ms + max(self._settings.basis_sample_interval_ms, 1000) < pair.basis_momentum_window_ms:
            return None
        return latest.premium_bps - oldest.premium_bps

    def _current_regime(self, pair: CryptoPairSettings, timestamp: datetime) -> tuple[str, float | None]:
        history = self._recent_basis[pair.name]
        if len(history) < 2:
            return "NEUTRAL", None
        first = history[0]
        last = history[-1]
        elapsed_ms = (last.timestamp - first.timestamp).total_seconds() * 1000.0
        if elapsed_ms + max(self._settings.basis_sample_interval_ms, 1000) < self._settings.regime_window_ms:
            return "NEUTRAL", None
        avg_basis_bps = sum(item.premium_bps for item in history) / len(history)
        if avg_basis_bps > self._settings.regime_contango_bps:
            return "CONTANGO", avg_basis_bps
        if avg_basis_bps < self._settings.regime_backwardation_bps:
            return "BACKWARDATION", avg_basis_bps
        return "NEUTRAL", avg_basis_bps

    def _maker_book(self, pair: CryptoPairSettings) -> OrderBookSnapshot | None:
        if pair.maker_venue == "binance" and pair.maker_market_type == "spot":
            return self._latest_books.get(("binance", "spot", pair.binance_spot_symbol))
        if pair.maker_venue == "bybit" and pair.maker_market_type == "linear":
            return self._latest_books.get(("bybit", "linear", pair.bybit_linear_symbol))
        return None

    def _maybe_fill_orders(self, trade: MarketTrade) -> None:
        still_open: list[PaperOrder] = []
        for order in self._open_orders:
            if order.status != "OPEN":
                continue
            if trade.timestamp >= order.expires_at:
                self._storage.mark_order_expired(order.order_id)
                order.status = "EXPIRED"
                continue
            if (order.venue, order.market_type, order.symbol) != (
                trade.venue,
                trade.market_type,
                trade.symbol,
            ):
                still_open.append(order)
                continue
            if not _trade_fills_order(order, trade):
                still_open.append(order)
                continue

            order.status = "FILLED"
            order.filled_at = trade.timestamp
            order.fill_price = order.limit_price
            order.fill_trade_id = trade.trade_id
            fill_latency_ms = (trade.timestamp - order.posted_at).total_seconds() * 1000.0
            self._storage.fill_order(
                order.order_id,
                filled_at=trade.timestamp.isoformat(),
                fill_price=order.limit_price,
                fill_trade_id=trade.trade_id,
                fill_latency_ms=fill_latency_ms,
            )
            self._schedule_markouts(
                target_kind="paper",
                target_id=order.order_id,
                venue=order.venue,
                market_type=order.market_type,
                symbol=order.symbol,
                side=order.side,
                reference_price=order.limit_price,
                start_time=trade.timestamp,
            )
        self._open_orders = still_open

    def _expire_orders(self, now: datetime) -> None:
        still_open: list[PaperOrder] = []
        for order in self._open_orders:
            if order.status == "OPEN" and now >= order.expires_at:
                self._storage.mark_order_expired(order.order_id)
                order.status = "EXPIRED"
                continue
            still_open.append(order)
        self._open_orders = still_open

    def _schedule_markouts(
        self,
        *,
        target_kind: str,
        target_id: int,
        venue: str,
        market_type: str,
        symbol: str,
        side: str,
        reference_price: float,
        start_time: datetime,
    ) -> None:
        for horizon_ms in self._settings.analysis_horizons_ms:
            self._pending_markouts.append(
                PendingMarkout(
                    target_kind=target_kind,
                    target_id=target_id,
                    venue=venue,
                    market_type=market_type,
                    symbol=symbol,
                    side=side,
                    reference_price=reference_price,
                    due_at=start_time + timedelta(milliseconds=horizon_ms),
                    horizon_ms=horizon_ms,
                )
            )

    def _settle_markouts(self, snapshot: OrderBookSnapshot) -> None:
        remaining: list[PendingMarkout] = []
        for pending in self._pending_markouts:
            if (pending.venue, pending.market_type, pending.symbol) != (
                snapshot.venue,
                snapshot.market_type,
                snapshot.symbol,
            ):
                remaining.append(pending)
                continue
            if snapshot.timestamp < pending.due_at:
                remaining.append(pending)
                continue

            markout_bps = _side_adjusted_markout(
                side=pending.side,
                reference_price=pending.reference_price,
                current_mid=snapshot.mid_price,
            )
            if pending.target_kind == "signal":
                self._storage.insert_signal_markout(
                    pending.target_id,
                    pending.horizon_ms,
                    snapshot.timestamp.isoformat(),
                    markout_bps,
                )
            else:
                self._storage.insert_paper_markout(
                    pending.target_id,
                    pending.horizon_ms,
                    snapshot.timestamp.isoformat(),
                    markout_bps,
                )
        self._pending_markouts = remaining


def _matches_signal_source(pair: CryptoPairSettings, snapshot: OrderBookSnapshot) -> bool:
    if pair.signal_source == "binance_spot":
        return (
            snapshot.venue == "binance"
            and snapshot.market_type == "spot"
            and snapshot.symbol == pair.binance_spot_symbol
        )
    if pair.signal_source == "bybit_linear":
        return (
            snapshot.venue == "bybit"
            and snapshot.market_type == "linear"
            and snapshot.symbol == pair.bybit_linear_symbol
        )
    return False


def _trade_fills_order(order: PaperOrder, trade: MarketTrade) -> bool:
    if order.side == "BUY":
        return trade.taker_side == "Sell" and trade.price <= order.limit_price
    return trade.taker_side == "Buy" and trade.price >= order.limit_price


def _side_adjusted_markout(side: str, reference_price: float, current_mid: float) -> float:
    if side == "BUY":
        return ((current_mid / reference_price) - 1.0) * 10_000.0
    return ((reference_price / current_mid) - 1.0) * 10_000.0
