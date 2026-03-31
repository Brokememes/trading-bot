from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class CryptoPairSettings:
    name: str
    binance_spot_symbol: str
    bybit_linear_symbol: str
    maker_venue: str = "binance"
    maker_market_type: str = "spot"
    signal_source: str = "binance_spot"
    imbalance_threshold: float = 0.35
    max_spread_bps: float = 3.0
    maker_order_ttl_ms: int = 2000
    signal_cooldown_ms: int = 1000
    order_size: float = 0.001
    extreme_basis_size_fraction: float = 0.5
    basis_entry_threshold_bps: float = 80.0
    basis_entry_threshold_low_bps: float = 60.0
    basis_entry_threshold_high_bps: float = 120.0
    basis_exit_threshold_bps: float = 30.0
    funding_entry_min_rate: float = 0.0005
    funding_exit_rate: float = 0.0
    basis_momentum_window_ms: int = 180000
    max_hold_ms: int = 28800000


@dataclass(frozen=True)
class CryptoResearchSettings:
    database_path: str = "data/crypto_microstructure.sqlite"
    binance_stream_url: str = "wss://stream.binance.com:9443/stream"
    bybit_linear_url: str = "wss://stream.bybit.com/v5/public/linear"
    quote_throttle_ms: int = 100
    imbalance_levels: int = 5
    storage_depth_levels: int = 5
    basis_sample_interval_ms: int = 1000
    funding_poll_interval_ms: int = 60000
    funding_history_limit: int = 9
    analysis_horizons_ms: tuple[int, ...] = (1000, 5000, 30000)
    fee_preset: str = "custom"
    exit_mode: str = "taker"
    maker_entry_fee_bps: float = 0.0
    exit_fee_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    equity_curve_horizon_ms: int = 0
    regime_window_ms: int = 3600000
    regime_contango_bps: float = 10.0
    regime_backwardation_bps: float = -10.0
    basis_consecutive_samples_required: int = 3
    pre_funding_window_ms: int = 3600000
    pre_funding_basis_threshold_bps: float = 50.0
    pre_funding_trend_window_ms: int = 600000
    reverse_spot_borrow_apy: float = 0.10
    include_funding_in_pnl: bool = False
    pairs: tuple[CryptoPairSettings, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class OrderBookSnapshot:
    pair: str
    venue: str
    market_type: str
    symbol: str
    timestamp: datetime
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    @property
    def bid_price(self) -> float:
        return self.bids[0][0]

    @property
    def bid_size(self) -> float:
        return self.bids[0][1]

    @property
    def ask_price(self) -> float:
        return self.asks[0][0]

    @property
    def ask_size(self) -> float:
        return self.asks[0][1]

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread_bps(self) -> float:
        return ((self.ask_price - self.bid_price) / self.mid_price) * 10_000.0

    def imbalance(self, levels: int) -> float:
        bid_notional = sum(price * size for price, size in self.bids[:levels])
        ask_notional = sum(price * size for price, size in self.asks[:levels])
        total = bid_notional + ask_notional
        if total == 0:
            return 0.0
        return (bid_notional - ask_notional) / total

    def depth_notional(self, levels: int, side: str) -> float:
        levels_slice = self.bids[:levels] if side == "bid" else self.asks[:levels]
        return sum(price * size for price, size in levels_slice)


@dataclass(frozen=True)
class MarketTrade:
    pair: str
    venue: str
    market_type: str
    symbol: str
    timestamp: datetime
    price: float
    size: float
    taker_side: str
    trade_id: str


@dataclass(frozen=True)
class FundingSnapshot:
    pair: str
    venue: str
    symbol: str
    timestamp: datetime
    current_funding_rate: float | None
    average_funding_rate: float | None
    next_funding_time: datetime | None
    basis_rate: float | None
    basis_value: float | None


@dataclass(frozen=True)
class BasisObservation:
    pair: str
    timestamp: datetime
    spot_venue: str
    spot_symbol: str
    perp_venue: str
    perp_symbol: str
    spot_mid: float
    perp_mid: float
    premium_bps: float
    spot_imbalance: float
    perp_imbalance: float
    current_funding_rate: float | None
    average_funding_rate: float | None
    next_funding_time: datetime | None
    basis_rate: float | None
    basis_value: float | None


@dataclass(frozen=True)
class SignalEvent:
    pair: str
    venue: str
    market_type: str
    symbol: str
    timestamp: datetime
    side: str
    signal_source: str
    entry_bid: float
    entry_ask: float
    entry_mid: float
    spread_bps: float
    imbalance: float
    threshold: float


@dataclass
class PaperOrder:
    order_id: int
    signal_id: int
    pair: str
    venue: str
    market_type: str
    symbol: str
    side: str
    posted_at: datetime
    expires_at: datetime
    limit_price: float
    quantity: float
    status: str = "OPEN"
    filled_at: datetime | None = None
    fill_price: float | None = None
    fill_trade_id: str | None = None


@dataclass(frozen=True)
class PendingMarkout:
    target_kind: str
    target_id: int
    venue: str
    market_type: str
    symbol: str
    side: str
    reference_price: float
    due_at: datetime
    horizon_ms: int


@dataclass
class SpreadPosition:
    position_id: int
    signal_id: int
    pair: str
    side: str
    regime: str
    entry_time: datetime
    quantity: float
    entry_spot_price: float
    entry_perp_price: float
    entry_basis_bps: float
    basis_exit_threshold_bps: float
    entry_funding_rate: float | None
    signal_quality_score: float | None
    signal_quality_band: str
    spot_imbalance: float
    perp_imbalance: float
