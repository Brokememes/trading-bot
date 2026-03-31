from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Mt5Settings:
    path: str | None = None
    login: int | None = None
    password: str | None = None
    server: str | None = None
    initialize_timeout_ms: int = 5000
    poll_interval_ms: int = 200
    deviation_points: int = 20
    magic: int = 260330


@dataclass(frozen=True)
class TelegramSettings:
    enabled: bool = False
    bot_token: str | None = None
    chat_id: str | None = None


@dataclass(frozen=True)
class FuturesProviderSettings:
    kind: str = "polygon"
    api_key: str | None = None
    polygon_url: str = "wss://socket.polygon.io/futures"
    ib_host: str = "127.0.0.1"
    ib_port: int = 7497
    ib_client_id: int = 27


@dataclass(frozen=True)
class PairSettings:
    name: str
    spot_symbol: str
    futures_symbol: str
    pip_size: float
    threshold_pips: float
    normalization: str = "identity"
    lot: float = 0.01
    cooldown_seconds: float = 15.0
    close_tolerance_pips: float = 0.2
    enabled: bool = True
    ib_contract: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AppSettings:
    database_path: str = "data/basis_arb.sqlite"
    execute_trades: bool = False
    dry_run: bool = True
    stale_after_ms: int = 3000
    mt5: Mt5Settings = field(default_factory=Mt5Settings)
    telegram: TelegramSettings = field(default_factory=TelegramSettings)
    futures_provider: FuturesProviderSettings = field(default_factory=FuturesProviderSettings)
    pairs: tuple[PairSettings, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class MarketQuote:
    pair: str
    symbol: str
    bid: float
    ask: float
    timestamp: datetime
    source: str

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class BasisSnapshot:
    pair: str
    timestamp: datetime
    direction: str
    raw_futures_bid: float
    raw_futures_ask: float
    normalized_futures_bid: float
    normalized_futures_ask: float
    spot_bid: float
    spot_ask: float
    futures_mid: float
    normalized_futures_mid: float
    spot_mid: float
    gap_price: float
    gap_pips: float
    threshold_pips: float


@dataclass(frozen=True)
class AlertEvent:
    pair: str
    direction: str
    timestamp: datetime
    raw_futures_price: float
    normalized_futures_price: float
    spot_price: float
    gap_pips: float
    lot: float
    sl_price: float
    tp_price: float


@dataclass
class TrackedAlert:
    alert_id: int
    pair: str
    direction: str
    opened_at: datetime
    entry_gap_pips: float
    threshold_pips: float
    close_tolerance_pips: float
    max_gap_abs_pips: float
    closed: bool = False
