from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Callable

from ..models import FuturesProviderSettings, MarketQuote, PairSettings

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract
    from ibapi.wrapper import EWrapper
except ImportError:  # pragma: no cover - optional dependency
    EClient = None
    Contract = None
    EWrapper = None


class IbFuturesFeed:
    def __init__(
        self,
        settings: FuturesProviderSettings,
        pairs: list[PairSettings],
        callback: Callable[[MarketQuote], None],
    ) -> None:
        if EClient is None or Contract is None:
            raise RuntimeError("ibapi package is not installed")
        self._app = _IbApp(settings, pairs, callback)
        self._settings = settings

    def start(self) -> None:
        self._app.connect(self._settings.ib_host, self._settings.ib_port, self._settings.ib_client_id)
        self._app.start()

    def stop(self) -> None:
        self._app.stop()


if EClient is not None and EWrapper is not None:
    class _IbApp(EWrapper, EClient):  # pragma: no cover - live path
        def __init__(
            self,
            settings: FuturesProviderSettings,
            pairs: list[PairSettings],
            callback: Callable[[MarketQuote], None],
        ) -> None:
            EWrapper.__init__(self)
            EClient.__init__(self, wrapper=self)
            self._pairs = pairs
            self._callback = callback
            self._thread: threading.Thread | None = None
            self._pair_by_req_id: dict[int, PairSettings] = {}
            self._bid_cache: dict[int, float] = {}
            self._ask_cache: dict[int, float] = {}

        def start(self) -> None:
            self._thread = threading.Thread(target=self.run, name="ib-futures-feed", daemon=True)
            self._thread.start()
            time.sleep(1.0)
            for index, pair in enumerate(self._pairs, start=1):
                self._pair_by_req_id[index] = pair
                self.reqMktData(index, _build_contract(pair), "", False, False, [])

        def stop(self) -> None:
            try:
                self.disconnect()
            finally:
                if self._thread is not None:
                    self._thread.join(timeout=5)

        def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:  # noqa: N802
            if price <= 0:
                return
            if tickType == 1:
                self._bid_cache[reqId] = price
            elif tickType == 2:
                self._ask_cache[reqId] = price

            if reqId in self._bid_cache and reqId in self._ask_cache:
                pair = self._pair_by_req_id[reqId]
                self._callback(
                    MarketQuote(
                        pair=pair.name,
                        symbol=pair.futures_symbol,
                        bid=self._bid_cache[reqId],
                        ask=self._ask_cache[reqId],
                        timestamp=datetime.now(tz=timezone.utc),
                        source="ib",
                    )
                )

        def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:  # noqa: N802,E501
            if errorCode not in {2104, 2106, 2158}:
                print(f"[IB] reqId={reqId} code={errorCode} msg={errorString}")

else:
    class _IbApp:  # pragma: no cover - optional dependency missing
        def __init__(self, *args, **kwargs) -> None:
            raise RuntimeError("ibapi package is not installed")


def _build_contract(pair: PairSettings) -> Contract:  # pragma: no cover - live path
    contract = Contract()
    contract.secType = "FUT"
    for key, value in pair.ib_contract.items():
        setattr(contract, key, value)
    return contract
