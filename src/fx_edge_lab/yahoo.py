from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

DEFAULT_SYMBOLS = {
    "dxy": "DX-Y.NYB",
    "eurusd": "EURUSD=X",
    "usdjpy": "JPY=X",
    "gbpusd": "GBPUSD=X",
    "usdcad": "CAD=X",
    "usdsek": "SEK=X",
    "usdchf": "CHF=X",
    "eurusd_future": "6E=F",
}


@dataclass(frozen=True)
class YahooQuote:
    symbol: str
    price: float
    as_of_utc: str


def fetch_latest_close(symbol: str) -> YahooQuote:
    encoded_symbol = urllib.parse.quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}?interval=1d&range=10d"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.load(response)

    result = payload["chart"]["result"][0]
    timestamps = result["timestamp"]
    closes = result["indicators"]["quote"][0]["close"]

    for timestamp, close_value in reversed(list(zip(timestamps, closes, strict=True))):
        if close_value is not None:
            as_of = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            return YahooQuote(symbol=symbol, price=float(close_value), as_of_utc=as_of)

    raise ValueError(f"no close data returned for symbol {symbol}")


def fetch_default_snapshot() -> tuple[dict[str, float], str]:
    prices: dict[str, float] = {}
    as_of = ""
    for key, symbol in DEFAULT_SYMBOLS.items():
        quote = fetch_latest_close(symbol)
        prices[key] = quote.price
        as_of = max(as_of, quote.as_of_utc)
    return prices, as_of
