from __future__ import annotations

from .models import BasisSnapshot, MarketQuote, PairSettings


def normalize_futures_quote(pair: PairSettings, futures_quote: MarketQuote) -> tuple[float, float]:
    if pair.normalization == "identity":
        return futures_quote.bid, futures_quote.ask

    if pair.normalization == "invert":
        if futures_quote.bid <= 0 or futures_quote.ask <= 0:
            raise ValueError(f"inverted quote must be positive for {pair.name}")
        return 1.0 / futures_quote.ask, 1.0 / futures_quote.bid

    raise ValueError(f"unsupported normalization mode {pair.normalization}")


def build_snapshot(pair: PairSettings, futures_quote: MarketQuote, spot_quote: MarketQuote) -> BasisSnapshot:
    normalized_futures_bid, normalized_futures_ask = normalize_futures_quote(pair, futures_quote)
    normalized_futures_mid = (normalized_futures_bid + normalized_futures_ask) / 2.0
    gap_price = normalized_futures_mid - spot_quote.mid
    gap_pips = gap_price / pair.pip_size

    direction = "NONE"
    if gap_pips >= pair.threshold_pips:
        direction = "BUY"
    elif gap_pips <= -pair.threshold_pips:
        direction = "SELL"

    return BasisSnapshot(
        pair=pair.name,
        timestamp=max(futures_quote.timestamp, spot_quote.timestamp),
        direction=direction,
        raw_futures_bid=futures_quote.bid,
        raw_futures_ask=futures_quote.ask,
        normalized_futures_bid=normalized_futures_bid,
        normalized_futures_ask=normalized_futures_ask,
        spot_bid=spot_quote.bid,
        spot_ask=spot_quote.ask,
        futures_mid=futures_quote.mid,
        normalized_futures_mid=normalized_futures_mid,
        spot_mid=spot_quote.mid,
        gap_price=gap_price,
        gap_pips=gap_pips,
        threshold_pips=pair.threshold_pips,
    )
