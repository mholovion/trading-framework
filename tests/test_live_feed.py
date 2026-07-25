"""tradingkit.core.live_feed — LiveFeed pub-sub routing."""
from __future__ import annotations

import inspect

from tradingkit.core.live_feed import LiveFeed


def _candle(ts: int, close: float = 100.0) -> dict:
    return {"timestamp": ts, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0}


async def test_publish_does_not_cross_exchanges_for_same_symbol_and_timeframe():
    """Regression: publish()/subscribe() used to key on (symbol, timeframe) only,
    silently dropping exchange -- two different exchanges streaming the same symbol
    at the same timeframe would cross-deliver each other's candles."""
    feed = LiveFeed()
    q_whitebit = await feed.subscribe("whitebit", "SOL_USDT", "1m")
    q_binance  = await feed.subscribe("binance",  "SOL_USDT", "1m")

    feed.publish("whitebit", "SOL_USDT", "1m", _candle(1000, close=73.78))

    delivered = q_whitebit.get_nowait()
    assert delivered["time"] == 1000
    assert delivered["close"] == 73.78
    assert q_binance.empty()


async def test_publish_matches_exchange_case_insensitively():
    """Regression: exchange tags aren't consistently cased at the connection layer
    (observed "whitebit" vs "WhiteBit" for the same real exchange) -- routing must
    normalize case or those get silently treated as different feeds."""
    feed = LiveFeed()
    q = await feed.subscribe("WhiteBit", "BTC_USDT", "1m")

    feed.publish("whitebit", "BTC_USDT", "1m", _candle(2000, close=100.0))

    assert q.get_nowait()["close"] == 100.0


def test_publish_with_no_subscribers_is_a_noop():
    feed = LiveFeed()
    feed.publish("whitebit", "BTC_USDT", "1m", _candle(1000))  # must not raise


async def test_unsubscribe_stops_delivery():
    feed = LiveFeed()
    q = await feed.subscribe("whitebit", "SOL_USDT", "1m")
    feed.unsubscribe("whitebit", "SOL_USDT", "1m", q)

    feed.publish("whitebit", "SOL_USDT", "1m", _candle(1000))

    assert q.empty()


async def test_publish_drops_oldest_when_queue_is_full():
    feed = LiveFeed()
    q = await feed.subscribe("whitebit", "SOL_USDT", "1m")
    for ts in range(105):  # queue maxsize is 100
        feed.publish("whitebit", "SOL_USDT", "1m", _candle(ts))

    assert q.qsize() == 100
    first = q.get_nowait()
    assert first["time"] == 5  # oldest 5 were dropped to make room


def test_subscribe_and_unsubscribe_are_sync_and_async_respectively():
    # subscribe() is async (returns a Queue you must await), unsubscribe() is sync --
    # asserting the shape here since a mismatch would only surface at call time.
    assert inspect.iscoroutinefunction(LiveFeed.subscribe)
    assert not inspect.iscoroutinefunction(LiveFeed.unsubscribe)
