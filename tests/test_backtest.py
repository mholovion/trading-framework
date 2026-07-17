"""tradingkit.backtest — BacktestRunner, BacktestResult, Trade pairing."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.backtest import BacktestResult, BacktestRunner
from tradingkit.backtest.result import Trade
from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.strategy import BarContext, Signal, Strategy


class ConstantRSI(Indicator):
    """Feeds a scripted sequence of RSI values so buy/sell pairing is deterministic."""
    def __init__(self, values, **params):
        super().__init__(**params)
        self._values = values

    def compute(self, ctx: IndicatorContext):
        import numpy as np
        return np.array(self._values[: len(ctx)])

    def required_periods(self) -> int:
        return 1


class ThresholdStrategy(Strategy):
    async def on_bar(self, bar: BarContext):
        if bar.rsi < 30:
            return Signal("buy", 0.8)
        if bar.rsi > 70:
            return Signal("sell", 0.7)
        return None


@pytest.fixture
def data() -> pl.DataFrame:
    return pl.DataFrame({
        "timestamp": [0, 60, 120, 180, 240],
        "open":  [10.0, 10.0, 10.0, 10.0, 10.0],
        "high":  [10.0, 10.0, 10.0, 10.0, 10.0],
        "low":   [10.0, 10.0, 10.0, 10.0, 10.0],
        "close": [10.0, 11.0, 12.0, 13.0, 14.0],
        "volume": [1.0, 1.0, 1.0, 1.0, 1.0],
    })


async def test_backtest_pairs_buy_and_sell_into_a_trade(data):
    runner = BacktestRunner()
    result = await runner.run(
        data=data,
        indicators={"rsi": ConstantRSI([20, 50, 50, 50, 80], period=1)},
        strategy=ThresholdStrategy(),
    )
    assert result.total_trades == 1
    trade = result.trades[0]
    assert trade.side == "buy"
    assert trade.entry_price == 10.0
    assert trade.exit_price == 14.0
    assert trade.pnl == 4.0
    assert trade.is_win is True


async def test_backtest_no_signals_produces_no_trades(data):
    runner = BacktestRunner()
    result = await runner.run(
        data=data,
        indicators={"rsi": ConstantRSI([50, 50, 50, 50, 50], period=1)},
        strategy=ThresholdStrategy(),
    )
    assert result.total_trades == 0
    assert result.win_rate == 0.0


async def test_backtest_empty_data_returns_empty_result():
    runner = BacktestRunner()
    empty = pl.DataFrame(schema={"timestamp": pl.Int64, "close": pl.Float64})
    result = await runner.run(data=empty, indicators={}, strategy=ThresholdStrategy())
    assert result.total_trades == 0
    assert result.signals == []


def test_backtest_result_summary_and_properties():
    trades = [
        Trade(entry_ts=0, exit_ts=1, side="buy", entry_price=10, exit_price=12, pnl=2, pnl_pct=20),
        Trade(entry_ts=2, exit_ts=3, side="buy", entry_price=10, exit_price=8, pnl=-2, pnl_pct=-20),
    ]
    result = BacktestResult(trades=trades, signals=[], data=pl.DataFrame(), indicators={})
    assert result.total_trades == 2
    assert len(result.winning_trades) == 1
    assert len(result.losing_trades) == 1
    assert result.win_rate == 0.5
    assert result.total_pnl == 0
    assert result.avg_pnl == 0
    summary = result.summary()
    assert summary["total_trades"] == 2
    assert summary["wins"] == 1


def test_backtest_result_max_drawdown():
    trades = [
        Trade(entry_ts=0, exit_ts=1, side="buy", entry_price=10, exit_price=15, pnl=5, pnl_pct=50),
        Trade(entry_ts=2, exit_ts=3, side="buy", entry_price=10, exit_price=7, pnl=-3, pnl_pct=-30),
        Trade(entry_ts=4, exit_ts=5, side="buy", entry_price=10, exit_price=8, pnl=-2, pnl_pct=-20),
    ]
    result = BacktestResult(trades=trades, signals=[], data=pl.DataFrame(), indicators={})
    # peak after trade1 = 5, trough after trade3 = 0 -> drawdown = 5
    assert result.max_drawdown == 5


def test_backtest_result_empty_trades_properties_dont_crash():
    result = BacktestResult(trades=[], signals=[], data=pl.DataFrame(), indicators={})
    assert result.win_rate == 0.0
    assert result.avg_pnl == 0.0
    assert result.max_drawdown == 0.0