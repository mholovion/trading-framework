"""
tradingkit.backtest.result — BacktestResult and Trade.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass
class Trade:
    entry_ts:    int
    exit_ts:     int
    side:        str      # "buy" or "sell"
    entry_price: float
    exit_price:  float
    pnl:         float
    pnl_pct:     float
    entry_signal_confidence: float = 0.0
    exit_signal_confidence:  float = 0.0

    @property
    def is_win(self) -> bool:
        return self.pnl > 0

    def to_dict(self) -> dict:
        return {
            "entry_ts":    self.entry_ts,
            "exit_ts":     self.exit_ts,
            "side":        self.side,
            "entry_price": self.entry_price,
            "exit_price":  self.exit_price,
            "pnl":         self.pnl,
            "pnl_pct":     self.pnl_pct,
        }


@dataclass
class BacktestResult:
    trades:     list[Trade]
    signals:    list[dict]
    data:       pl.DataFrame
    indicators: dict[str, pl.Series]

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def winning_trades(self) -> list[Trade]:
        return [t for t in self.trades if t.is_win]

    @property
    def losing_trades(self) -> list[Trade]:
        return [t for t in self.trades if not t.is_win]

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        return len(self.winning_trades) / len(self.trades)

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    @property
    def avg_pnl(self) -> float:
        if not self.trades:
            return 0.0
        return self.total_pnl / len(self.trades)

    @property
    def max_drawdown(self) -> float:
        if not self.trades:
            return 0.0
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in sorted(self.trades, key=lambda x: x.entry_ts):
            cumulative += t.pnl
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd
        return max_dd

    def summary(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "wins":         len(self.winning_trades),
            "losses":       len(self.losing_trades),
            "win_rate":     self.win_rate,
            "total_pnl":    self.total_pnl,
            "avg_pnl":      self.avg_pnl,
            "max_drawdown": self.max_drawdown,
            "signals":      len(self.signals),
        }
