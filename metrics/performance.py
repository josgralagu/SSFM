"""
performance.py
==============
Computes summary performance metrics from a completed trade DataFrame
and its associated equity curve.

This module does NOT modify trades or equity data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config import settings as S

logger = logging.getLogger(__name__)

# Approximate number of M5 bars in one trading year.
# 6E trades ~23 hours/day, 5 days/week, ~252 days/year.
# 23h × 60min/5min × 252 = 69,552 bars/year.
M5_BARS_PER_YEAR: float = 69_552.0


@dataclass(frozen=True)
class PerformanceSummary:
    """
    Container for all required performance metrics.

    Attributes
    ----------
    total_trades : int
    winning_trades : int
    losing_trades : int
    win_rate : float           Fraction of trades that are winners [0, 1].
    gross_profit : float       Sum of positive net PnL across all trades.
    gross_loss : float         Absolute sum of negative net PnL.
    net_profit : float         gross_profit - gross_loss.
    profit_factor : float      gross_profit / |gross_loss|. inf if no losses.
    expectancy : float         Mean net PnL per trade.
    avg_win : float            Mean net PnL of winning trades.
    avg_loss : float           Mean net PnL of losing trades (negative).
    cagr : float               Compound Annual Growth Rate (as decimal).
    sharpe_ratio : float       Annualised Sharpe ratio.
    """
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    net_profit: float
    profit_factor: float
    expectancy: float
    avg_win: float
    avg_loss: float
    cagr: float
    sharpe_ratio: float


def compute_performance(
    trade_df: pd.DataFrame,
    equity: pd.Series,
) -> PerformanceSummary:
    """
    Compute all required performance metrics.

    Parameters
    ----------
    trade_df : pd.DataFrame
        Output of ``Ledger.to_dataframe()``.
        Required columns: net_pnl, is_winner.
    equity : pd.Series
        Bar-resolution equity curve (cumulative net PnL).

    Returns
    -------
    PerformanceSummary

    Raises
    ------
    ValueError
        If trade_df is empty.
    """
    if trade_df.empty:
        raise ValueError("Cannot compute performance: no trades found.")

    pnl = trade_df["net_pnl"]
    winners = trade_df[trade_df["is_winner"]]
    losers = trade_df[~trade_df["is_winner"]]

    total = len(trade_df)
    n_win = len(winners)
    n_loss = len(losers)

    gross_profit = winners["net_pnl"].sum() if not winners.empty else 0.0
    gross_loss = losers["net_pnl"].sum() if not losers.empty else 0.0

    profit_factor = (
        gross_profit / abs(gross_loss)
        if gross_loss != 0
        else float("inf")
    )

    avg_win = winners["net_pnl"].mean() if not winners.empty else float("nan")
    avg_loss = losers["net_pnl"].mean() if not losers.empty else float("nan")

    cagr = _compute_cagr(equity)
    sharpe = _compute_sharpe(equity)

    summary = PerformanceSummary(
        total_trades=total,
        winning_trades=n_win,
        losing_trades=n_loss,
        win_rate=n_win / total,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        net_profit=gross_profit + gross_loss,
        profit_factor=profit_factor,
        expectancy=float(pnl.mean()),
        avg_win=avg_win,
        avg_loss=avg_loss,
        cagr=cagr,
        sharpe_ratio=sharpe,
    )

    logger.info(
        "Performance: trades=%d  win_rate=%.1f%%  PF=%.2f  "
        "expectancy=%.2f  CAGR=%.2f%%  Sharpe=%.2f",
        total, summary.win_rate * 100, profit_factor,
        summary.expectancy, cagr * 100, sharpe,
    )

    return summary


def _compute_cagr(equity: pd.Series) -> float:
    """
    Compute CAGR from an equity curve expressed as cumulative USD PnL.

    CAGR is computed as:
        (final_equity / initial_equity) ^ (1 / years) - 1

    Because the curve starts at 0 (cumulative PnL), we convert to an
    index relative to a notional capital of 1 USD per unit for the
    purpose of the ratio calculation.
    When the first equity value is 0, CAGR is undefined and returns NaN.
    """
    if len(equity) < 2:
        return float("nan")

    n_bars = len(equity)
    years = n_bars / M5_BARS_PER_YEAR

    start = equity.iloc[0]
    end = equity.iloc[-1]

    if start == 0:
        # Curve starts at 0 — use total return over the period instead.
        logger.warning(
            "Equity starts at 0; CAGR returned as NaN. "
            "Use initial_capital > 0 for meaningful CAGR."
        )
        return float("nan")

    ratio = end / start
    if ratio <= 0:
        return float("nan")

    return ratio ** (1.0 / years) - 1.0


def _compute_sharpe(equity: pd.Series) -> float:
    """
    Compute the annualised Sharpe ratio from bar-level equity changes.

    bar_return[i] = equity[i] - equity[i-1]  (USD PnL per bar)
    Sharpe = mean(bar_return) / std(bar_return) × sqrt(bars_per_year)
    """
    if len(equity) < 2:
        return float("nan")

    bar_returns = equity.diff().dropna()

    std = float(bar_returns.std())
    if std == 0:
        return float("nan")

    mean = float(bar_returns.mean())
    sharpe = (mean / std) * np.sqrt(M5_BARS_PER_YEAR)

    return sharpe
