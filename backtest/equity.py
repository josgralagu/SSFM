"""
equity.py
=========
Builds bar-by-bar equity curves from completed trade records.

The equity curve tracks cumulative net PnL over time. It is used
by the drawdown module and for performance reporting.

Open-trade mark-to-market is NOT implemented in v1.2. The equity
curve moves only when a trade closes.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def build_equity_curve(
    trade_df: pd.DataFrame,
    bar_index: pd.DatetimeIndex,
    initial_capital: float = 0.0,
) -> pd.Series:
    """
    Construct a bar-resolution equity curve from closed trades.

    The curve is flat between trade closures and steps up/down at
    each exit bar. Open positions are NOT marked to market.

    Parameters
    ----------
    trade_df : pd.DataFrame
        DataFrame output of ``Ledger.to_dataframe()``.
        Must contain columns ``exit_bar`` and ``net_pnl``.
    bar_index : pd.DatetimeIndex
        Full M5 bar timestamps used as the curve's time axis.
    initial_capital : float
        Starting equity level (default 0 — curves show cumulative PnL).

    Returns
    -------
    pd.Series
        Equity (cumulative net PnL + initial_capital) indexed on
        ``bar_index``. Values are forward-filled between trade events.
    """
    if trade_df.empty:
        logger.warning("No trades found. Returning flat equity curve.")
        return pd.Series(initial_capital, index=bar_index)

    # Aggregate PnL per exit bar (multiple trades may close on same bar).
    pnl_by_bar = trade_df.groupby("exit_bar")["net_pnl"].sum()

    # Reindex to full bar timeline; missing bars → 0 PnL change.
    pnl_full = pnl_by_bar.reindex(bar_index, fill_value=0.0)

    equity = initial_capital + pnl_full.cumsum()

    logger.info(
        "Equity curve: start=%.2f  end=%.2f  range=%d bars.",
        equity.iloc[0], equity.iloc[-1], len(equity),
    )
    return equity


def split_equity(
    equity: pd.Series,
    split_timestamp: pd.Timestamp,
) -> tuple[pd.Series, pd.Series]:
    """
    Split an equity curve at the IS/OOS boundary.

    Parameters
    ----------
    equity : pd.Series
        Full equity curve.
    split_timestamp : pd.Timestamp
        First bar of the OOS period.

    Returns
    -------
    tuple[pd.Series, pd.Series]
        (equity_is, equity_oos)
    """
    equity_is = equity.loc[equity.index < split_timestamp]
    equity_oos = equity.loc[equity.index >= split_timestamp]
    return equity_is, equity_oos
