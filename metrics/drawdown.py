"""
drawdown.py
===========
Computes drawdown statistics from an equity curve.

Drawdown at bar[i] is defined as the decline from the running peak:
    dd[i] = equity[i] - max(equity[0..i])

Maximum drawdown is the largest single trough below any preceding peak.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DrawdownResult:
    """
    Drawdown statistics for an equity curve.

    Attributes
    ----------
    max_drawdown_usd : float
        Largest absolute drawdown in USD (negative value).
    max_drawdown_pct : float
        Maximum drawdown as a fraction of the preceding peak equity.
        Negative. Returns NaN if any peak is zero or negative.
    drawdown_series : pd.Series
        Bar-resolution drawdown series (USD).
    peak_series : pd.Series
        Running equity peak series.
    """
    max_drawdown_usd: float
    max_drawdown_pct: float
    drawdown_series: pd.Series
    peak_series: pd.Series


def compute_drawdown(equity: pd.Series) -> DrawdownResult:
    """
    Compute drawdown statistics from an equity curve.

    Parameters
    ----------
    equity : pd.Series
        Cumulative PnL (or capital) indexed on a DatetimeIndex.

    Returns
    -------
    DrawdownResult
    """
    if equity.empty:
        raise ValueError("Cannot compute drawdown on an empty equity series.")

    peak = equity.cummax()
    drawdown_usd = equity - peak

    max_dd_usd = float(drawdown_usd.min())

    # Percentage drawdown relative to the peak at each bar.
    # Guard against division by zero when peak == 0.
    with np.errstate(invalid="ignore", divide="ignore"):
        dd_pct = np.where(peak != 0, drawdown_usd / peak, np.nan)

    dd_pct_series = pd.Series(dd_pct, index=equity.index)
    max_dd_pct = float(np.nanmin(dd_pct))

    logger.info(
        "Max drawdown: %.2f USD  /  %.2f%%",
        max_dd_usd, max_dd_pct * 100,
    )

    return DrawdownResult(
        max_drawdown_usd=max_dd_usd,
        max_drawdown_pct=max_dd_pct,
        drawdown_series=drawdown_usd,
        peak_series=peak,
    )
