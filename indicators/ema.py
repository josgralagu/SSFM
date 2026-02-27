"""
ema.py
======
Computes Exponential Moving Averages (EMA) using the standard smoothing
factor:  alpha = 2 / (period + 1).

Wilder smoothing (alpha = 1 / period) is explicitly NOT used, per Spec 1.2.

Anti-lookahead guarantee
------------------------
EMA at bar[i] uses only prices up to and including bar[i]. The calculation
is entirely backward-looking. pandas ewm() with adjust=False satisfies
this requirement when applied to a fully-formed historical series.

Warmup
------
The first ``warmup_bars`` values are set to NaN, irrespective of the EMA
calculation. Signal generation must check for NaN before acting.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from config import settings as S

logger = logging.getLogger(__name__)


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    """
    Compute the EMA of a price series.

    Parameters
    ----------
    series : pd.Series
        Price series (typically Close prices).
    period : int
        EMA period. Must be >= 2.

    Returns
    -------
    pd.Series
        EMA values with the same index as ``series``.
        First ``S.WARMUP_BARS`` values are NaN.

    Raises
    ------
    ValueError
        If period < 2 or series is empty.
    """
    if period < 2:
        raise ValueError(f"EMA period must be >= 2, got {period}.")
    if series.empty:
        raise ValueError("Cannot compute EMA on an empty series.")

    alpha = 2.0 / (period + 1)

    # adjust=False: each EMA value is a weighted average of the current
    # price and the previous EMA, not an expanding sum — no lookahead.
    ema = series.ewm(alpha=alpha, adjust=False).mean()

    # Enforce warmup: mask the first WARMUP_BARS values as NaN so that
    # the signal module never fires before the indicators have stabilised.
    warmup = S.WARMUP_BARS
    if len(ema) >= warmup:
        ema.iloc[:warmup] = np.nan

    return ema


def compute_ema_pair(
    close: pd.Series,
    fast_period: int | None = None,
    slow_period: int | None = None,
) -> tuple[pd.Series, pd.Series]:
    """
    Compute the fast and slow EMA pair used by SFFM v1.2.

    Parameters
    ----------
    close : pd.Series
        Close price series.
    fast_period : int, optional
        Override for the fast EMA period (default: settings.EMA_FAST).
    slow_period : int, optional
        Override for the slow EMA period (default: settings.EMA_SLOW).

    Returns
    -------
    tuple[pd.Series, pd.Series]
        (ema_fast, ema_slow) — both share the same index as ``close``.
    """
    fp = fast_period if fast_period is not None else S.EMA_FAST
    sp = slow_period if slow_period is not None else S.EMA_SLOW

    ema_fast = compute_ema(close, fp)
    ema_slow = compute_ema(close, sp)

    logger.debug(
        "Computed EMA(%d) and EMA(%d) over %d bars.",
        fp, sp, len(close),
    )

    return ema_fast, ema_slow
