"""
crossover.py
============
Generates entry signals from EMA crossover events.

Anti-lookahead contract
-----------------------
A signal at bar[i] is based exclusively on EMA values at bar[i] and
bar[i-1]. The signal directs the execution engine to enter at the OPEN
of bar[i+1]. This module produces no fills and applies no slippage.

Signal encoding
---------------
  +1  →  Long  (fast crossed above slow on this bar close)
  -1  →  Short (fast crossed below slow on this bar close)
   0  →  No crossover on this bar
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def generate_crossover_signals(
    ema_fast: pd.Series,
    ema_slow: pd.Series,
) -> pd.Series:
    """
    Produce a signal series from two EMA series.

    A crossover is detected when the sign of (ema_fast - ema_slow)
    changes between bar[i-1] and bar[i].

    Parameters
    ----------
    ema_fast : pd.Series
        Fast EMA series. May contain leading NaN from the warmup period.
    ema_slow : pd.Series
        Slow EMA series. Same index as ``ema_fast``.

    Returns
    -------
    pd.Series[int]
        Signal series with values in {-1, 0, +1}.
        NaN input bars produce signal 0 (no trade).

    Raises
    ------
    ValueError
        If the two series have different indices.
    """
    if not ema_fast.index.equals(ema_slow.index):
        raise ValueError("ema_fast and ema_slow must share the same index.")

    diff = ema_fast - ema_slow           # positive → fast above slow
    prev_diff = diff.shift(1)

    # Crossover conditions (strict: both current and previous must be non-NaN).
    valid = diff.notna() & prev_diff.notna()

    cross_up = valid & (diff > 0) & (prev_diff <= 0)   # fast crosses above
    cross_dn = valid & (diff < 0) & (prev_diff >= 0)   # fast crosses below

    signal = pd.Series(0, index=ema_fast.index, dtype=np.int8)
    signal.loc[cross_up] = 1
    signal.loc[cross_dn] = -1

    n_long = int(cross_up.sum())
    n_short = int(cross_dn.sum())
    logger.info(
        "Crossover signals: %d long, %d short.", n_long, n_short
    )

    return signal
