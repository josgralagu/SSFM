"""
bootstrap.py
============
Trade-level bootstrap resampling to estimate the 95% confidence interval
of the expectancy (mean net PnL per trade).

Method
------
IID bootstrap over completed trades:
  1. Resample N trades with replacement (N = number of original trades).
  2. Compute mean net PnL of the resample.
  3. Repeat B times.
  4. Report the 2.5th and 97.5th percentiles as the 95% CI.

Limitation acknowledged
-----------------------
IID bootstrap does not preserve serial autocorrelation. In a
trend-following system, consecutive trades may be correlated. The result
should be interpreted as an approximation. Block bootstrap is deferred
to a future spec version.

This module does NOT modify the original trade list.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config import settings as S

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BootstrapResult:
    """
    Result of a trade-level bootstrap simulation.

    Attributes
    ----------
    observed_expectancy : float
        Mean net PnL per trade in the original sample.
    ci_lower : float
        Lower bound of the confidence interval.
    ci_upper : float
        Upper bound of the confidence interval.
    confidence_level : float
        Confidence level used (e.g. 0.95).
    n_resamples : int
        Number of bootstrap iterations performed.
    expectancy_distribution : np.ndarray
        Full distribution of resampled expectancy values.
    """
    observed_expectancy: float
    ci_lower: float
    ci_upper: float
    confidence_level: float
    n_resamples: int
    expectancy_distribution: np.ndarray


def run_bootstrap(trade_df: pd.DataFrame) -> BootstrapResult:
    """
    Run trade-level bootstrap on the net PnL column.

    Parameters
    ----------
    trade_df : pd.DataFrame
        Output of ``Ledger.to_dataframe()``.
        Must contain column ``net_pnl``.

    Returns
    -------
    BootstrapResult

    Raises
    ------
    ValueError
        If trade_df is empty or missing net_pnl column.
    """
    if trade_df.empty:
        raise ValueError("Cannot bootstrap: no trades found.")
    if "net_pnl" not in trade_df.columns:
        raise ValueError("trade_df must contain 'net_pnl' column.")

    pnl_values: np.ndarray = trade_df["net_pnl"].to_numpy(dtype=np.float64)
    n_trades = len(pnl_values)
    observed_expectancy = float(pnl_values.mean())

    rng = np.random.default_rng(S.BOOTSTRAP_RANDOM_SEED)
    n_resamples = S.BOOTSTRAP_RESAMPLES

    # Vectorised bootstrap: draw all samples at once.
    indices = rng.integers(0, n_trades, size=(n_resamples, n_trades))
    resampled_means = pnl_values[indices].mean(axis=1)

    alpha = 1.0 - S.BOOTSTRAP_CONFIDENCE
    ci_lower = float(np.percentile(resampled_means, 100 * alpha / 2))
    ci_upper = float(np.percentile(resampled_means, 100 * (1 - alpha / 2)))

    logger.info(
        "Bootstrap (%d resamples): expectancy=%.2f  "
        "CI[%.0f%%]: [%.2f, %.2f]",
        n_resamples,
        observed_expectancy,
        S.BOOTSTRAP_CONFIDENCE * 100,
        ci_lower,
        ci_upper,
    )

    return BootstrapResult(
        observed_expectancy=observed_expectancy,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence_level=S.BOOTSTRAP_CONFIDENCE,
        n_resamples=n_resamples,
        expectancy_distribution=resampled_means,
    )
