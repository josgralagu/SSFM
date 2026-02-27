"""
roll_manager.py
===============
Detects contract roll events in a continuous futures series by monitoring
changes in the instrument_id column, and produces an auditable roll log.

This module does NOT modify prices. It only identifies and records
the bars at which the underlying contract changed.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config import constants as C

logger = logging.getLogger(__name__)


def detect_rolls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify bars where the underlying contract changed.

    Parameters
    ----------
    df : pd.DataFrame
        Raw M1 DataFrame with a column named by ``C.COL_INSTRUMENT_ID``.
        Index must be a DatetimeIndex.

    Returns
    -------
    pd.DataFrame
        A subset of ``df`` containing only the roll bars, with an
        additional column ``prev_instrument_id`` showing the outgoing
        contract.

    Raises
    ------
    KeyError
        If the instrument_id column is absent from ``df``.
    """
    if C.COL_INSTRUMENT_ID not in df.columns:
        raise KeyError(
            f"Column '{C.COL_INSTRUMENT_ID}' not found. "
            "Ensure data was downloaded with stype_out='raw_symbol'."
        )

    prev = df[C.COL_INSTRUMENT_ID].shift(1)
    roll_mask = (df[C.COL_INSTRUMENT_ID] != prev) & prev.notna()

    rolls = df.loc[roll_mask, [C.COL_INSTRUMENT_ID]].copy()
    rolls.insert(0, "prev_instrument_id", prev[roll_mask].values)
    rolls.rename(columns={C.COL_INSTRUMENT_ID: "new_instrument_id"}, inplace=True)

    logger.info("Detected %d roll event(s).", len(rolls))
    return rolls


def save_roll_log(rolls: pd.DataFrame, path: Path) -> None:
    """
    Persist the roll log to CSV for audit purposes.

    Parameters
    ----------
    rolls : pd.DataFrame
        Output of :func:`detect_rolls`.
    path : Path
        Destination CSV file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rolls.to_csv(path)
    logger.info("Roll log saved to %s", path)


def annotate_rolls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a boolean ``is_roll`` column to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw M1 DataFrame with instrument_id column.

    Returns
    -------
    pd.DataFrame
        Copy of ``df`` with ``is_roll`` column added.
    """
    out = df.copy()
    if C.COL_INSTRUMENT_ID in out.columns:
        prev = out[C.COL_INSTRUMENT_ID].shift(1)
        out["is_roll"] = (out[C.COL_INSTRUMENT_ID] != prev) & prev.notna()
    else:
        out["is_roll"] = False
    return out
