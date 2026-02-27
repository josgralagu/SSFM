"""
resampler.py
============
Resamples M1 OHLCV data to M5, handling the CME Globex daily session
break correctly.

Critical design decisions
-------------------------
1. Timezone conversion happens BEFORE any time-based filtering.
   The CME session break is defined in America/Chicago local time.
   Filtering on a fixed UTC offset would produce errors on DST boundaries.

2. Bars that fall entirely within the session break (17:00–17:59 CT)
   are excluded before resampling. This prevents a single M5 bar from
   spanning data from two different trading sessions.

3. After resampling, bars where ``open`` is NaN (no trades in that
   5-minute window) are dropped. This eliminates synthetic empty bars.

4. The output index is converted back to UTC for consistency with the
   rest of the pipeline.
"""

from __future__ import annotations

import logging

import pandas as pd

from config import constants as C
from config import settings as S

logger = logging.getLogger(__name__)


def _exclude_session_break(df_ct: pd.DataFrame) -> pd.DataFrame:
    """
    Remove M1 bars that fall within the CME daily session break.

    The break is 17:00–17:59 inclusive, America/Chicago local time.

    Parameters
    ----------
    df_ct : pd.DataFrame
        M1 DataFrame indexed in America/Chicago time.

    Returns
    -------
    pd.DataFrame
        DataFrame with break bars removed.
    """
    break_start = pd.Timestamp(C.SESSION_BREAK_START).time()
    break_end = pd.Timestamp(C.SESSION_BREAK_END).time()

    # between_time is inclusive on both ends by default.
    break_mask = (df_ct.index.time >= break_start) & (
        df_ct.index.time <= break_end
    )
    filtered = df_ct.loc[~break_mask]

    removed = break_mask.sum()
    if removed > 0:
        logger.debug("Excluded %d M1 bars in session break.", removed)

    return filtered


def resample_m1_to_m5(df_m1: pd.DataFrame) -> pd.DataFrame:
    """
    Resample a UTC-indexed M1 OHLCV DataFrame to M5.

    Steps
    -----
    1. Convert index UTC → America/Chicago.
    2. Exclude bars in the 17:00–17:59 CT session break.
    3. Resample to 5-minute bars using left-closed, left-labelled intervals.
    4. Drop empty bars (no trades in window).
    5. Convert index back to UTC.

    Parameters
    ----------
    df_m1 : pd.DataFrame
        Raw M1 data with UTC DatetimeIndex and columns:
        open, high, low, close, volume.

    Returns
    -------
    pd.DataFrame
        M5 OHLCV DataFrame with UTC DatetimeIndex.
        Each bar's timestamp represents the bar OPEN time.

    Raises
    ------
    ValueError
        If required OHLCV columns are missing.
    """
    required = (C.COL_OPEN, C.COL_HIGH, C.COL_LOW, C.COL_CLOSE, C.COL_VOLUME)
    missing = [c for c in required if c not in df_m1.columns]
    if missing:
        raise ValueError(f"Missing columns for resampling: {missing}")

    # Step 1 — Convert to Chicago time for session-aware filtering.
    df_ct = df_m1.copy()
    df_ct.index = df_ct.index.tz_convert(C.SESSION_TIMEZONE)

    # Step 2 — Remove session break bars.
    df_ct = _exclude_session_break(df_ct)

    # Step 3 — Resample to M5.
    ohlcv_agg: dict[str, str] = {
        C.COL_OPEN: "first",
        C.COL_HIGH: "max",
        C.COL_LOW: "min",
        C.COL_CLOSE: "last",
        C.COL_VOLUME: "sum",
    }

    # Carry forward only OHLCV columns to avoid aggregating metadata columns.
    df_ohlcv = df_ct[[*ohlcv_agg.keys()]]

    df_m5_ct = df_ohlcv.resample(
        S.RESAMPLE_FREQ,
        closed=S.RESAMPLE_CLOSED,
        label=S.RESAMPLE_LABEL,
    ).agg(ohlcv_agg)

    # Step 3b — Propagate roll flag if present in M1 data.
    # A M5 bar is flagged contains_roll=True if ANY of its constituent
    # M1 bars was a roll bar. This flag is consumed by the backtest engine
    # to trigger forced close and freeze logic. It does NOT affect OHLCV.
    if "is_roll" in df_ct.columns:
        roll_m5 = (
            df_ct["is_roll"]
            .astype(int)  # True→1, False→0 so sum() works with resample
            .resample(S.RESAMPLE_FREQ, closed=S.RESAMPLE_CLOSED, label=S.RESAMPLE_LABEL)
            .sum()
            .gt(0)        # True if any M1 bar in the block was a roll
        )
        df_m5_ct["contains_roll"] = roll_m5.reindex(df_m5_ct.index, fill_value=False)
    else:
        df_m5_ct["contains_roll"] = False

    # Step 4 — Drop bars with no trades (open is NaN when no data).
    # contains_roll is preserved; a roll bar will never have NaN open
    # because the roll event itself generates trades.
    df_m5_ct = df_m5_ct.dropna(subset=[C.COL_OPEN])

    # Step 5 — Convert back to UTC.
    df_m5 = df_m5_ct.copy()
    df_m5.index = df_m5_ct.index.tz_convert("UTC")

    logger.info(
        "Resampled M1 (%d bars) → M5 (%d bars).",
        len(df_m1),
        len(df_m5),
    )

    return df_m5
