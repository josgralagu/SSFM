"""
loader.py
=========
Loads the raw M1 Parquet file produced by downloader.py and validates
that the schema contract between the data layer and the preprocessing
layer is satisfied.

This module does NOT resample or transform data.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config import constants as C
from config import settings as S

logger = logging.getLogger(__name__)

# Minimum required columns in the raw Parquet file.
_REQUIRED_COLUMNS: tuple[str, ...] = (
    C.COL_OPEN,
    C.COL_HIGH,
    C.COL_LOW,
    C.COL_CLOSE,
    C.COL_VOLUME,
)


def _raw_parquet_path() -> Path:
    """Return the canonical path for the raw M1 Parquet file."""
    start = S.DATA_START[:10].replace("-", "")
    end = S.DATA_END[:10].replace("-", "")
    filename = f"6E_M1_continuous_c0_{start}_{end}.parquet"
    return S.DATA_DIR / filename


def load_raw_m1(path: Path | None = None) -> pd.DataFrame:
    """
    Load the raw M1 Parquet file into a DataFrame.

    The index is set to a UTC-aware DatetimeIndex. All OHLCV columns
    are cast to float64. Volume is cast to int64.

    Parameters
    ----------
    path : Path, optional
        Explicit path to the Parquet file. If None, the canonical path
        derived from settings is used.

    Returns
    -------
    pd.DataFrame
        Raw M1 OHLCV data with a UTC DatetimeIndex.

    Raises
    ------
    FileNotFoundError
        If the Parquet file does not exist.
    ValueError
        If required columns are missing or the index is not a DatetimeIndex.
    """
    if path is None:
        path = _raw_parquet_path()

    if not path.exists():
        raise FileNotFoundError(
            f"Raw data file not found: {path}\n"
            "Run downloader.download() first."
        )

    logger.info("Loading raw M1 data from %s ...", path)
    df = pd.read_parquet(path)

    # Validate required columns.
    missing = [col for col in _REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in raw data: {missing}")

    # Ensure DatetimeIndex with UTC timezone.
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be a DatetimeIndex.")

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df.index.name = C.COL_TS

    # Enforce column dtypes.
    for col in (C.COL_OPEN, C.COL_HIGH, C.COL_LOW, C.COL_CLOSE):
        df[col] = df[col].astype("float64")
    df[C.COL_VOLUME] = df[C.COL_VOLUME].astype("int64")

    logger.info(
        "Loaded %d rows from %s to %s.",
        len(df),
        df.index[0],
        df.index[-1],
    )
    return df
