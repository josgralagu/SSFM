"""
downloader.py
=============
Responsible for fetching raw M1 OHLCV data from Databento and
persisting it to disk as a Parquet file.

This module performs NO transformation. Raw data is saved exactly
as received from the provider.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

import pandas as pd

from config import constants as C
from config import settings as S

logger = logging.getLogger(__name__)


def _compute_sha256(path: Path) -> str:
    """Return the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _raw_parquet_path() -> Path:
    """Return the canonical path for the raw M1 Parquet file."""
    start = S.DATA_START[:10].replace("-", "")
    end = S.DATA_END[:10].replace("-", "")
    filename = f"6E_M1_continuous_c0_{start}_{end}.parquet"
    return S.DATA_DIR / filename


def _manifest_path() -> Path:
    """Return the canonical path for the dataset manifest JSON."""
    return _raw_parquet_path().with_suffix(".manifest.json")


def download(force: bool = False) -> Path:
    """
    Download raw M1 OHLCV data from Databento and save to Parquet.

    Parameters
    ----------
    force : bool
        If True, re-download even if the file already exists.

    Returns
    -------
    Path
        Path to the saved Parquet file.

    Raises
    ------
    EnvironmentError
        If DATABENTO_API_KEY is not set.
    ImportError
        If the `databento` package is not installed.
    """
    if not S.DATABENTO_API_KEY:
        raise EnvironmentError(
            "DATABENTO_API_KEY environment variable is not set."
        )

    try:
        import databento as db  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "The `databento` package is required. "
            "Install it with: pip install databento"
        ) from exc

    S.DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _raw_parquet_path()

    if out_path.exists() and not force:
        logger.info("Raw data file already exists, skipping download: %s", out_path)
        return out_path

    logger.info(
        "Estimating download cost for %s from %s to %s ...",
        C.SYMBOL_CONTINUOUS, S.DATA_START, S.DATA_END,
    )

    client = db.Historical(S.DATABENTO_API_KEY)

    # Always estimate cost before committing to download.
    cost = client.metadata.get_cost(
        dataset=C.DATASET,
        symbols=[C.SYMBOL_CONTINUOUS],
        stype_in=C.STYPE_IN,
        schema=C.SCHEMA,
        start=S.DATA_START,
        end=S.DATA_END,
    )
    logger.info("Estimated download cost: $%.4f", cost)

    logger.info("Starting download ...")
    data = client.timeseries.get_range(
        dataset=C.DATASET,
        symbols=[C.SYMBOL_CONTINUOUS],
        stype_in=C.STYPE_IN,
        stype_out=C.STYPE_OUT,
        schema=C.SCHEMA,
        start=S.DATA_START,
        end=S.DATA_END,
    )

    df: pd.DataFrame = data.to_df()
    logger.info("Downloaded %d rows.", len(df))

    df.to_parquet(out_path, index=True)
    logger.info("Saved raw data to %s", out_path)

    # Write manifest for reproducibility audit.
    _write_manifest(out_path)

    return out_path


def _write_manifest(parquet_path: Path) -> None:
    """Write a dataset manifest JSON alongside the Parquet file."""
    import databento as db  # type: ignore

    manifest = {
        "dataset_root": C.INSTRUMENT,
        "roll_rule": C.SYMBOL_CONTINUOUS.split(".")[-1],
        "schema": C.SCHEMA,
        "timezone": C.SESSION_TIMEZONE,
        "start": S.DATA_START,
        "end": S.DATA_END,
        "spec_version": "1.2",
        "databento_sdk_version": getattr(db, "__version__", "unknown"),
        "python_version": __import__("sys").version,
        "download_timestamp_utc": pd.Timestamp.utcnow().isoformat(),
        "sha256_raw_file": _compute_sha256(parquet_path),
    }

    manifest_path = _manifest_path()
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Manifest written to %s", manifest_path)
