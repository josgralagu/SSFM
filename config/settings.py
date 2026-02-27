"""
settings.py
===========
All tunable parameters for SFFM v1.2.
This is the single source of truth for every configurable value.
No parameter may be hardcoded outside this module.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data_cache"
OUTPUT_DIR: Path = PROJECT_ROOT / "output"

# ---------------------------------------------------------------------------
# Databento API
# ---------------------------------------------------------------------------
DATABENTO_API_KEY: str = os.environ.get("DATABENTO_API_KEY", "")

# ---------------------------------------------------------------------------
# Historical data range
# Modify start/end to change the backtest universe.
# ---------------------------------------------------------------------------
DATA_START: str = "2019-01-01T00:00:00"
DATA_END: str = "2024-01-01T00:00:00"

# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------
RESAMPLE_FREQ: str = "5min"       # Target bar frequency
RESAMPLE_CLOSED: str = "left"     # Interval closure convention
RESAMPLE_LABEL: str = "left"      # Bar label convention

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------
EMA_FAST: int = 20
EMA_SLOW: int = 50
WARMUP_BARS: int = 200            # Bars discarded before signal activation

# ---------------------------------------------------------------------------
# IS / OOS split
# ---------------------------------------------------------------------------
IS_FRACTION: float = 0.70         # Proportion of data used for In-Sample

# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
SLIPPAGE_TICKS: int = 1           # Ticks of adverse slippage per fill
COMMISSION_PER_SIDE: float = 2.50 # USD per contract per side
CONTRACTS: int = 1                # Fixed position size (single contract)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
BOOTSTRAP_RESAMPLES: int = 1_000
BOOTSTRAP_CONFIDENCE: float = 0.95
BOOTSTRAP_RANDOM_SEED: int = 42

# ---------------------------------------------------------------------------
# Risk-free rate (annualised, for Sharpe calculation)
# ---------------------------------------------------------------------------
RISK_FREE_RATE: float = 0.00
