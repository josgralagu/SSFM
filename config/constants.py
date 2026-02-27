"""
constants.py
============
Immutable instrument and market constants for the 6E futures contract.
These values reflect CME specifications and must not be modified without
a corresponding spec version change.
"""

# ---------------------------------------------------------------------------
# Instrument identity
# ---------------------------------------------------------------------------
INSTRUMENT: str = "6E"
DATASET: str = "GLBX.MDP3"
SYMBOL_CONTINUOUS: str = "6E.c.0"
STYPE_IN: str = "continuous"
STYPE_OUT: str = "raw_symbol"
SCHEMA: str = "ohlcv-1m"

# ---------------------------------------------------------------------------
# Contract specifications (CME Euro FX Futures)
# ---------------------------------------------------------------------------
TICK_SIZE: float = 0.00005        # Minimum price increment
TICK_VALUE: float = 6.25          # USD value per tick per contract
CONTRACT_SIZE: int = 125_000      # Notional EUR per contract

# ---------------------------------------------------------------------------
# Session definition (America/Chicago local time)
# Globex 6E trades 17:00 Sun – 16:00 Fri with a daily break 17:00–18:00 CT
# ---------------------------------------------------------------------------
SESSION_TIMEZONE: str = "America/Chicago"
SESSION_BREAK_START: str = "17:00"   # Inclusive break start (CT)
SESSION_BREAK_END: str = "17:59"     # Inclusive break end   (CT)

# ---------------------------------------------------------------------------
# Raw data column names (as delivered by Databento ohlcv-1m schema)
# ---------------------------------------------------------------------------
COL_TS: str = "ts_event"
COL_OPEN: str = "open"
COL_HIGH: str = "high"
COL_LOW: str = "low"
COL_CLOSE: str = "close"
COL_VOLUME: str = "volume"
COL_INSTRUMENT_ID: str = "instrument_id"
