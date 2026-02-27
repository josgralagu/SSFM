# SFFM v1.2 — Benchmark Backtesting System

**Spec Version:** 1.2 (Frozen)  
**Instrument:** 6E Euro FX Futures (CME)  
**Strategy:** EMA(20) / EMA(50) crossover on M5 bars  
**Data Source:** Databento — GLBX.MDP3, `ohlcv-1m`, continuous contract `.c.0`

---

## Project Structure

```
sffm_v1_2/
│
├── config/
│   ├── constants.py        # Immutable instrument/market constants
│   └── settings.py         # All tunable parameters (single source of truth)
│
├── data/
│   ├── downloader.py       # Fetches raw M1 data from Databento
│   ├── roll_manager.py     # Detects and logs contract roll events
│   └── loader.py           # Loads raw Parquet; validates schema
│
├── preprocessing/
│   └── resampler.py        # M1 → M5 with session-gap handling
│
├── indicators/
│   └── ema.py              # EMA calculation (standard alpha, warmup enforced)
│
├── signals/
│   └── crossover.py        # EMA crossover signal generation
│
├── execution/
│   ├── position_manager.py # Position state + action determination
│   └── execution_engine.py # Fill price calculation with slippage
│
├── backtest/
│   ├── engine.py           # Bar-by-bar loop; anti-lookahead enforced
│   ├── ledger.py           # Immutable trade records + PnL formula
│   └── equity.py           # Equity curve construction
│
├── metrics/
│   ├── performance.py      # CAGR, Sharpe, win rate, expectancy, PF
│   ├── drawdown.py         # Max drawdown USD and %
│   └── bootstrap.py        # Trade-level bootstrap CI for expectancy
│
├── main.py                 # End-to-end pipeline entry point
└── README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install pandas numpy pyarrow databento
```

### 2. Set your Databento API key

```bash
export DATABENTO_API_KEY="your_key_here"
```

### 3. Download data (first run only)

```python
from data.downloader import download
download()
```

### 4. Run the full pipeline

```bash
python main.py
```

---

## Configuration

All parameters are in `config/settings.py`:

| Parameter             | Default              | Description                          |
|-----------------------|----------------------|--------------------------------------|
| `DATA_START`          | `"2019-01-01..."`    | Backtest start date                  |
| `DATA_END`            | `"2024-01-01..."`    | Backtest end date                    |
| `EMA_FAST`            | `20`                 | Fast EMA period                      |
| `EMA_SLOW`            | `50`                 | Slow EMA period                      |
| `WARMUP_BARS`         | `200`                | Bars before first signal             |
| `IS_FRACTION`         | `0.70`               | Proportion of data used for IS       |
| `SLIPPAGE_TICKS`      | `1`                  | Adverse ticks per fill               |
| `COMMISSION_PER_SIDE` | `2.50`               | USD per contract per side            |
| `BOOTSTRAP_RESAMPLES` | `1000`               | Bootstrap iterations                 |

---

## Key Design Decisions

**Anti-lookahead:** Signal at bar[i] uses only data ≤ bar[i]. Execution
fills at bar[i+1].open. The backtest loop never reads bar[i+1] during
signal evaluation.

**IS/OOS EMA state continuity:** EMAs are computed once over the full
dataset before the loop. The IS/OOS boundary does not reset indicator
state, ensuring the first OOS bar sees the same EMA values as a live
system would.

**Session gap handling:** M1 bars in the CME daily break (17:00–17:59 CT)
are excluded before resampling. The timezone conversion UTC→Chicago/CT
happens before the filter to handle DST correctly.

**PnL formula:**
```
gross_pnl = direction × (exit_fill - entry_fill) × (TICK_VALUE / TICK_SIZE)
net_pnl   = gross_pnl - (COMMISSION_PER_SIDE × 2)
```
Slippage is baked into fill prices; it is NOT subtracted again in the
PnL formula.

---

## Outputs

After a successful run, the following files are written to `output/`:

| File                | Contents                              |
|---------------------|---------------------------------------|
| `trade_list.csv`    | Every completed round-trip trade      |
| `equity_curve.csv`  | Bar-resolution cumulative PnL         |
| `roll_log.csv`      | Contract roll events (if detected)    |

The dataset manifest (`dataset_manifest.json`) is written to `data_cache/`
alongside the raw Parquet file and includes the SHA-256 hash for
reproducibility auditing.

---

## Reproducibility

To reproduce a run exactly:
1. Use the same `dataset_manifest.json` to verify the raw data file hash.
2. Use the same `requirements_snapshot.txt` (generate with `pip freeze`).
3. Run `python main.py` with unchanged `config/settings.py`.

The same inputs will always produce the same outputs.
