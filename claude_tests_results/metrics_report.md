# SFFM v1.2 — Pipeline Audit Report

**Generated:** 2026-02-27 18:21 UTC  
**Dataset:** Synthetic 6E M1 — 2019-01-07 to 2024-01-01 (5 years)  
**Seed:** 42 (deterministic)  

---

## 1. Data Pipeline

| Metric | Value |
|--------|-------|
| M1 bars generated | 2,620,801 |
| M5 bars after resample | 502,321 |
| M1 roll bars | 20 |
| M5 roll bars (contains_roll) | 20 (0.004%) |
| M5 freeze bars | 260 (0.1%) |
| Roll propagation validated | ✓ |
| EMA max price gap at roll | 0.00612 |

---

## 2. Performance Summary

### Full Period

| Metric | Value |
|--------|-------|
| Total trades | 1802 |
| Win rate | 31.2% |
| Profit factor | 1.40 |
| Expectancy (USD/trade) | 134.51 |
| Net profit (USD) | 242383.75 |
| CAGR | N/A (equity starts at 0) |
| Sharpe ratio | 1.53 |
| Max drawdown USD | -27095.00 |
| Max drawdown % | -48.33% |

### In-Sample (IS 70%)

| Metric | Value |
|--------|-------|
| Total trades | 1086 |
| Win rate | 32.0% |
| Profit factor | 1.48 |
| Expectancy (USD/trade) | 162.08 |
| Net profit (USD) | 176020.00 |
| CAGR | N/A (equity starts at 0) |
| Sharpe ratio | 1.66 |
| Max drawdown USD | -10868.75 |
| Max drawdown % | -48.33% |

### Out-of-Sample (OOS 30%)

| Metric | Value |
|--------|-------|
| Total trades | 716 |
| Win rate | 29.9% |
| Profit factor | 1.27 |
| Expectancy (USD/trade) | 92.69 |
| Net profit (USD) | 66363.75 |
| CAGR | 15.91% |
| Sharpe ratio | 1.27 |
| Max drawdown USD | -27095.00 |
| Max drawdown % | -10.45% |

### Bootstrap — IS Period

| Metric | Value |
|--------|-------|
| Resamples | 1000 |
| Observed expectancy | 162.08 |
| 95% CI lower | 78.40 |
| 95% CI upper | 248.22 |

---

## 3. Roll Execution Policy

| Metric | Value |
|--------|-------|
| Total roll events | 20 |
| Roll-forced position closes | 10 |
| Signals discarded during freeze | 0 |
| ROLL_FREEZE_BARS_POST | 12 |
| ROLL_CLOSE_SLIPPAGE_TICKS | 2 |
| Avg trade duration — normal (bars) | 103.7 |
| Avg trade duration — roll close (bars) | 3793.5 |

---

## 4. Edge Case Validation

| Check | Result |
|-------|--------|
| EC1: Signals discarded during freeze | 0 signals suppressed |
| EC2: Roll closures ≤ roll events | 10 ≤ 20 ✓ |
| EC3: Roll M5 bars have valid OHLCV | 0 NaN bars ✓ |
| EC4: No entries during freeze window | ✓ |
| EC5: Reproducibility (2 identical runs) | ✓ PASS |

---

## 5. Risk Observations

- **10 rolls occurred while flat** (no forced close needed). System was already without position at those roll bars.
- IS/OOS profit factor ratio: 1.27/1.48 = 0.86 (benchmark: >0.5 acceptable).
- EMA contamination at rolls: max price gap is within normal intraday range. No artificial EMA spike detected.
- Roll policy correctly isolates forced closes in ledger via `exit_reason='roll'`. These can be excluded from signal-quality analysis without affecting PnL totals.

---

## 6. Production Readiness Assessment

**Roll & Freeze policy: ✓ CORRECTLY IMPLEMENTED**

All structural checks passed:
- `contains_roll` propagates from M1 → M5 via `.any()` semantic
- Forced closes execute at next bar open (same mechanic as signal closes)
- Freeze counter decrements correctly; no entries during freeze
- Signals during freeze are discarded, never accumulated
- `exit_reason` tag enables clean separation of roll vs signal trades
- System is fully reproducible: identical outputs on identical inputs
