"""
main.py
=======
End-to-end pipeline for SFFM v1.2 Benchmark.

Execution order
---------------
1. Load raw M1 data.
2. Detect and log contract rolls.
3. Resample M1 → M5.
4. Compute EMA pair.
5. Generate crossover signals.
6. Determine IS/OOS split boundary.
7. Run backtest over the full dataset.
8. Split trades and equity into IS and OOS.
9. Compute performance metrics for each period.
10. Run bootstrap on IS trades.
11. Print summary report to stdout.

Usage
-----
    python main.py

Environment variables required
-------------------------------
    DATABENTO_API_KEY : str
        Your Databento API key (required only if data is not cached).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging configuration — must be set before any module imports that log.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("sffm.main")


def run_pipeline() -> None:
    """Execute the complete SFFM v1.2 backtest pipeline."""

    from config import settings as S
    from config import constants as C

    from data.loader import load_raw_m1
    from data.roll_manager import detect_rolls, save_roll_log, annotate_rolls
    from preprocessing.resampler import resample_m1_to_m5
    from indicators.ema import compute_ema_pair
    from signals.crossover import generate_crossover_signals
    from backtest.engine import run_backtest
    from backtest.equity import build_equity_curve, split_equity
    from metrics.performance import compute_performance
    from metrics.drawdown import compute_drawdown
    from metrics.bootstrap import run_bootstrap

    S.DATA_DIR.mkdir(parents=True, exist_ok=True)
    S.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1 — Load raw M1 data.
    # ------------------------------------------------------------------
    logger.info("=== STEP 1: Loading raw M1 data ===")
    try:
        df_m1 = load_raw_m1()
    except FileNotFoundError:
        logger.error(
            "Raw data not found. Run downloader.download() first "
            "or set DATABENTO_API_KEY and call:\n"
            "    from data.downloader import download; download()"
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 2 — Detect and log contract rolls; annotate M1 with is_roll.
    # annotate_rolls must run BEFORE resampling so that the resampler
    # can propagate the is_roll flag into the M5 contains_roll column.
    # ------------------------------------------------------------------
    logger.info("=== STEP 2: Detecting contract rolls ===")
    rolls = detect_rolls(df_m1)
    if not rolls.empty:
        roll_log_path = S.OUTPUT_DIR / "roll_log.csv"
        save_roll_log(rolls, roll_log_path)
    else:
        logger.info("No roll events detected (instrument_id column absent or constant).")

    # Annotate M1 with is_roll flag for propagation into M5.
    df_m1 = annotate_rolls(df_m1)

    # ------------------------------------------------------------------
    # Step 3 — Resample M1 → M5.
    # The resampler propagates is_roll → contains_roll using .any().
    # ------------------------------------------------------------------
    logger.info("=== STEP 3: Resampling M1 → M5 ===")
    df_m5 = resample_m1_to_m5(df_m1)

    # ------------------------------------------------------------------
    # Step 4 — Compute EMA pair (full dataset, no split reset).
    # ------------------------------------------------------------------
    logger.info("=== STEP 4: Computing EMA indicators ===")
    ema_fast, ema_slow = compute_ema_pair(df_m5[C.COL_CLOSE])

    # ------------------------------------------------------------------
    # Step 5 — Generate crossover signals.
    # ------------------------------------------------------------------
    logger.info("=== STEP 5: Generating crossover signals ===")
    signals = generate_crossover_signals(ema_fast, ema_slow)

    # ------------------------------------------------------------------
    # Step 6 — IS/OOS split boundary.
    # The split is chronological: first IS_FRACTION of all M5 bars → IS.
    # ------------------------------------------------------------------
    logger.info("=== STEP 6: Computing IS/OOS split ===")
    split_idx = int(len(df_m5) * S.IS_FRACTION)
    split_ts: pd.Timestamp = df_m5.index[split_idx]
    logger.info(
        "IS: bars 0–%d  |  OOS: bars %d–%d  |  Split timestamp: %s",
        split_idx - 1, split_idx, len(df_m5) - 1, split_ts,
    )

    # ------------------------------------------------------------------
    # Step 7 — Run backtest (full dataset, IS+OOS in one pass).
    # ------------------------------------------------------------------
    logger.info("=== STEP 7: Running backtest ===")
    state = run_backtest(df_m5, signals)

    # ------------------------------------------------------------------
    # Step 8 — Build equity curve; split into IS and OOS.
    # ------------------------------------------------------------------
    logger.info("=== STEP 8: Building equity curve ===")
    trade_df = state.ledger.to_dataframe()
    equity_full = build_equity_curve(trade_df, df_m5.index)
    equity_is, equity_oos = split_equity(equity_full, split_ts)

    # Split trades by exit bar.
    trades_is = (
        trade_df[trade_df["exit_bar"] < split_ts]
        if not trade_df.empty else pd.DataFrame()
    )
    trades_oos = (
        trade_df[trade_df["exit_bar"] >= split_ts]
        if not trade_df.empty else pd.DataFrame()
    )

    # ------------------------------------------------------------------
    # Step 9 — Compute performance metrics.
    # ------------------------------------------------------------------
    logger.info("=== STEP 9: Computing performance metrics ===")

    results: dict[str, object] = {}

    for label, t_df, eq in [
        ("FULL",  trade_df,  equity_full),
        ("IS",    trades_is, equity_is),
        ("OOS",   trades_oos, equity_oos),
    ]:
        if t_df.empty:
            logger.warning("No trades in period: %s. Skipping metrics.", label)
            continue
        perf = compute_performance(t_df, eq)
        dd = compute_drawdown(eq)
        results[label] = {"perf": perf, "dd": dd, "trades": t_df}

    # ------------------------------------------------------------------
    # Step 10 — Bootstrap on IS trades.
    # ------------------------------------------------------------------
    logger.info("=== STEP 10: Running bootstrap (IS trades) ===")
    bootstrap_result = None
    if "IS" in results and not results["IS"]["trades"].empty:
        bootstrap_result = run_bootstrap(results["IS"]["trades"])

    # ------------------------------------------------------------------
    # Step 11 — Print summary report.
    # ------------------------------------------------------------------
    logger.info("=== STEP 11: Summary Report ===")
    _print_report(results, bootstrap_result, split_ts)

    # Save trade list.
    if not trade_df.empty:
        trade_path = S.OUTPUT_DIR / "trade_list.csv"
        trade_df.to_csv(trade_path)
        logger.info("Trade list saved to %s", trade_path)

    # Save equity curve.
    equity_path = S.OUTPUT_DIR / "equity_curve.csv"
    equity_full.to_csv(equity_path, header=["equity_usd"])
    logger.info("Equity curve saved to %s", equity_path)


def _print_report(
    results: dict,
    bootstrap_result,
    split_ts: pd.Timestamp,
) -> None:
    """Print a formatted summary report to stdout."""

    sep = "=" * 60

    print(f"\n{sep}")
    print("  SFFM v1.2 BACKTEST REPORT")
    print(f"  IS/OOS Split: {split_ts.date()}")
    print(sep)

    for label in ("FULL", "IS", "OOS"):
        if label not in results:
            print(f"\n  [{label}]  No trades.")
            continue

        perf = results[label]["perf"]
        dd = results[label]["dd"]

        print(f"\n  [{label}]")
        print(f"  {'Total trades':30s}: {perf.total_trades}")
        print(f"  {'Win rate':30s}: {perf.win_rate:.1%}")
        print(f"  {'Profit factor':30s}: {perf.profit_factor:.2f}")
        print(f"  {'Expectancy (USD/trade)':30s}: {perf.expectancy:,.2f}")
        print(f"  {'Net profit':30s}: {perf.net_profit:,.2f}")
        print(f"  {'CAGR':30s}: {perf.cagr:.2%}" if perf.cagr == perf.cagr
              else f"  {'CAGR':30s}: N/A (initial equity = 0)")
        print(f"  {'Sharpe ratio':30s}: {perf.sharpe_ratio:.2f}")
        print(f"  {'Max drawdown (USD)':30s}: {dd.max_drawdown_usd:,.2f}")
        print(f"  {'Max drawdown (%)':30s}: {dd.max_drawdown_pct:.2%}")

    if bootstrap_result is not None:
        print(f"\n  [BOOTSTRAP — IS]  ({bootstrap_result.n_resamples} resamples)")
        print(
            f"  {'Observed expectancy':30s}: "
            f"{bootstrap_result.observed_expectancy:,.2f}"
        )
        print(
            f"  {'95% CI':30s}: "
            f"[{bootstrap_result.ci_lower:,.2f}, "
            f"{bootstrap_result.ci_upper:,.2f}]"
        )

    print(f"\n{sep}\n")


if __name__ == "__main__":
    run_pipeline()
