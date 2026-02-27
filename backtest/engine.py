"""
engine.py
=========
The core backtest engine. Iterates over M5 bars in chronological order,
evaluating signals at bar close and executing orders at the next bar open.

Anti-lookahead guarantee
------------------------
At bar[i] (signal evaluation):
  - Only data up to and including bar[i] is visible.
  - EMA values at bar[i] are computed from close[0..i] — no future data.

At bar[i+1] (execution):
  - The fill price is bar[i+1].open ± slippage.
  - Only bar[i+1].open is used; high/low/close of bar[i+1] are NOT read
    during execution.

IS/OOS EMA state continuity
----------------------------
EMAs are computed once over the full dataset BEFORE the loop begins.
The split boundary does NOT reset indicator state. This satisfies
the spec requirement that "EMAs must preserve state across split."
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from backtest.ledger import Ledger
from config import constants as C
from config import settings as S
from execution.execution_engine import compute_fill_price, round_to_tick
from execution.position_manager import Direction, PositionManager

logger = logging.getLogger(__name__)


@dataclass
class PendingOrder:
    """
    Represents an order queued for execution at the next bar open.

    Attributes
    ----------
    close_direction : Direction or None
        If set, close the existing position in this direction.
    open_direction : Direction or None
        If set, open a new position in this direction.
    signal_bar : pd.Timestamp
        The bar at which the signal was generated (for audit).
    exit_reason : str
        Reason for the close. "signal" for normal crossover exits,
        "roll" for forced closes triggered by a contract roll event.
    """
    close_direction: Optional[Direction] = None
    open_direction: Optional[Direction] = None
    signal_bar: Optional[pd.Timestamp] = None
    exit_reason: str = "signal"


@dataclass
class BacktestState:
    """
    Mutable state carried through the bar loop.

    Attributes
    ----------
    position : PositionManager
    ledger : Ledger
    pending : PendingOrder or None
    entry_price : float or None
        Post-slippage entry price of the current open trade.
    entry_bar : pd.Timestamp or None
        Bar at which the current trade was opened.
    roll_freeze_remaining : int
        Number of M5 bars remaining in the post-roll freeze window.
        Zero means no freeze is active. Decremented each bar during freeze.
        Signal evaluation is skipped while this is > 0.
    """
    position: PositionManager = field(default_factory=PositionManager)
    ledger: Ledger = field(default_factory=Ledger)
    pending: Optional[PendingOrder] = None
    entry_price: Optional[float] = None
    entry_bar: Optional[pd.Timestamp] = None
    roll_freeze_remaining: int = 0


def run_backtest(
    df_m5: pd.DataFrame,
    signals: pd.Series,
) -> BacktestState:
    """
    Execute the SFFM v1.2 backtest over a prepared M5 DataFrame.

    Parameters
    ----------
    df_m5 : pd.DataFrame
        M5 OHLCV DataFrame. Index is a DatetimeIndex (UTC).
        Required columns: open, high, low, close.
    signals : pd.Series
        Signal series aligned to df_m5.index.
        Values: +1 (long), -1 (short), 0 (no signal).

    Returns
    -------
    BacktestState
        State object containing the ledger (all completed trades)
        and the final position manager state.
    """
    state = BacktestState()
    bars = df_m5.reset_index()   # numeric indexing is simpler in the loop

    n_bars = len(bars)
    logger.info("Starting backtest over %d M5 bars.", n_bars)

    for i in range(n_bars):
        bar = bars.iloc[i]
        bar_ts: pd.Timestamp = bar[C.COL_TS]
        bar_open: float = bar[C.COL_OPEN]
        is_roll_bar: bool = bool(bar.get("contains_roll", False))

        # ---------------------------------------------------------------
        # STEP 1 — EXECUTE pending order from the previous bar's signal.
        # Runs unconditionally: even during a freeze or on a roll bar, a
        # pending close from the prior bar must be filled at this open.
        # Skipping this step would leave the position in an invalid state.
        # ---------------------------------------------------------------
        if state.pending is not None:
            _execute_pending(state, bar_ts, bar_open)

        # ---------------------------------------------------------------
        # STEP 2 — ROLL BAR: force-close open position, activate freeze.
        #
        # A roll bar is one whose M5 block contains at least one M1 bar
        # where instrument_id changed. No new positions are opened here.
        # The close is registered as a PendingOrder so it executes at
        # bar[i+1].open — identical mechanics to a normal signal close.
        #
        # Why not open_direction here: opening on a roll bar means
        # entering at a price that straddles two contracts. Prohibited.
        # ---------------------------------------------------------------
        if is_roll_bar:
            if not state.position.is_flat():
                state.pending = PendingOrder(
                    close_direction=Direction(state.position.current_direction),
                    open_direction=None,  # never open on a roll bar
                    signal_bar=bar_ts,
                    exit_reason="roll",
                )
            # Activate freeze regardless of whether a position was open.
            state.roll_freeze_remaining = S.ROLL_FREEZE_BARS_POST
            # Do NOT evaluate signals on this bar. Signal evaluation skipped.
            continue

        # ---------------------------------------------------------------
        # STEP 3 — FREEZE WINDOW: skip signal evaluation, decrement counter.
        #
        # During the freeze, pending orders from STEP 1 still execute
        # (handled above). Only signal generation is suppressed.
        # Signals are discarded, not accumulated — no deferred execution.
        # ---------------------------------------------------------------
        if state.roll_freeze_remaining > 0:
            state.roll_freeze_remaining -= 1
            continue

        # ---------------------------------------------------------------
        # STEP 4 — NORMAL SIGNAL EVALUATION (unchanged from original).
        # Reached only when: not a roll bar, not in freeze window.
        # ---------------------------------------------------------------
        signal_value = int(signals.iloc[i])
        action = state.position.evaluate_signal(signal_value, i)

        if action.close_existing or action.open_new:
            state.pending = PendingOrder(
                close_direction=(
                    Direction(state.position.current_direction)
                    if action.close_existing else None
                ),
                open_direction=(
                    action.new_direction if action.open_new else None
                ),
                signal_bar=bar_ts,
                exit_reason="signal",
            )
        else:
            # No signal — clear any stale pending state.
            state.pending = None

    # Close any open position at the end of the dataset using the last bar.
    _force_close_at_end(state, bars)

    logger.info(
        "Backtest complete. %d trades recorded.", len(state.ledger.trades)
    )
    return state


def _execute_pending(
    state: BacktestState,
    bar_ts: pd.Timestamp,
    bar_open: float,
) -> None:
    """
    Execute the pending order at bar_open.

    Handles close-only, open-only, and close+open (reversal) scenarios.
    """
    pending = state.pending
    assert pending is not None

    # --- Close existing position ---
    if pending.close_direction is not None:
        closing_order_dir = Direction(-pending.close_direction.value)
        exit_price = round_to_tick(
            compute_fill_price(
                closing_order_dir,
                bar_open,
                is_roll_close=(pending.exit_reason == "roll"),
            )
        )

        assert state.entry_price is not None
        assert state.entry_bar is not None

        state.ledger.record(
            direction=pending.close_direction,
            entry_bar=state.entry_bar,
            exit_bar=bar_ts,
            entry_price=state.entry_price,
            exit_price=exit_price,
        )
        state.position.on_close()
        state.entry_price = None
        state.entry_bar = None

    # --- Open new position ---
    if pending.open_direction is not None:
        entry_price = round_to_tick(
            compute_fill_price(pending.open_direction, bar_open)
        )
        state.position.on_open(pending.open_direction, bar_index=-1)
        state.entry_price = entry_price
        state.entry_bar = bar_ts

    state.pending = None


def _force_close_at_end(state: BacktestState, bars: pd.DataFrame) -> None:
    """
    Force-close any open position at the final bar's open price.
    This ensures the ledger is complete at backtest end.
    """
    if state.position.is_flat():
        return

    last_bar = bars.iloc[-1]
    bar_ts: pd.Timestamp = last_bar[C.COL_TS]
    bar_open: float = last_bar[C.COL_OPEN]

    closing_dir = Direction(-state.position.current_direction.value)
    exit_price = round_to_tick(compute_fill_price(closing_dir, bar_open))

    assert state.entry_price is not None
    assert state.entry_bar is not None

    state.ledger.record(
        direction=state.position.current_direction,
        entry_bar=state.entry_bar,
        exit_bar=bar_ts,
        entry_price=state.entry_price,
        exit_price=exit_price,
    )
    state.position.on_close()
    logger.info("Force-closed open position at end of data.")
