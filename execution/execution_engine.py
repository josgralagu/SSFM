"""
execution_engine.py
===================
Computes the actual fill price for a given order, applying slippage
in the adverse direction.

Responsibilities
----------------
- Receive an order (direction, bar open price).
- Apply adverse slippage (normal or roll-close rate).
- Return the fill price.

This module does NOT track position state and does NOT compute PnL.
"""

from __future__ import annotations

import logging

from config import constants as C
from config import settings as S
from execution.position_manager import Direction

logger = logging.getLogger(__name__)


def compute_fill_price(
    direction: Direction,
    bar_open: float,
    is_roll_close: bool = False,
) -> float:
    """
    Compute the fill price for a market order at bar open.

    Slippage is applied adversely:
      - Long  entry  → fill = open + (slippage_ticks × TICK_SIZE)
      - Short entry  → fill = open - (slippage_ticks × TICK_SIZE)

    For exits (closing an existing position) the direction passed is the
    direction of the CLOSING order (opposite to the position being closed),
    so adverse slippage still applies correctly:
      - Closing a Long  → sell order → direction = SHORT
                          fill = open - slippage  (worse for seller)
      - Closing a Short → buy  order → direction = LONG
                          fill = open + slippage  (worse for buyer)

    Parameters
    ----------
    direction : Direction
        Direction of this particular fill (LONG buy or SHORT sell).
    bar_open : float
        Open price of the execution bar.
    is_roll_close : bool, optional
        If True, use ROLL_CLOSE_SLIPPAGE_TICKS instead of SLIPPAGE_TICKS.
        Set to True only for forced position closes triggered by a roll event.
        Default: False.

    Returns
    -------
    float
        Fill price after slippage.

    Raises
    ------
    ValueError
        If direction is FLAT (cannot fill a flat order).
    """
    if direction == Direction.FLAT:
        raise ValueError("Cannot compute fill price for Direction.FLAT.")

    ticks = S.ROLL_CLOSE_SLIPPAGE_TICKS if is_roll_close else S.SLIPPAGE_TICKS
    slippage_amount = ticks * C.TICK_SIZE

    if direction == Direction.LONG:
        fill = bar_open + slippage_amount
    else:  # SHORT
        fill = bar_open - slippage_amount

    logger.debug(
        "Fill: direction=%s  open=%.5f  slippage_ticks=%d  fill=%.5f%s",
        direction.name, bar_open, ticks, fill,
        "  [ROLL CLOSE]" if is_roll_close else "",
    )
    return fill
    return fill


def round_to_tick(price: float) -> float:
    """
    Round a price to the nearest valid tick.

    For the 6E futures contract with TICK_SIZE = 0.00005, this ensures
    that fill prices do not contain sub-tick precision artefacts.

    Parameters
    ----------
    price : float
        Raw price value.

    Returns
    -------
    float
        Price rounded to the nearest TICK_SIZE.
    """
    tick = C.TICK_SIZE
    return round(round(price / tick) * tick, 10)
