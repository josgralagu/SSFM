"""
position_manager.py
===================
Tracks the current position state during the backtest loop.

Responsibilities
----------------
- Know whether we are flat, long, or short.
- Determine whether a new signal requires an entry, a reversal,
  or no action (same direction as current position).
- Produce an action descriptor that the execution engine acts on.

This module does NOT compute prices, apply slippage, or record PnL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class Direction(IntEnum):
    """Represents the direction of a position or trade."""
    FLAT = 0
    LONG = 1
    SHORT = -1


@dataclass
class Action:
    """
    Describes the trading action required for a given bar.

    Attributes
    ----------
    close_existing : bool
        True if the current position must be closed.
    open_new : bool
        True if a new position must be opened.
    new_direction : Direction
        Direction of the new position (only relevant if open_new is True).
    """
    close_existing: bool = False
    open_new: bool = False
    new_direction: Direction = Direction.FLAT


@dataclass
class PositionManager:
    """
    Maintains the current open position state.

    Attributes
    ----------
    current_direction : Direction
        Current position direction (FLAT, LONG, or SHORT).
    entry_bar_index : int
        Bar index at which the current position was opened (-1 if flat).
    """
    current_direction: Direction = field(default=Direction.FLAT)
    entry_bar_index: int = field(default=-1)

    def is_flat(self) -> bool:
        """Return True if there is no open position."""
        return self.current_direction == Direction.FLAT

    def evaluate_signal(self, signal: int, bar_index: int) -> Action:
        """
        Evaluate a signal and return the required action.

        Parameters
        ----------
        signal : int
            Signal value: +1 (long), -1 (short), 0 (no signal).
        bar_index : int
            Current bar index (used for logging / audit).

        Returns
        -------
        Action
            Descriptor of what the execution engine should do.
        """
        if signal == 0:
            return Action()  # nothing to do

        desired = Direction(signal)

        if self.current_direction == desired:
            # Already in the desired direction — no action.
            return Action()

        if self.is_flat():
            # Flat → open new position.
            return Action(close_existing=False, open_new=True, new_direction=desired)

        # Existing position in opposite direction → reversal.
        return Action(close_existing=True, open_new=True, new_direction=desired)

    def on_open(self, direction: Direction, bar_index: int) -> None:
        """
        Update state after opening a new position.

        Parameters
        ----------
        direction : Direction
            Direction of the newly opened position.
        bar_index : int
            Bar index at which the position was opened.
        """
        self.current_direction = direction
        self.entry_bar_index = bar_index

    def on_close(self) -> None:
        """Reset state after closing the current position."""
        self.current_direction = Direction.FLAT
        self.entry_bar_index = -1
