"""
ledger.py
=========
Defines the Trade dataclass and the Ledger that accumulates completed
trades during the backtest run.

A Trade is created when a position is CLOSED. It records the full
round-trip: entry bar, exit bar, prices, costs, and net PnL.

PnL formula
-----------
    gross_pnl = direction × (exit_price - entry_price) × (TICK_VALUE / TICK_SIZE)
    total_cost = (commission_per_side × 2) + (slippage_ticks × 2 × TICK_VALUE)
    net_pnl    = gross_pnl - total_cost

Notes
-----
- Slippage is already baked into the fill prices (entry_price and
  exit_price are post-slippage). The formula therefore does NOT
  subtract slippage again — doing so would double-count it.
- total_cost here refers only to commission (both sides).
- The gross_pnl already reflects slippage-adjusted prices.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from config import constants as C
from config import settings as S
from execution.position_manager import Direction

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Trade:
    """
    Immutable record of a completed round-trip trade.

    Attributes
    ----------
    trade_id : int
        Sequential trade identifier.
    direction : Direction
        Direction of the trade (LONG or SHORT).
    entry_bar : pd.Timestamp
        Bar timestamp at which the position was opened (fill bar).
    exit_bar : pd.Timestamp
        Bar timestamp at which the position was closed (fill bar).
    entry_price : float
        Post-slippage fill price at entry.
    exit_price : float
        Post-slippage fill price at exit.
    gross_pnl : float
        PnL before commission.
    commission : float
        Total commission for both sides (USD).
    net_pnl : float
        Net PnL after commission.
    is_winner : bool
        True if net_pnl > 0.
    r_multiple : float
        Placeholder — not applicable in v1.2 (no fixed stop). Set to NaN.
    """
    trade_id: int
    direction: Direction
    entry_bar: pd.Timestamp
    exit_bar: pd.Timestamp
    entry_price: float
    exit_price: float
    gross_pnl: float
    commission: float
    net_pnl: float
    is_winner: bool
    r_multiple: float


def build_trade(
    trade_id: int,
    direction: Direction,
    entry_bar: pd.Timestamp,
    exit_bar: pd.Timestamp,
    entry_price: float,
    exit_price: float,
) -> Trade:
    """
    Construct a Trade record from raw fill data, computing PnL.

    The gross PnL uses the price ratio TickValue / TickSize which gives
    the USD value of a 1-unit price move for 1 contract.

    Parameters
    ----------
    trade_id, direction, entry_bar, exit_bar,
    entry_price, exit_price : see Trade docstring.

    Returns
    -------
    Trade
    """
    price_to_usd = C.TICK_VALUE / C.TICK_SIZE   # = 125,000 USD per price unit

    gross_pnl = direction.value * (exit_price - entry_price) * price_to_usd

    # Commission: both sides, one contract.
    commission = S.COMMISSION_PER_SIDE * 2

    net_pnl = gross_pnl - commission

    return Trade(
        trade_id=trade_id,
        direction=direction,
        entry_bar=entry_bar,
        exit_bar=exit_bar,
        entry_price=entry_price,
        exit_price=exit_price,
        gross_pnl=gross_pnl,
        commission=commission,
        net_pnl=net_pnl,
        is_winner=net_pnl > 0,
        r_multiple=float("nan"),
    )


@dataclass
class Ledger:
    """
    Accumulates Trade records during the backtest.

    Attributes
    ----------
    trades : list[Trade]
        Ordered list of completed trades.
    _next_id : int
        Auto-incremented trade counter.
    """
    trades: List[Trade] = field(default_factory=list)
    _next_id: int = field(default=1, init=False, repr=False)

    def record(
        self,
        direction: Direction,
        entry_bar: pd.Timestamp,
        exit_bar: pd.Timestamp,
        entry_price: float,
        exit_price: float,
    ) -> Trade:
        """
        Build and store a new Trade.

        Returns
        -------
        Trade
            The newly created trade record.
        """
        trade = build_trade(
            trade_id=self._next_id,
            direction=direction,
            entry_bar=entry_bar,
            exit_bar=exit_bar,
            entry_price=entry_price,
            exit_price=exit_price,
        )
        self.trades.append(trade)
        self._next_id += 1

        logger.debug(
            "Trade #%d recorded: %s | entry=%.5f exit=%.5f net_pnl=%.2f",
            trade.trade_id, trade.direction.name,
            trade.entry_price, trade.exit_price, trade.net_pnl,
        )
        return trade

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert the trade list to a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            One row per completed trade.
        """
        if not self.trades:
            return pd.DataFrame()

        records = [
            {
                "trade_id": t.trade_id,
                "direction": t.direction.name,
                "entry_bar": t.entry_bar,
                "exit_bar": t.exit_bar,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "gross_pnl": t.gross_pnl,
                "commission": t.commission,
                "net_pnl": t.net_pnl,
                "is_winner": t.is_winner,
            }
            for t in self.trades
        ]
        return pd.DataFrame(records).set_index("trade_id")
