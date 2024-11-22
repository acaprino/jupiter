# strategies/base_strategy.py

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from dto.Position import Position
from misc_utils.enums import Timeframe


class TradingStrategy(ABC):
    """
    Abstract base class for trading strategies.
    """

    @abstractmethod
    async def start(self):
        """
        Method called to start the strategy engine.
        """
        pass

    @abstractmethod
    async def bootstrap(self):
        """
        Method called to initialize the strategy before processing the first candles.
        This method is run asynchronously after the strategy is started, so other callbacks can be called while the strategy is still initializing.
        """
        pass

    @abstractmethod
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        """
        Method called when a new tick is available.

        Args:
            timeframe (Timeframe): The timeframe of the tick.
            timestamp (datetime): The timestamp of the tick.
        """
        pass

    @abstractmethod
    async def on_market_status_change(self, is_open: bool, closing_time: Optional[float], opening_time: Optional[float], initializing: Optional[bool]):
        """
        Method called when the market status changes.

        Args:
            is_open (bool): Whether the market is open.
            closing_time (Optional[float]): The closing time of the market.
            opening_time (Optional[float]): The opening time of the market.
            initializing (Optional[bool]): Whether the market is initializing.
        """
        pass

    @abstractmethod
    async def on_economic_event(self, event_info: dict):
        """
        Method called when an economic event occurs.

        Args:
            event_info (dict): Information about the economic event.
        """
        pass  # strategies/base_strategy.py

    @abstractmethod
    async def shutdown(self):
        """
        Method called to shutdown the strategy.
        """
        pass
