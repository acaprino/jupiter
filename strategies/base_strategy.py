# strategies/base_strategy.py

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from misc_utils.enums import Timeframe


class SignalGeneratorAgent(ABC):
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
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: Optional[float], opening_time: Optional[float], initializing: Optional[bool]):
        """
        :param symbol: A string representing the market identifier, such as the stock
            exchange or commodity symbol.
        :param is_open: A boolean indicating whether the market is open (True) or
            closed (False).
        :param closing_time: The time when the market is scheduled to close. It is
            optional and may be None if not applicable.
        :param opening_time: The time when the market is scheduled to open. It is
            optional and may be None if not applicable.
        :param initializing: An optional boolean indicating whether the status update
            is part of an ongoing initialization process. May be None if not
            applicable.
        :return: This method does not return a value as it is a coroutine awaiting
            implementation by the subclass.
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
        Method called to shut down the strategy.
        """
        pass
