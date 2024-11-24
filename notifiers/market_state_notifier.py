import asyncio
import time
from datetime import datetime
from typing import Callable, Awaitable, List, Optional

from brokers.broker_interface import BrokerAPI
from misc_utils.error_handler import exception_handler
from misc_utils.bot_logger import BotLogger


class MarketStateNotifier:
    """
    Monitors and notifies registered callbacks of changes in the market's open/closed state for a specific symbol.
    """

    def __init__(self, routine_label: str, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.routine_label = routine_label
        self.logger = BotLogger.get_logger(routine_label)
        self.broker = broker
        self.symbol = symbol
        self.execution_lock = execution_lock
        self.check_interval_minutes = 1
        self._market_open: Optional[bool] = None
        self._running = False
        self._task = None
        self._initialized = False
        self._market_closed_time: Optional[float] = None
        self._market_opened_time: Optional[float] = None
        self._on_market_status_change_callbacks: List[Callable[[bool, Optional[float], Optional[float], Optional[bool]], Awaitable[None]]] = []

    @exception_handler
    async def start(self):
        """Starts the market state monitoring loop."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            self.logger.info(f"MarketStateNotifier started for symbol: {self.symbol}")

    @exception_handler
    async def stop(self):
        """Stops the market state monitoring loop."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info(f"MarketStateNotifier stopped for symbol: {self.symbol}")

    def register_on_market_status_change(
            self,
            callback: Callable[[bool, Optional[float], Optional[float], Optional[bool]], Awaitable[None]]
    ):
        """Registers a callback to notify when the market status changes."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_market_status_change_callbacks.append(callback)
        self.logger.info("Callback registered for market status changes.")

    @exception_handler
    async def _update_market_state(self, market_is_open: bool, initializing: bool = False):
        """Updates the current market state and notifies registered callbacks."""
        current_time = time.time()

        # Update open/closed timestamps
        if market_is_open:
            self._market_opened_time = current_time
            self._market_closed_time = None
        else:
            self._market_closed_time = current_time
            self._market_opened_time = None

        self._market_open = market_is_open

        # Notify registered callbacks of the new market state
        tasks = [
            callback(self._market_open, self._market_closed_time, self._market_opened_time, initializing)
            for callback in self._on_market_status_change_callbacks
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.info(f"Market state updated to {'open' if market_is_open else 'closed'} for {self.symbol}.")

    @exception_handler
    async def _run(self):
        """Continuously checks the market state at specified interval in minutes."""
        while self._running:
            try:
                # Call broker to check if the market is open
                market_is_open = await self.broker.is_market_open(self.symbol)

                # Initial state check or state change detection
                if not self._initialized:
                    await self._update_market_state(market_is_open, initializing=True)
                    self._initialized = True
                elif market_is_open != self._market_open:
                    await self._update_market_state(market_is_open, initializing=False)

                # Calculate the time until the next interval in minutes
                now = datetime.now()
                seconds_until_next_interval = (self.check_interval_minutes - now.minute % self.check_interval_minutes) * 60 - now.second

                # self.logger.debug(f"Market is {'open' if market_is_open else 'closed'} for {self.symbol}. Next check in {seconds_until_next_interval} seconds.")
                await asyncio.sleep(seconds_until_next_interval)
            except Exception as e:
                self.logger.error(f"Error in MarketStateNotifier._run: {e}")
                await asyncio.sleep(5)  # Sleep before retrying after an error
