import asyncio
import time
from typing import Dict, Optional, Callable, Awaitable

from brokers.broker_proxy import Broker
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[str, bool, Optional[float], Optional[float], bool], Awaitable[None]]


class MarketStateObserver:
    """Represents an observer for a symbol's market state."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback
        self.market_open: Optional[bool] = None  # Last known state
        self.market_closed_time: Optional[float] = None
        self.market_opened_time: Optional[float] = None


class NotifierMarketState(LoggingMixin):
    """Singleton class that manages market state monitoring for different symbols."""

    _instance: Optional['NotifierMarketState'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader):
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self.config = config

        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._start_lock: asyncio.Lock = asyncio.Lock()

        self.observers: Dict[str, Dict[str, MarketStateObserver]] = {}

        self.agent = "MarketStateManager"
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._polling_interval: float = 60.0

        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierMarketState':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierMarketState(config)
            return cls._instance

    @exception_handler
    async def register_observer(self, symbol: str, callback: ObserverCallback, observer_id: str):
        """Registers an observer for a symbol's market state."""

        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            observer = MarketStateObserver(callback)
            self.observers[symbol][observer_id] = observer
            self.info(f"Registered observer {observer_id} for symbol {symbol}")

        # Initial state check and notification
        await self._update_and_notify_single_symbol(symbol, observer_id, observer)
        await self.start()

    @exception_handler
    async def unregister_observer(self, symbol: str, observer_id: str):
        """Removes an observer for a symbol."""

        async with self._observers_lock:
            if symbol in self.observers and observer_id in self.observers[symbol]:
                del self.observers[symbol][observer_id]
                self.info(f"Unregistered observer {observer_id} for symbol {symbol}")

                if not self.observers[symbol]:
                    del self.observers[symbol]
                    self.info(f"Removed monitoring for symbol {symbol}")

    @exception_handler
    async def start(self):
        """Starts the market state monitoring loop (idempotent)."""

        async with self._start_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.info("Market state monitoring started")

    @exception_handler
    async def stop(self):
        """Stops the market state monitoring loop."""
        async with self._start_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self.info("Market state monitoring stopped")
                    self._task = None

    async def shutdown(self):
        """Stops the monitoring and clears resources."""
        await self.stop()
        async with self._observers_lock:
            self.observers.clear()
            self.info("NotifierMarketState shutdown complete.")

    async def _get_observers_copy(self) -> Dict[str, Dict[str, MarketStateObserver]]:
        """Creates a deep copy of the observers dictionary."""
        async with self._observers_lock:
            return {
                symbol: {
                    observer_id: observer
                    for observer_id, observer in observers.items()
                }
                for symbol, observers in self.observers.items()
            }

    async def _update_and_notify_single_symbol(self, symbol: str, observer_id: str, observer: MarketStateObserver):
        """Checks and updates the market state for a single symbol and notifies the observer."""

        try:
            market_is_open = await Broker().with_context(f"{symbol}").is_market_open(symbol)
            current_timestamp = now_utc().timestamp()

            if observer.market_open != market_is_open:
                if market_is_open:
                    observer.market_opened_time = current_timestamp
                    observer.market_closed_time = None
                    self.info(f"Market for symbol {symbol} opened")
                else:
                    observer.market_closed_time = current_timestamp
                    observer.market_opened_time = None
                    self.info(f"Market for symbol {symbol} closed")

                observer.market_open = market_is_open
                await observer.callback(
                    symbol,
                    market_is_open,
                    observer.market_closed_time,
                    observer.market_opened_time,
                    False  # Indicate this is not the initial state
                )
        except Exception as e:
            self.error(f"Error processing symbol {symbol}", exec_info=e)

    async def _notify_observers(self, symbol: str, observers: Dict[str, MarketStateObserver]):
        """Notifies observers for a given symbol about market state changes."""
        notification_tasks = []
        for observer_id, observer in observers.items():
            notification_tasks.append(
                self._update_and_notify_single_symbol(symbol, observer_id, observer)
            )
        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)  # Handle exceptions from callbacks
            self.debug(f"Notified observers for symbol {symbol}")

    async def _calculate_sleep_time(self) -> float:
        current_ts = now_utc().timestamp()
        interval = self._polling_interval

        if interval <= 0:
            raise ValueError("Polling interval must be positive")

        next_check_ts = ((current_ts // interval) + 1) * interval
        return next_check_ts - current_ts

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                start_time = time.monotonic()

                observers_copy = await self._get_observers_copy()

                for symbol, observers in observers_copy.items():
                    await self._notify_observers(symbol, observers)

                # If sleet time is greater than interval
                elapsed = time.monotonic() - start_time
                sleep_duration = max(0.0, await self._calculate_sleep_time() - elapsed)
                await asyncio.sleep(sleep_duration)


            except Exception as e:
                self.error(f"Error in market state monitor loop", exec_info=e)
                await asyncio.sleep(5)  # Fallback sleep on unhandled error
