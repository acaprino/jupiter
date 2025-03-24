import asyncio
import threading
from typing import Dict, Optional, Callable, Awaitable, Any
import datetime
import time

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
    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(self, config: ConfigReader):
        super().__init__(config)
        # Locks to protect shared resources
        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._init_lock: asyncio.Lock = asyncio.Lock()

        # Dictionary of observers: {symbol: {observer_id: MarketStateObserver}}
        self.observers: Dict[str, Dict[str, MarketStateObserver]] = {}

        self.config = config
        self.agent = "MarketStateManager"

        self._running: bool = False
        self._task: Optional[asyncio.Task] = None

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierMarketState':
        async with cls._lock:
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

        market_is_open = await Broker().with_context(f"{symbol}.*.*").is_market_open(symbol)
        current_timestamp = now_utc().timestamp()

        observer.market_open = market_is_open
        if market_is_open:
            observer.market_opened_time = current_timestamp
            observer.market_closed_time = None
        else:
            observer.market_closed_time = current_timestamp
            observer.market_opened_time = None

        await callback(
            symbol,
            market_is_open,
            observer.market_closed_time,
            observer.market_opened_time,
            True
        )

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

        async with self._init_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.info("Market state monitoring started")

    @exception_handler
    async def stop(self):
        """Stops the market state monitoring loop."""
        async with self._init_lock:
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

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                # Calculate the *absolute* timestamp for the next minute.
                now = now_utc()
                next_minute = (now + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
                sleep_until = time.mktime(next_minute.timetuple())

                # Create a deep copy of observers within the lock
                async with self._observers_lock:
                    observers_copy = {
                        symbol: {
                            observer_id: observer
                            for observer_id, observer in observers.items()
                        }
                        for symbol, observers in self.observers.items()
                    }

                # Process each symbol
                for symbol, observers in observers_copy.items():
                    try:
                        market_is_open = await Broker().with_context(f"{symbol}.*.*").is_market_open(symbol)
                        current_timestamp = now_utc().timestamp() # Use consistent now_utc

                        notification_tasks = []
                        for observer_id, observer in observers.items():
                            # Check if the market state has changed.
                            if observer.market_open != market_is_open:
                                if market_is_open:
                                    observer.market_opened_time = current_timestamp
                                    observer.market_closed_time = None
                                    self.info(f"Market for symbol {symbol} opened at {now.isoformat()}") # Use now for logging
                                else:
                                    observer.market_closed_time = current_timestamp
                                    observer.market_opened_time = None
                                    self.info(f"Market for symbol {symbol} closed at {now.isoformat()}")

                                observer.market_open = market_is_open
                                notification_tasks.append(
                                    observer.callback(
                                        symbol,
                                        market_is_open,
                                        observer.market_closed_time,
                                        observer.market_opened_time,
                                        False
                                    )
                                )

                        if notification_tasks:
                            await asyncio.gather(*notification_tasks, return_exceptions=True)
                            self.debug(f"Notified observers for symbol {symbol} market state change")

                    except Exception as e:
                        self.error(f"Error processing symbol {symbol}: {e}")

                # Calculate sleep duration and sleep until the next minute.
                current_loop_time = asyncio.get_event_loop().time()
                sleep_duration = sleep_until - current_loop_time
                if sleep_duration > 0:
                    await asyncio.sleep(sleep_duration)

            except Exception as e:
                self.error(f"Error in market state monitor loop: {e}")
                await asyncio.sleep(5)
