import asyncio
import threading
from typing import Dict, Optional, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[bool, Optional[float], Optional[float], bool], Awaitable[None]]


class MarketStateObserver:
    """Represents an observer for a symbol's market state."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback
        self.market_open: Optional[bool] = None  # Last known state
        self.market_closed_time: Optional[float] = None
        self.market_opened_time: Optional[float] = None


class MarketStateManager:
    """Singleton class that manages market state monitoring for different symbols."""

    _instance: Optional['MarketStateManager'] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'MarketStateManager':
        with cls._instance_lock:
            if cls._instance is None:
                instance = super(MarketStateManager, cls).__new__(cls)
                instance.__initialized = False
                cls._instance = instance
            return cls._instance

    def __init__(self):
        if not getattr(self, '__initialized', False):
            # Locks to protect shared resources
            self._observers_lock: asyncio.Lock = asyncio.Lock()
            self._state_lock: asyncio.Lock = asyncio.Lock()

            # Dictionary of observers: {symbol: {observer_id: MarketStateObserver}}
            self.observers: Dict[str, Dict[str, MarketStateObserver]] = {}

            self.logger = BotLogger.get_logger("MarketStateManager")

            self._running: bool = False
            self._task: Optional[asyncio.Task] = None
            self.broker: Optional[BrokerAPI] = None
            self.check_interval_seconds = 60  # Check every minute

            self.__initialized = True

    @exception_handler
    async def register_observer(self,
                                symbol: str,
                                broker: BrokerAPI,
                                callback: ObserverCallback,
                                observer_id: str):
        """Registers a new observer for a symbol."""
        start_needed = False

        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}

            observer = MarketStateObserver(callback)
            self.observers[symbol][observer_id] = observer

            self.logger.info(f"Registered observer {observer_id} for symbol {symbol}")

            async with self._state_lock:
                if not self.broker:
                    self.broker = broker
                if not self._running:
                    self._running = True
                    start_needed = True

            if start_needed:
                await self.start()

        # Notify the observer with the current state if available
        market_is_open = await self.broker.is_market_open(symbol)
        current_timestamp = now_utc().timestamp()

        observer.market_open = market_is_open
        if market_is_open:
            observer.market_opened_time = current_timestamp
            observer.market_closed_time = None
        else:
            observer.market_closed_time = current_timestamp
            observer.market_opened_time = None

        await callback(
            market_is_open,
            observer.market_closed_time,
            observer.market_opened_time,
            True
        )

    @exception_handler
    async def unregister_observer(self, symbol: str, observer_id: str):
        """Removes an observer for a symbol."""
        stop_needed = False

        async with self._observers_lock:
            if symbol in self.observers:
                if observer_id in self.observers[symbol]:
                    del self.observers[symbol][observer_id]
                    self.logger.info(f"Unregistered observer {observer_id} for symbol {symbol}")

                # Remove the symbol entry if no observers left
                if not self.observers[symbol]:
                    del self.observers[symbol]
                    self.logger.info(f"Removed monitoring for symbol {symbol}")

        async with self._state_lock:
            if not any(self.observers.values()) and self._running:
                stop_needed = True
        if stop_needed:
            await self.stop()

    async def start(self):
        """Starts the market state monitoring loop."""
        async with self._state_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.logger.info("Market state monitoring started")

    async def stop(self):
        """Stops the market state monitoring loop."""
        async with self._state_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info("Market state monitoring stopped")
                self._task = None

    async def shutdown(self):
        """Stops the monitoring and clears resources."""
        async with self._state_lock:
            await self.stop()
        async with self._observers_lock:
            self.observers.clear()

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while True:
            try:
                async with self._state_lock:
                    if not self._running:
                        break

                current_time = now_utc()

                # Create a safe copy of observers
                async with self._observers_lock:
                    observers_copy = {symbol: observers.copy() for symbol, observers in self.observers.items()}

                # For each symbol
                for symbol, observers in observers_copy.items():
                    try:
                        market_is_open = await self.broker.is_market_open(symbol)
                        current_timestamp = current_time.timestamp()

                        # For each observer of the symbol
                        notification_tasks = []
                        for observer_id, observer in observers.items():
                            # Check if market state has changed or if this is the first check
                            if observer.market_open != market_is_open or observer.market_open is None:
                                if market_is_open:
                                    observer.market_opened_time = current_timestamp
                                    observer.market_closed_time = None
                                else:
                                    observer.market_closed_time = current_timestamp
                                    observer.market_opened_time = None

                                observer.market_open = market_is_open

                                # Prepare the callback
                                notification_tasks.append(
                                    observer.callback(
                                        market_is_open,
                                        observer.market_closed_time,
                                        observer.market_opened_time,
                                        False
                                    )
                                )

                        # Notify observers
                        if notification_tasks:
                            await asyncio.gather(*notification_tasks, return_exceptions=True)
                            self.logger.debug(
                                f"Notified observers for symbol {symbol} market state change"
                            )

                    except Exception as e:
                        self.logger.error(f"Error processing symbol {symbol}: {e}")

                # Sleep until next check
                await asyncio.sleep(self.check_interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in market state monitor loop: {e}")
                await asyncio.sleep(5)
