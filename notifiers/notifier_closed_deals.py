import asyncio
import threading
from typing import Dict, List, Optional, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from brokers.broker_proxy import Broker
from misc_utils.logger_mixing import LoggingMixin
from dto.Position import Position
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[Position], Awaitable[None]]


class SymbolDealsObserver:
    """Represents an observer for closed positions of a symbol."""

    def __init__(self, symbol: str, magic_number: int, callback: ObserverCallback):
        self.symbol: str = symbol
        self.magic_number: int = magic_number
        self.callback: ObserverCallback = callback


class ClosedDealsNotifier(LoggingMixin):
    """Thread-safe manager for monitoring closed positions."""

    _instance: Optional['ClosedDealsNotifier'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader) -> None:
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._start_lock: asyncio.Lock = asyncio.Lock()  # Separate lock for start/stop

        self.observers: Dict[str, Dict[int, Dict[str, SymbolDealsObserver]]] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

        self.config = config
        self.agent = "ClosedDealsManager"

        self.interval_seconds: float = 60.0
        self._min_sleep_time: float = 1.0
        self._running = False  # Add _running flag
        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'ClosedDealsNotifier':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = ClosedDealsNotifier(config)
            return cls._instance

    @exception_handler
    async def register_observer(self,
                                symbol: str,
                                magic_number: int,
                                callback: ObserverCallback,
                                observer_id: str) -> None:
        """Registers a new observer for a specific symbol and magic number."""
        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            if magic_number not in self.observers[symbol]:
                self.observers[symbol][magic_number] = {}

            observer = SymbolDealsObserver(symbol, magic_number, callback)
            self.observers[symbol][magic_number][observer_id] = observer

            self.info(
                f"Registered observer {observer_id} for symbol {symbol} with magic number {magic_number}"
            )
            # Start monitoring if not already running
            if not self._running:
                await self.start()

    @exception_handler
    async def unregister_observer(self, symbol: str, magic_number: int, observer_id: str) -> None:
        """Removes an observer for a specific symbol and magic number."""
        async with self._observers_lock:
            await self._remove_observer_and_cleanup(symbol, magic_number, observer_id)

    async def _remove_observer_and_cleanup(self, symbol: str, magic_number: int, observer_id: str):
        """Removes an observer and stops the monitoring task if necessary."""
        async with self._observers_lock:
            if symbol in self.observers:
                if magic_number in self.observers[symbol]:
                    if observer_id in self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number][observer_id]
                        self.info(f"Unregistered observer {observer_id} for symbol {symbol} with magic number {magic_number}")

                    if not self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number]
                        self.info(f"Removed monitoring for magic number {magic_number} of symbol {symbol}")

                if not self.observers[symbol]:
                    await self._stop_monitoring_symbol(symbol)

    async def _stop_monitoring_symbol(self, symbol: str):
        del self.observers[symbol]
        self.info(f"Stopped monitoring for symbol {symbol}")

        if symbol in self.tasks:
            self.tasks[symbol].cancel()
            try:
                await self.tasks[symbol]
            except asyncio.CancelledError:
                pass
            del self.tasks[symbol]
            self.info(f"Stopped monitoring task for symbol {symbol}")

    async def start(self):
        """Starts monitoring closed deals."""
        async with self._start_lock:  # use start lock.
            if not self._running:
                self._running = True
                # Start all monitoring tasks on startup.
                for symbol in list(self.observers.keys()):  # list() to avoid modifying dict during iteration.
                    if symbol not in self.tasks:
                        self.tasks[symbol] = asyncio.create_task(self._monitor_symbol(symbol))
                        self.info(f"Started monitoring for symbol {symbol}")
                self.info("Closed deals monitoring started")

    async def stop(self):
        """Stops monitoring closed deals."""
        async with self._start_lock:  # use start lock.
            if self._running:
                self._running = False
                for symbol, task in list(self.tasks.items()):  # Iterate over a copy
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.info(f"Stopped monitoring task for symbol {symbol}")
                self.tasks.clear()  # Clear tasks after stopping them
                self.info("Closed deals monitoring stopped")

    async def _calculate_sleep_time(self) -> float:
        """Calculates the time to sleep."""
        return max(self.interval_seconds, self._min_sleep_time)

    async def _get_observers_copy(self, symbol: str) -> Dict[int, Dict[str, SymbolDealsObserver]]:
        """Gets a copy of the observers for a given symbol."""
        async with self._observers_lock:
            return self.observers.get(symbol, {}).copy()

    async def _notify_observers(self, symbol: str, magic_number: int, positions: List[Position]):
        """Notifies all observers for a given symbol and magic number."""

        observers = await self._get_observers_copy(symbol)
        if magic_number not in observers:
            return
        notification_tasks = [
            observer.callback(position)
            for position in positions
            for observer in observers[magic_number].values()
        ]
        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            self.debug(f"Notified {len(positions)} closed positions for symbol {symbol} with magic number {magic_number} to {len(observers[magic_number])} observers")

    async def _monitor_symbol(self, symbol: str) -> None:
        """Monitoring loop for a specific symbol."""
        last_check_time = now_utc()
        try:
            while self._running:  # Use self._running for the loop condition
                current_time = now_utc()
                prev_check_time = last_check_time
                last_check_time = current_time

                if not await Broker().with_context(f"{symbol}").is_market_open(symbol):
                    sleep_duration = await self._calculate_sleep_time()
                    await asyncio.sleep(sleep_duration)
                    continue

                magic_observers = await self._get_observers_copy(symbol)

                for magic_number in magic_observers.keys():
                    try:
                        positions: List[Position] = await Broker().with_context(f"{symbol}").get_historical_positions(
                            prev_check_time,
                            current_time,
                            symbol,
                            magic_number
                        )
                        if positions:
                            await self._notify_observers(symbol, magic_number, positions)

                    except Exception as e:
                        self.error(f"Error processing symbol {symbol} with magic number {magic_number}", exec_info=e)

                sleep_duration = await self._calculate_sleep_time()
                await asyncio.sleep(sleep_duration)


        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.error(f"Error in monitor loop for symbol {symbol}", exec_info=e)

    async def shutdown(self) -> None:
        """Stops all monitoring tasks and cleans up resources."""
        await self.stop()  # use stop that is idempotent
        async with self._observers_lock:
            self.observers.clear()
            self.info("ClosedDealsManager shutdown completed")
