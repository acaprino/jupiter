import asyncio
from datetime import timedelta
from typing import Dict, List, Optional, Callable, Awaitable

from brokers.broker_proxy import Broker
from dto.Position import Position
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc


# Define an alias for the observer callback type (an asynchronous function)
ObserverCallback = Callable[[Position], Awaitable[None]]

class SymbolFilledOrdersObserver:
    """Represents an observer for filled orders of a symbol."""
    def __init__(self, symbol: str, magic_number: Optional[int], callback: ObserverCallback):
        self.symbol: str = symbol
        self.magic_number: Optional[int] = magic_number
        self.callback: ObserverCallback = callback

class FilledOrdersNotifier(LoggingMixin):

    _instance: Optional['FilledOrdersNotifier'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            raise RuntimeError("Use ClosedDealsNotifier.get_instance() instead")
        return super().__new__(cls)

    def __init__(self, config: ConfigReader) -> None:
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._start_lock: asyncio.Lock = asyncio.Lock()

        # Allow Optional[int] for magic_number in the observers dictionary
        self.observers: Dict[str, Dict[Optional[int], Dict[str, SymbolFilledOrdersObserver]]] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

        self.config = config
        self.agent = "FilledOrdersManager"

        self.interval_seconds: float = 30.0
        self._running = False
        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'FilledOrdersNotifier':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(config)
            return cls._instance

    async def register_observer(self,
                                symbol: str,
                                callback: ObserverCallback,
                                observer_id: str,
                                magic_number: Optional[int] = None) -> None:
        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            # Assicuriamoci che la chiave magic_number esista (incluso None)
            if magic_number not in self.observers[symbol]:
                self.observers[symbol][magic_number] = {}

            observer = SymbolFilledOrdersObserver(symbol, magic_number, callback)
            self.observers[symbol][magic_number][observer_id] = observer

            self.info(f"Registered observer {observer_id} for {symbol}/{magic_number if magic_number is not None else 'any magic number'}")

            if not self._running:
                await self.start()
            else:
                # Se _running è True e non esiste ancora un task per questo simbolo, crealo.
                if symbol not in self.tasks:
                    self.tasks[symbol] = asyncio.create_task(self._monitor_symbol(symbol))
                    self.info(f"Started monitoring {symbol}")

    @exception_handler
    async def unregister_observer(self, symbol: str, magic_number: Optional[int], observer_id: str) -> None:
        async with self._observers_lock:
            await self._remove_observer_and_cleanup(symbol, magic_number, observer_id)

    async def _remove_observer_and_cleanup(self, symbol: str, magic_number: Optional[int], observer_id: str):
        async with self._observers_lock:
            if symbol in self.observers:
                if magic_number in self.observers[symbol]:
                    if observer_id in self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number][observer_id]
                        self.info(f"Unregistered observer {observer_id} for {symbol}/{magic_number if magic_number is not None else 'any magic number'}")

                    if not self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number]
                        self.info(f"Removed magic number {magic_number} for {symbol}")

                if not self.observers[symbol]:
                    await self._stop_monitoring_symbol(symbol)

    async def _stop_monitoring_symbol(self, symbol: str):
        del self.observers[symbol]
        self.info(f"Stopped monitoring {symbol}")

        if symbol in self.tasks:
            self.tasks[symbol].cancel()
            try:
                await self.tasks[symbol]
            except asyncio.CancelledError:
                pass
            del self.tasks[symbol]
            self.info(f"Stopped task for {symbol}")

    async def start(self):
        async with self._start_lock:
            if not self._running:
                self._running = True
                for symbol in list(self.observers.keys()):
                    if symbol not in self.tasks:
                        self.tasks[symbol] = asyncio.create_task(self._monitor_symbol(symbol))
                        self.info(f"Started monitoring {symbol}")
                self.info("Monitoring started")

    async def stop(self):
        async with self._start_lock:
            if self._running:
                self._running = False
                for symbol, task in list(self.tasks.items()):
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.info(f"Stopped task for {symbol}")
                self.tasks.clear()
                self.info("Monitoring stopped")

    async def _get_observers_copy(self, symbol: str) -> Dict[Optional[int], Dict[str, SymbolFilledOrdersObserver]]:
        async with self._observers_lock:
            return self.observers.get(symbol, {}).copy()

    async def _notify_observers(self, symbol: str, magic_number: Optional[int], positions: List[Position]):
        observers = await self._get_observers_copy(symbol)
        if magic_number not in observers:
            return
        tasks = [
            observer.callback(position)
            for position in positions
            for observer in observers[magic_number].values()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            self.debug(f"Notified {len(positions)} positions for {symbol}/{magic_number if magic_number is not None else 'any magic number'}")

    async def _monitor_symbol(self, symbol: str):
        known_position_ids: Dict[Optional[int], set] = {}
        first_run = True
        while self._running:
            try:
                observers = await self._get_observers_copy(symbol)
                broker = Broker().with_context(symbol)
                for magic_number in observers.keys():
                    open_positions: List[Position] = await broker.get_open_positions(symbol, magic_number)
                    current_ids = {pos.position_id for pos in open_positions}
                    prev_ids = known_position_ids.get(magic_number, set())
                    new_ids = current_ids - prev_ids

                    if first_run:
                        self.debug(f"{self.agent}: First run - initialize known_position_ids for {symbol}/{magic_number}")
                    elif new_ids:
                        self.info(
                            f"{self.agent}: Detected new filled orders for {symbol}/{magic_number if magic_number is not None else 'any magic number'}: {new_ids}"
                        )
                        new_positions = [pos for pos in open_positions if pos.position_id in new_ids]
                        await self._notify_observers(symbol, magic_number, new_positions)
                    else:
                        self.debug(
                            f"{self.agent}: No new filled orders detected for {symbol}/{magic_number if magic_number is not None else 'any magic number'}."
                        )
                    known_position_ids[magic_number] = current_ids
                first_run = False
            except Exception as e:
                self.error(f"Error during polling for {self.agent} on symbol {symbol}: {e}")
            await asyncio.sleep(self.interval_seconds)



