import asyncio
from datetime import timedelta
from typing import Dict, List, Optional, Callable, Awaitable

from brokers.broker_proxy import Broker
from dto.Position import Position
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
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
        self._start_lock: asyncio.Lock = asyncio.Lock()

        self.observers: Dict[str, Dict[int, Dict[str, SymbolDealsObserver]]] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

        self.config = config
        self.agent = "ClosedDealsManager"

        self.interval_seconds: float = 60.0  # Default to 60 seconds
        self._running = False
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
        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            if magic_number not in self.observers[symbol]:
                self.observers[symbol][magic_number] = {}

            observer = SymbolDealsObserver(symbol, magic_number, callback)
            self.observers[symbol][magic_number][observer_id] = observer

            self.info(f"Registered observer {observer_id} for {symbol}/{magic_number}")
            if not self._running:
                await self.start()

    @exception_handler
    async def unregister_observer(self, symbol: str, magic_number: int, observer_id: str) -> None:
        async with self._observers_lock:
            await self._remove_observer_and_cleanup(symbol, magic_number, observer_id)

    async def _remove_observer_and_cleanup(self, symbol: str, magic_number: int, observer_id: str):
        async with self._observers_lock:
            if symbol in self.observers:
                if magic_number in self.observers[symbol]:
                    if observer_id in self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number][observer_id]
                        self.info(f"Unregistered observer {observer_id} for {symbol}/{magic_number}")

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

    async def _get_observers_copy(self, symbol: str) -> Dict[int, Dict[str, SymbolDealsObserver]]:
        async with self._observers_lock:
            return self.observers.get(symbol, {}).copy()

    async def _notify_observers(self, symbol: str, magic_number: int, positions: List[Position]):
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
            self.debug(f"Notified {len(positions)} positions for {symbol}/{magic_number}")

    async def _monitor_symbol(self, symbol: str) -> None:
        """Monitoring loop for a specific symbol with precise interval timing."""
        last_check_time = now_utc()
        next_interval = last_check_time + timedelta(seconds=self.interval_seconds)

        while self._running:
            try:
                # Calcola il tempo rimanente fino al prossimo intervallo
                current_time = now_utc()
                sleep_duration = (next_interval - current_time).total_seconds()

                # Aspetta solo se siamo in anticipo rispetto al prossimo intervallo
                if sleep_duration > 0:
                    await asyncio.sleep(sleep_duration)

                # Aggiorna i tempi per il ciclo successivo
                current_check_time = now_utc()
                next_interval = current_check_time + timedelta(seconds=self.interval_seconds)

                # Verifica se il mercato è aperto
                if not await Broker().with_context(symbol).is_market_open(symbol):
                    continue

                # Elabora le posizioni per ogni magic number
                magic_observers = await self._get_observers_copy(symbol)
                for magic_number in magic_observers:
                    try:
                        positions = await Broker().with_context(symbol).get_historical_positions(
                            last_check_time,
                            current_check_time,
                            symbol,
                            magic_number
                        )

                        if positions:
                            await self._notify_observers(symbol, magic_number, positions)

                        # Aggiorna last_check_time solo dopo il successo del processing
                        last_check_time = current_check_time

                    except Exception as e:
                        self.error(f"Error processing {symbol}/{magic_number}", exc_info=e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.error(f"Error monitoring {symbol}", exc_info=e)
                await asyncio.sleep(self.interval_seconds)  # Fallback sleep on error

    async def shutdown(self) -> None:
        await self.stop()
        async with self._observers_lock:
            self.observers.clear()
            self.info("Shutdown complete")
