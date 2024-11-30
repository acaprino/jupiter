import asyncio
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Awaitable, Tuple, TypeVar

from brokers.broker_interface import BrokerAPI
from dto.Position import Position
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

T = TypeVar('T')
ObserverCallback = Callable[[Position], Awaitable[None]]


class SymbolDealsObserver:
    """Rappresenta un observer per le posizioni chiuse di un simbolo."""

    def __init__(self, symbol: str, magic_number: int, callback: ObserverCallback):
        self.symbol: str = symbol
        self.magic_number: int = magic_number
        self.callback: ObserverCallback = callback


class ClosedDealsManager:
    """Manager thread-safe per monitorare le posizioni chiuse."""

    _instance: Optional['ClosedDealsManager'] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'ClosedDealsManager':
        with cls._instance_lock:
            if cls._instance is None:
                instance = super(ClosedDealsManager, cls).__new__(cls)
                instance.__initialized = False
                cls._instance = instance
            return cls._instance

    def __init__(self) -> None:
        with self._instance_lock:
            if not self.__initialized:
                # Lock per proteggere le operazioni sugli observer e lo stato
                self._observers_lock: asyncio.Lock = asyncio.Lock()
                self._state_lock: asyncio.Lock = asyncio.Lock()

                # Attributi di istanza
                self.observers: Dict[Tuple[str, int], Dict[str, SymbolDealsObserver]] = {}
                self.logger: BotLogger = BotLogger.get_logger("ClosedDealsManager")
                self._running: bool = False
                self._task: Optional[asyncio.Task] = None
                self.broker = None
                self.interval_seconds: int = 60 * 1  # 1 minuto
                self.last_check_time: datetime = now_utc()

                self.__initialized = True

    def _get_observer_key(self, symbol: str, magic_number: int) -> Tuple[str, int]:
        """Crea una chiave univoca per l'observer."""
        return (symbol, magic_number)

    @exception_handler
    async def register_observer(self,
                                symbol: str,
                                magic_number: int,
                                broker: BrokerAPI,
                                callback: ObserverCallback,
                                observer_id: str) -> None:
        """Registra un nuovo observer per un simbolo e magic number specifici."""
        start_needed = False

        async with self._observers_lock:
            key = self._get_observer_key(symbol, magic_number)
            if key not in self.observers:
                self.observers[key] = {}

            observer = SymbolDealsObserver(symbol, magic_number, callback)
            self.observers[key][observer_id] = observer

            self.logger.info(
                f"Registered observer {observer_id} for symbol {symbol} "
                f"with magic number {magic_number}"
            )
            async with self._state_lock:
                # Avvia il monitor se non è già in esecuzione
                if not self.broker:
                    self.broker = broker
                if not self._running:
                    self._running = True
                    start_needed = True

            if start_needed:
                await self.start()

    @exception_handler
    async def unregister_observer(self, symbol: str, magic_number: int, observer_id: str) -> None:
        """Rimuove un observer per un simbolo e magic number specifici."""
        stop_needed = False

        async with self._observers_lock:
            key = self._get_observer_key(symbol, magic_number)
            if key in self.observers:
                if observer_id in self.observers[key]:
                    del self.observers[key][observer_id]
                    self.logger.info(
                        f"Unregistered observer {observer_id} for symbol {symbol} "
                        f"with magic number {magic_number}"
                    )

                # Rimuovi la configurazione se non ha più observers
                if not self.observers[key]:
                    del self.observers[key]
                    self.logger.info(
                        f"Removed monitoring for symbol {symbol} "
                        f"with magic number {magic_number}"
                    )

            # Ferma il monitor se non ci sono più observers
            async with self._state_lock:
                if not any(self.observers.values()) and self._running:
                    stop_needed = True
            if stop_needed:
                await self.stop()

    async def start(self) -> None:
        """Avvia il monitor delle posizioni chiuse."""
        async with self._state_lock:
            if not self._running:
                self._running = True
                self.last_check_time = now_utc()
                self._task = asyncio.create_task(self._monitor_loop())
                self.logger.info("Closed deals monitoring started")

    async def stop(self) -> None:
        """Ferma il monitor."""
        async with self._state_lock:
            if self._running:
                self._running = False
                if self._task is not None:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self._task = None
                self.logger.info("Closed deals monitoring stopped")

    async def shutdown(self) -> None:
        """Ferma il monitor e pulisce le risorse."""
        async with self._state_lock:
            await self.stop()
        async with self._observers_lock:
            self.observers.clear()

    async def _monitor_loop(self) -> None:
        """Loop principale di monitoraggio."""
        while True:
            try:
                async with self._state_lock:
                    if not self._running:
                        break

                current_time = now_utc()
                prev_check_time = self.last_check_time
                self.last_check_time = current_time

                # Crea una copia sicura degli observers per l'iterazione
                async with self._observers_lock:
                    observers_copy = {k: v.copy() for k, v in self.observers.items()}

                # Raggruppa gli observers per symbol
                symbol_observers: Dict[str, Dict[int, List[SymbolDealsObserver]]] = {}
                for (symbol, magic_number), observer_group in observers_copy.items():
                    if symbol not in symbol_observers:
                        symbol_observers[symbol] = {}
                    symbol_observers[symbol][magic_number] = list(observer_group.values())

                # Per ogni simbolo
                for symbol, magic_observers in symbol_observers.items():
                    try:

                        # Verifica se il mercato è aperto
                        if not await self.broker.is_market_open(symbol):
                            continue

                        # Per ogni magic number del simbolo
                        for magic_number, observers in magic_observers.items():
                            try:
                                # Ottieni le posizioni chiuse per il simbolo e magic number
                                positions: List[Position] = await self.broker.get_historical_positions(
                                    prev_check_time,
                                    current_time,
                                    symbol,
                                    magic_number
                                )

                                if not positions:
                                    continue

                                # Notifica tutti gli observers per questo magic number
                                notification_tasks = [
                                    observer.callback(position)
                                    for observer in observers
                                    for position in positions
                                ]

                                if notification_tasks:
                                    await asyncio.gather(*notification_tasks, return_exceptions=True)

                                    self.logger.debug(
                                        f"Notified {len(positions)} closed positions for "
                                        f"symbol {symbol} with magic number {magic_number} "
                                        f"to {len(observers)} observers"
                                    )

                            except Exception as e:
                                self.logger.error(
                                    f"Error processing symbol {symbol} with magic number {magic_number}: {e}"
                                )

                    except Exception as e:
                        self.logger.error(f"Error processing symbol {symbol}: {e}")

                await asyncio.sleep(self.interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(5)
