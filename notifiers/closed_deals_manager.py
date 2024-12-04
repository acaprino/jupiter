import asyncio
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Awaitable, Tuple

from brokers.broker_interface import BrokerAPI
from dto.Position import Position
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

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
                cls._instance = super(ClosedDealsManager, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, '_initialized', False):
            return

        with self._instance_lock:
            if not getattr(self, '_initialized', False):
                # Lock per proteggere le operazioni sugli observer
                self._observers_lock: asyncio.Lock = asyncio.Lock()
                # Dizionari per gli observers e i task
                self.observers: Dict[str, Dict[int, Dict[str, SymbolDealsObserver]]] = {}
                self.tasks: Dict[str, asyncio.Task] = {}
                self.logger: BotLogger = BotLogger.get_logger("ClosedDealsManager")
                self.interval_seconds: int = 60  # 1 minuto
                self._initialized = True

    @exception_handler
    async def register_observer(self,
                                symbol: str,
                                magic_number: int,
                                broker: BrokerAPI,
                                callback: ObserverCallback,
                                observer_id: str) -> None:
        """Registra un nuovo observer per un simbolo e magic number specifici."""
        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            if magic_number not in self.observers[symbol]:
                self.observers[symbol][magic_number] = {}

            observer = SymbolDealsObserver(symbol, magic_number, callback)
            self.observers[symbol][magic_number][observer_id] = observer

            self.logger.info(
                f"Registered observer {observer_id} for symbol {symbol} with magic number {magic_number}"
            )

            # Avvia un nuovo task di monitoraggio per questo simbolo se non già in esecuzione
            if symbol not in self.tasks:
                self.tasks[symbol] = asyncio.create_task(self._monitor_symbol(symbol, broker))
                self.logger.info(f"Started monitoring for symbol {symbol}")

    @exception_handler
    async def unregister_observer(self, symbol: str, magic_number: int, observer_id: str) -> None:
        """Rimuove un observer per un simbolo e magic number specifici."""
        async with self._observers_lock:
            if symbol in self.observers:
                if magic_number in self.observers[symbol]:
                    if observer_id in self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number][observer_id]
                        self.logger.info(
                            f"Unregistered observer {observer_id} for symbol {symbol} with magic number {magic_number}"
                        )
                    if not self.observers[symbol][magic_number]:
                        del self.observers[symbol][magic_number]
                        self.logger.info(
                            f"Removed monitoring for magic number {magic_number} of symbol {symbol}"
                        )
                if not self.observers[symbol]:
                    del self.observers[symbol]
                    self.logger.info(f"Stopped monitoring for symbol {symbol}")

                    # Cancella il task di monitoraggio per questo simbolo
                    if symbol in self.tasks:
                        self.tasks[symbol].cancel()
                        try:
                            await self.tasks[symbol]
                        except asyncio.CancelledError:
                            pass
                        del self.tasks[symbol]
                        self.logger.info(f"Stopped monitoring task for symbol {symbol}")

    async def _monitor_symbol(self, symbol: str, broker: BrokerAPI) -> None:
        """Loop di monitoraggio per un simbolo specifico."""
        try:
            last_check_time = now_utc()
            while True:
                current_time = now_utc()
                prev_check_time = last_check_time
                last_check_time = current_time

                # Verifica se il mercato è aperto
                if not await broker.is_market_open(symbol):
                    await asyncio.sleep(self.interval_seconds)
                    continue

                async with self._observers_lock:
                    magic_observers = self.observers.get(symbol, {}).copy()

                for magic_number, observers in magic_observers.items():
                    try:
                        # Ottieni le posizioni chiuse per il simbolo e magic number
                        positions: List[Position] = await broker.get_historical_positions(
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
                            for observer in observers.values()
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

                await asyncio.sleep(self.interval_seconds)

        except asyncio.CancelledError:
            # Il task è stato cancellato
            pass
        except Exception as e:
            self.logger.error(f"Error in monitor loop for symbol {symbol}: {e}")

    async def shutdown(self) -> None:
        """Ferma tutti i task di monitoraggio e pulisce le risorse."""
        async with self._observers_lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
            self.logger.info("ClosedDealsManager shutdown completed")
