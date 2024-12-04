import asyncio
import threading
from datetime import datetime, timezone
from typing import Dict, Callable, Awaitable, Optional

from misc_utils.enums import Timeframe
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[Timeframe, datetime], Awaitable[None]]


class TickObserver:
    """Rappresenta un osservatore per un tick di un timeframe."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback


class TickManager:
    """Classe singleton che gestisce le notifiche di tick per diversi timeframe."""
    _instance: Optional['TickManager'] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'TickManager':
        with cls._instance_lock:  # Acquisisce il lock prima di tutto
            if cls._instance is None:
                cls._instance = super(TickManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        with self._instance_lock:
            if not getattr(self, "_initialized", False):
                # Inizializza le variabili di istanza
                self._observers_lock = asyncio.Lock()
                self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}
                self.tasks: Dict[Timeframe, asyncio.Task] = {}
                self.logger = BotLogger.get_logger("TickManager")
                self._initialized = True

    @exception_handler
    async def register_observer(self,
                                timeframe: Timeframe,
                                callback: ObserverCallback,
                                observer_id: str):
        """Registra un nuovo osservatore per un timeframe."""
        async with self._observers_lock:
            if timeframe not in self.observers:
                self.observers[timeframe] = {}
            self.observers[timeframe][observer_id] = TickObserver(callback)
            self.logger.info(f"Registered observer {observer_id} for timeframe {timeframe.name}")

            # Avvia un nuovo task per questo timeframe se non già in esecuzione
            if timeframe not in self.tasks:
                self.tasks[timeframe] = asyncio.create_task(self._monitor_timeframe(timeframe))
                self.logger.info(f"Started monitoring for timeframe {timeframe.name}")

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Rimuove un osservatore per un timeframe."""
        async with self._observers_lock:
            if timeframe in self.observers and observer_id in self.observers[timeframe]:
                del self.observers[timeframe][observer_id]
                self.logger.info(f"Unregistered observer {observer_id} for timeframe {timeframe.name}")

                # Se non ci sono più osservatori per questo timeframe, cancella il task
                if not self.observers[timeframe]:
                    del self.observers[timeframe]
                    if timeframe in self.tasks:
                        self.tasks[timeframe].cancel()
                        try:
                            await self.tasks[timeframe]
                        except asyncio.CancelledError:
                            pass
                        self.logger.info(f"Stopped monitoring for timeframe {timeframe.name}")
                        del self.tasks[timeframe]

    async def _monitor_timeframe(self, timeframe: Timeframe):
        """Loop di monitoraggio per un specifico timeframe."""
        try:
            timeframe_seconds = timeframe.to_seconds()
            while True:
                current_time = now_utc()
                # Calcola il prossimo tick time
                current_timestamp = int(current_time.timestamp())
                remainder = current_timestamp % timeframe_seconds
                sleep_seconds = timeframe_seconds - remainder if remainder != 0 else 0
                # Dorme fino al prossimo tick
                await asyncio.sleep(sleep_seconds)
                self.logger.info(f"New tick for {timeframe.name}.")
                tick_time = now_utc()

                # Notifica gli osservatori
                async with self._observers_lock:
                    observers = self.observers.get(timeframe, {}).copy()

                notification_tasks = []
                for observer_id, observer in observers.items():
                    try:
                        notification_tasks.append(observer.callback(timeframe, tick_time))
                    except Exception as e:
                        self.logger.error(f"Error preparing notification for observer {observer_id}: {e}")

                if notification_tasks:
                    await asyncio.gather(*notification_tasks, return_exceptions=True)
                    self.logger.debug(f"Notified observers for timeframe {timeframe.name} at {tick_time}")

        except asyncio.CancelledError:
            # Il task è stato cancellato, pulizia se necessario
            pass
        except Exception as e:
            self.logger.error(f"Error in timeframe monitor loop for {timeframe.name}: {e}")

    async def shutdown(self):
        """Ferma tutti i task di monitoraggio e pulisce le risorse."""
        async with self._observers_lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
            self.logger.info("TickManager shutdown completed")
