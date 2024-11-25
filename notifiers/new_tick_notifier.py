import asyncio
from datetime import datetime, timedelta
from typing import Callable, Awaitable, List

from misc_utils.enums import Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.bot_logger import BotLogger
from misc_utils.utils_functions import now_utc

class TickNotifier:
    """
    Notifica i callback registrati all'inizio di ogni nuovo tick basato sul timeframe specificato.
    """

    def __init__(self, routine_label: str, timeframe: Timeframe, execution_lock: asyncio.Lock = None):
        self.routine_label = routine_label
        self.logger = BotLogger.get_logger(routine_label)
        self.timeframe = timeframe
        self.execution_lock = execution_lock
        self._running = False
        self._task = None
        self._on_new_tick_callbacks: List[Callable[[Timeframe, datetime], Awaitable[None]]] = []

    @exception_handler
    async def start(self):
        """Avvia il loop del tick notifier."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            self.logger.info(f"TickNotifier avviato per timeframe {self.timeframe}.")

    @exception_handler
    async def stop(self):
        """Ferma il loop del tick notifier."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info(f"TickNotifier fermato per timeframe {self.timeframe}.")

    def register_on_new_tick(self, callback: Callable[[Timeframe, datetime], Awaitable[None]]):
        """Registra un callback da notificare all'inizio di ogni nuovo tick."""
        if not callable(callback):
            raise ValueError("Il callback deve essere callable")
        self._on_new_tick_callbacks.append(callback)
        self.logger.info("Callback registrato per le notifiche di nuovo tick.")

    @exception_handler
    async def _run(self):
        """Loop principale per attivare i callback registrati ad ogni nuovo intervallo di tick."""
        while self._running:
            try:
                timestamp = await self.wait_next_tick()
                self.logger.info(f"Nuovo tick attivato per {self.timeframe} alle {timestamp}.")

                # Attiva tutti i callback registrati
                tasks = [callback(self.timeframe, timestamp) for callback in self._on_new_tick_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                self.logger.error(f"Errore in TickNotifier._run: {e}")

    @exception_handler
    async def wait_next_tick(self) -> datetime:
        """Calcola il tempo di attesa fino al prossimo tick e attende fino a quel momento."""
        timeframe_seconds = self.timeframe.to_seconds()
        now = now_utc().replace(microsecond=0)

        current_timestamp = int(now.timestamp())

        # Calcola il timestamp del prossimo tick
        remainder = current_timestamp % timeframe_seconds
        if remainder == 0:
            next_tick_timestamp = current_timestamp
        else:
            next_tick_timestamp = current_timestamp + (timeframe_seconds - remainder)

        time_to_wait = next_tick_timestamp - current_timestamp
        next_tick_time = datetime.fromtimestamp(next_tick_timestamp, tz=now.tzinfo)

        self.logger.debug(f"Attesa per il prossimo tick, ora corrente {now}, prossimo tick alle {next_tick_time}, attesa di {time_to_wait} secondi.")

        await asyncio.sleep(time_to_wait)

        # Verifica dopo il risveglio
        now_after_sleep = now_utc().replace(microsecond=0)
        if now_after_sleep < next_tick_time:
            remaining_time = (next_tick_time - now_after_sleep).total_seconds()
            if remaining_time > 0:
                self.logger.debug(f"Attesa aggiuntiva necessaria: {remaining_time} secondi.")
                await asyncio.sleep(remaining_time)

        return next_tick_time
