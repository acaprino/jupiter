import asyncio
from datetime import datetime
from typing import Callable, Awaitable, List

from misc_utils.enums import Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.bot_logger import BotLogger
from misc_utils.utils_functions import dt_to_unix, now_utc, unix_to_datetime


class TickNotifier:
    """
    Notifies registered callbacks at the start of each new tick based on the specified timeframe.
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
        """Starts the tick notifier loop."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            self.logger.info(f"TickNotifier started for timeframe {self.timeframe}.")

    @exception_handler
    async def stop(self):
        """Stops the tick notifier loop."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info(f"TickNotifier stopped for timeframe {self.timeframe}.")

    def register_on_new_tick(self, callback: Callable[[Timeframe, datetime], Awaitable[None]]):
        """Registers a callback to be notified at the start of each new tick."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_new_tick_callbacks.append(callback)
        self.logger.info("Callback registered for new tick notifications.")

    @exception_handler
    async def _run(self):
        """Main loop to trigger registered callbacks at each new tick interval."""
        while self._running:
            try:
                timestamp = await self.wait_next_tick()
                self.logger.info(f"New tick triggered for {self.timeframe} at {timestamp}.")

                # Trigger all registered callbacks
                tasks = [callback(self.timeframe, timestamp) for callback in self._on_new_tick_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                self.logger.error(f"Error in TickNotifier._run: {e}")

    @exception_handler
    async def wait_next_tick(self) -> datetime:
        """Calculates the time to wait until the next tick and waits for it."""
        timeframe_duration = self.timeframe.to_seconds()
        now = now_utc()
        current_time = dt_to_unix(now)

        # Calculate time until the next tick
        time_to_next_candle = timeframe_duration - (current_time % timeframe_duration)
        next_candle_time = unix_to_datetime(current_time + time_to_next_candle).replace(microsecond=0)

        self.logger.debug(f"Waiting for next candle, current time {now}, {time_to_next_candle} seconds until next tick.")

        await asyncio.sleep(time_to_next_candle)

        return next_candle_time
