import asyncio
import threading
from datetime import datetime
from typing import Dict, Callable, Awaitable, Optional

from misc_utils.enums import Timeframe
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[Timeframe, datetime], Awaitable[None]]

class TickObserver:
    """Represents an observer for a timeframe tick."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback


class TickManager:
    """Singleton class that manages tick notifications for different timeframes."""

    _instance: Optional['TickManager'] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'TickManager':
        with cls._instance_lock:
            if cls._instance is None:
                instance = super(TickManager, cls).__new__(cls)
                instance.__initialized = False
                cls._instance = instance
            return cls._instance

    def __init__(self):
        if not getattr(self, '__initialized', False):
            # Locks to protect shared resources
            self._observers_lock: asyncio.Lock = asyncio.Lock()
            self._state_lock: asyncio.Lock = asyncio.Lock()

            # Dictionary of observers: {timeframe: {observer_id: TickObserver}}
            self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}

            self.logger = BotLogger.get_logger("TickManager")

            self._running: bool = False
            self._task: Optional[asyncio.Task] = None

            self.__initialized = True

    @exception_handler
    async def register_observer(self,
                                timeframe: Timeframe,
                                callback: ObserverCallback,
                                observer_id: str):
        """Registers a new observer for a timeframe."""
        start_needed = False

        async with self._observers_lock:
            if timeframe not in self.observers:
                self.observers[timeframe] = {}

            observer = TickObserver(callback)
            self.observers[timeframe][observer_id] = observer

            self.logger.info(f"Registered observer {observer_id} for timeframe {timeframe.name}")

            # Check if we need to start the monitoring loop
            async with self._state_lock:
                if not self._running:
                    self._running = True
                    start_needed = True

        if start_needed:
            await self.start()

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer for a timeframe."""
        stop_needed = False

        async with self._observers_lock:
            if timeframe in self.observers:
                if observer_id in self.observers[timeframe]:
                    del self.observers[timeframe][observer_id]
                    self.logger.info(f"Unregistered observer {observer_id} for timeframe {timeframe.name}")

                # Remove the timeframe entry if no observers left
                if not self.observers[timeframe]:
                    del self.observers[timeframe]
                    self.logger.info(f"Removed monitoring for timeframe {timeframe.name}")

            async with self._state_lock:
                if not any(self.observers.values()) and self._running:
                    stop_needed = True

            if stop_needed:
                await self.stop()

    async def start(self):
        """Starts the tick monitoring loop."""
        async with self._state_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.logger.info("Tick monitoring started")

    async def stop(self):
        """Stops the tick monitoring loop."""
        async with self._state_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info("Tick monitoring stopped")
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
                    timeframes = list(self.observers.keys())

                if not timeframes:
                    continue

                # Compute the next tick times for all timeframes
                next_tick_times = {}
                min_sleep_time = None

                for timeframe in timeframes:
                    timeframe_seconds = timeframe.to_seconds()

                    current_timestamp = int(current_time.timestamp())
                    remainder = current_timestamp % timeframe_seconds

                    if remainder == 0:
                        next_tick_timestamp = current_timestamp + timeframe_seconds
                    else:
                        next_tick_timestamp = current_timestamp + (timeframe_seconds - remainder)

                    next_tick_time = datetime.fromtimestamp(next_tick_timestamp, tz=current_time.tzinfo)
                    sleep_time = (next_tick_time - current_time).total_seconds()

                    next_tick_times[timeframe] = next_tick_time

                    if min_sleep_time is None or sleep_time < min_sleep_time:
                        min_sleep_time = sleep_time

                # Sleep until the earliest next tick
                if min_sleep_time is not None and min_sleep_time > 0:
                    await asyncio.sleep(min_sleep_time)

                # After sleeping, get the current time
                current_time = now_utc()

                # For each timeframe, check if it's time for the tick
                for timeframe, next_tick_time in next_tick_times.items():
                    if current_time >= next_tick_time:
                        # Notify observers for this timeframe
                        async with self._observers_lock:
                            observers = self.observers.get(timeframe, {}).copy()

                        notification_tasks = []
                        for observer_id, observer in observers.items():
                            try:
                                notification_tasks.append(observer.callback(timeframe, next_tick_time))
                            except Exception as e:
                                self.logger.error(f"Error preparing notification for observer {observer_id}: {e}")

                        if notification_tasks:
                            await asyncio.gather(*notification_tasks, return_exceptions=True)
                            self.logger.debug(f"Notified observers for timeframe {timeframe.name} at {next_tick_time}")

                # Sleep for a short time before next iteration
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Error in tick monitor loop: {e}")
                await asyncio.sleep(1)
