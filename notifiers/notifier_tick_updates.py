import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Callable, Awaitable, Optional

from misc_utils.config import ConfigReader
from misc_utils.enums import Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[Timeframe, datetime], Awaitable[None]]


class TickObserver:
    """Represents an observer for a tick of a timeframe."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback


class NotifierTickUpdates(LoggingMixin):
    """
    Singleton class that manages tick notifications for different timeframes.
    Provides improved precision and robustness.
    """
    _instance: Optional['NotifierTickUpdates'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()
    SEMAPHORE = asyncio.Semaphore(100)  # Max task concorrenti

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader):
        if getattr(self, "_initialized", False):
            return

        super().__init__(config)
        self._observers_lock = asyncio.Lock()
        self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}
        self.tasks: Dict[Timeframe, asyncio.Task] = {}
        self.config = config
        self.agent = "NotifierTickUpdates"
        self._max_observers = 100  # Default to 100.
        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierTickUpdates':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierTickUpdates(config)
            return cls._instance

    @exception_handler
    async def register_observer(self,
                                timeframe: Timeframe,
                                callback: ObserverCallback,
                                observer_id: str):
        """Registers a new observer for a timeframe."""
        # Validation remains the same as before
        async with self._observers_lock:
            if timeframe not in self.observers:
                self.observers[timeframe] = {}

            if len(self.observers[timeframe]) >= self._max_observers:
                raise RuntimeError(f"Too many observers for timeframe {timeframe.name}")

            self.observers[timeframe][observer_id] = TickObserver(callback)
            self.info(f"Registered observer {observer_id} for {timeframe.name}")

            if timeframe not in self.tasks:
                self.tasks[timeframe] = asyncio.create_task(self._monitor_timeframe(timeframe))

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer for a timeframe."""
        async with self._observers_lock:
            if timeframe in self.observers and observer_id in self.observers[timeframe]:
                del self.observers[timeframe][observer_id]
                self.info(f"Unregistered observer {observer_id} for {timeframe.name}")

                if not self.observers[timeframe]:
                    await self._stop_monitoring_timeframe(timeframe)

    async def _stop_monitoring_timeframe(self, timeframe: Timeframe):
        """Stops the monitoring task for a specific timeframe."""
        if timeframe in self.tasks:
            self.tasks[timeframe].cancel()
            try:
                await self.tasks[timeframe]
            except asyncio.CancelledError:
                pass
            del self.tasks[timeframe]
            self.info(f"Stopped monitoring {timeframe.name}")

    async def _get_observers_copy(self, timeframe: Timeframe) -> Dict[str, TickObserver]:
        async with self._observers_lock:
            return self.observers.get(timeframe, {}).copy()

    async def _notify_observers(self, timeframe: Timeframe, tick_time: datetime):
        """Simplified notification without complex metrics"""
        observers = await self._get_observers_copy(timeframe)

        for observer_id, observer in observers.items():
            asyncio.create_task(
                self._safe_notify_wrapper(observer_id, observer.callback, timeframe, tick_time)
            )

    async def _auto_unregister(self, observer_id, timeframe):
        """Automatic cleanup with backoff"""
        try:
            await self.unregister_observer(timeframe, observer_id)
            self.info(f"Auto-unregistered {observer_id} due to repeated errors")
        except Exception as e:
            self.error(f"Failed to unregister {observer_id}: {str(e)}", exec_info=e)

    def _handle_failed_observer(self,
                                observer_id: str,
                                timeframe: Timeframe,
                                error: Exception):
        """Handles cleanup for failed observers."""
        self.warning(f"Auto-unregistering failed observer: {observer_id}")
        asyncio.create_task(self.unregister_observer(timeframe, observer_id))

    async def _monitor_timeframe(self, timeframe: Timeframe):
        """Precision timing loop with drift correction and missed tick handling."""
        timeframe_seconds = timeframe.to_seconds()
        current_time = now_utc().timestamp()
        next_tick_time = (int(current_time) // timeframe_seconds) * timeframe_seconds + timeframe_seconds
        sleep_error = 0.0
        MAX_SLEEP_ERROR = 5.0

        while True:
            try:
                current_time = now_utc().timestamp()

                # 1. Calculate safe sleep duration
                sleep_duration = max(0.0, next_tick_time - current_time - sleep_error)

                # Reset timing if large clock drift detected
                if abs(sleep_error) > MAX_SLEEP_ERROR:
                    self.warning(f"Resetting large sleep error ({sleep_error}s) for {timeframe.name}")
                    sleep_error = 0.0
                    next_tick_time = (int(current_time) // timeframe_seconds) * timeframe_seconds + timeframe_seconds
                    sleep_duration = max(0.0, next_tick_time - current_time)

                # 2. Precision timing with two-phase wait
                if sleep_duration > 0.05:  # Coarse sleep for most of the duration
                    await asyncio.sleep(sleep_duration - 0.05)

                # Fine-grained busy wait for last 50ms
                busy_wait_start = time.monotonic()
                while (time.monotonic() - busy_wait_start) < 0.05:
                    await asyncio.sleep(0.001)  # Yield control briefly

                post_sleep_time = now_utc().timestamp()

                # 3. Handle potential missed ticks
                if post_sleep_time >= next_tick_time:
                    missed_ticks = int((post_sleep_time - next_tick_time) // timeframe_seconds)
                    total_ticks = missed_ticks + 1

                    self.info(f"Processing {total_ticks} {timeframe.name} tick(s) at {post_sleep_time}")

                    for i in range(total_ticks):
                        tick_time = datetime.fromtimestamp(
                            next_tick_time + i * timeframe_seconds,
                            tz=timezone.utc
                        )
                        await self._notify_observers(timeframe, tick_time)

                    # 4. Advance next tick time
                    next_tick_time += total_ticks * timeframe_seconds

                    # 5. Update sleep error based on last tick
                    sleep_error = post_sleep_time - (next_tick_time - timeframe_seconds)
                else:
                    # 6. Update error if we didn't trigger notification
                    actual_sleep = post_sleep_time - current_time
                    sleep_error = actual_sleep - sleep_duration

            except asyncio.CancelledError:
                self.info(f"Monitoring task for {timeframe.name} cancelled")
                break
            except Exception as e:
                self.error(f"Critical error in {timeframe.name} monitor: {str(e)}", exc_info=True)
                await asyncio.sleep(min(5, timeframe_seconds))  # Error cooldown

    async def _safe_notify_wrapper(self, observer_id: str, callback: ObserverCallback,
                                   timeframe: Timeframe, tick_time: datetime):
        """Execute callback with concurrency control and timeout handling."""
        async with self.SEMAPHORE:
            observer_timeout = 30.0
            try:
                await asyncio.wait_for(callback(timeframe, tick_time), observer_timeout)
                self.debug(f"Successfully notified {observer_id} for {timeframe.name}")
            except asyncio.TimeoutError:
                self.error(f"Observer {observer_id} timed out after {observer_timeout}s")
                await self._auto_unregister(observer_id, timeframe)
            except Exception as e:
                self.error(f"Error in observer {observer_id}: {str(e)}", exc_info=True)
                await self._auto_unregister(observer_id, timeframe)

    async def shutdown(self):
        """Stops all monitoring tasks."""
        async with self._observers_lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
        self.info("NotifierTickUpdates shutdown completed")
