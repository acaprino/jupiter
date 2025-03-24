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

    async def _safe_notify_wrapper(self, observer_id, callback, timeframe, tick_time):
        """Simplified wrapper with essential logging"""
        try:
            start = time.monotonic()
            await callback(timeframe, tick_time)
            self.debug(f"Observer {observer_id} notified in {time.monotonic() - start:.2f}s")
        except asyncio.CancelledError as c:
            self.warning(f"Notification to {observer_id} cancelled", exec_info=c)
        except Exception as e:
            self.error(f"Error in {observer_id}: {str(e)}", exec_info=e)
            await self._auto_unregister(observer_id, timeframe)

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
        """Monitoring loop for a specific timeframe with precise tick alignment."""
        timeframe_seconds = timeframe.to_seconds()
        current_time = now_utc().timestamp()
        # Align to the next tick boundary
        next_tick_time = (int(current_time) // timeframe_seconds) * timeframe_seconds + timeframe_seconds
        sleep_error = 0.0  # Track sleep deviations per timeframe

        while True:
            try:
                current_time = now_utc().timestamp()
                sleep_duration = next_tick_time - current_time - sleep_error
                await asyncio.sleep(sleep_duration)

                # Calculate actual sleep time and adjust error
                post_sleep_time = now_utc().timestamp()
                actual_sleep = post_sleep_time - current_time
                sleep_error = actual_sleep - sleep_duration  # Positive if overslept

                if post_sleep_time >= next_tick_time - 0.001:  # Tolerance for microsecond precision
                    tick_time = datetime.fromtimestamp(next_tick_time, tz=timezone.utc)
                    self.info(f"New tick for {timeframe.name} at {tick_time}.")
                    await self._notify_observers(timeframe, tick_time)

                    # Schedule next tick
                    next_tick_time += timeframe_seconds
                else:
                    # Handle cases where system time changed or sleep was interrupted early
                    # Re-align to the next tick
                    next_tick_time = (int(post_sleep_time) // timeframe_seconds) * timeframe_seconds + timeframe_seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.error(f"Error in {timeframe.name} monitor: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on persistent errors

    async def shutdown(self):
        """Stops all monitoring tasks."""
        async with self._observers_lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
        self.info("NotifierTickUpdates shutdown completed")
