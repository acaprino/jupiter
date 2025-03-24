import asyncio
from datetime import datetime, timedelta
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

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader):
        if getattr(self, "_initialized", False):
            return

        super().__init__(config)
        self._observers_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()  # Lock for config access
        self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}
        self.tasks: Dict[Timeframe, asyncio.Task] = {}
        self.config = config
        self.agent = "NotifierTickUpdates"
        self._min_sleep_time: float = 0.001  # Default to 1ms
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

        # Input Validation
        if not isinstance(timeframe, Timeframe):
            raise TypeError("timeframe must be a Timeframe enum")
        if not callable(callback):
            raise TypeError("callback must be a callable")
        if not isinstance(observer_id, str) or not observer_id:
            raise ValueError("observer_id must be a non-empty string")

        async with self._observers_lock:
            if timeframe not in self.observers:
                self.observers[timeframe] = {}

            if len(self.observers[timeframe]) >= self._max_observers:
                raise RuntimeError(f"Too many observers for timeframe {timeframe.name}")

            self.observers[timeframe][observer_id] = TickObserver(callback)
            self.info(f"Registered observer {observer_id} for timeframe {timeframe.name}")

            if timeframe not in self.tasks:
                self.tasks[timeframe] = asyncio.create_task(self._monitor_timeframe(timeframe))
                self.info(f"Started monitoring for timeframe {timeframe.name}")

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer for a timeframe."""
        await self._remove_observer_and_cleanup(timeframe, observer_id)

    async def _remove_observer_and_cleanup(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer and stops the monitoring task if no observers remain."""
        async with self._observers_lock:
            if timeframe in self.observers and observer_id in self.observers[timeframe]:
                del self.observers[timeframe][observer_id]
                self.info(f"Unregistered observer {observer_id} for timeframe {timeframe.name}")

                if not self.observers[timeframe]:
                    await self._stop_monitoring_timeframe(timeframe)

    async def _stop_monitoring_timeframe(self, timeframe: Timeframe):
        """Stops the monitoring task for a specific timeframe."""
        del self.observers[timeframe]
        if timeframe in self.tasks:
            self.tasks[timeframe].cancel()
            try:
                await self.tasks[timeframe]
            except asyncio.CancelledError:
                pass
            self.info(f"Stopped monitoring for timeframe {timeframe.name}")
            del self.tasks[timeframe]

    async def _get_observers_copy(self, timeframe: Timeframe) -> Dict[str, TickObserver]:
        """Gets a copy of the observers for a given timeframe."""
        async with self._observers_lock:
            return self.observers.get(timeframe, {}).copy()

    async def _notify_observers(self, timeframe: Timeframe, tick_time: datetime):
        """Notifies all observers for a given timeframe, with improved error handling."""
        observers = await self._get_observers_copy(timeframe)

        notification_tasks = []
        for observer_id, observer in observers.items():
            try:
                notification_tasks.append(observer.callback(timeframe, tick_time))
            except Exception as e:
                self.error(f"Error in observer callback for {observer_id} on {timeframe.name}: {e}")
                # Consider: Automatically unregistering the observer, or retrying.
                # await self.unregister_observer(timeframe, observer_id)  # Example: Auto-unregister

        if notification_tasks:
            results = await asyncio.gather(*notification_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    observer_id = list(observers.keys())[i]  # Get observer_id
                    self.error(f"Exception during notification of {observer_id}: {result}")

    async def _calculate_sleep_time(self, timeframe_seconds: int) -> float:
        """Calculates the time to sleep until the next tick, with jitter compensation."""
        current_time = now_utc()
        current_timestamp = int(current_time.timestamp())
        remainder = current_timestamp % timeframe_seconds
        sleep_seconds: float = timeframe_seconds - remainder if remainder != 0 else 0

        # Jitter Compensation
        if hasattr(self, '_last_tick_time') and hasattr(self, '_sleep_error'):
            expected_tick_time = self._last_tick_time + timedelta(seconds=timeframe_seconds)
            actual_error = (current_time - expected_tick_time).total_seconds()
            self._sleep_error = self._sleep_error * 0.9 + actual_error * 0.1  # Exponential moving average
            sleep_seconds -= self._sleep_error  # Apply correction

        return max(sleep_seconds, self._min_sleep_time)

    async def _monitor_timeframe(self, timeframe: Timeframe):
        """Monitoring loop for a specific timeframe."""
        self._sleep_error = 0.0  # Initialize sleep error
        try:
            timeframe_seconds = timeframe.to_seconds()
            while True:
                sleep_seconds = await self._calculate_sleep_time(timeframe_seconds)
                await asyncio.sleep(sleep_seconds)

                tick_time = now_utc()
                self._last_tick_time = tick_time  # Store the actual tick time
                self.info(f"New tick for {timeframe.name}.")

                await self._notify_observers(timeframe, tick_time)

        except asyncio.CancelledError:
            pass  # Expected on shutdown
        except Exception as e:
            self.error(f"Error in timeframe monitor loop for {timeframe.name}: {e}")

    async def shutdown(self):
        """Stops all monitoring tasks and cleans up resources."""
        async with self._observers_lock:
            for timeframe, task in self.tasks.items():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
        self.info("TickManager shutdown completed")
