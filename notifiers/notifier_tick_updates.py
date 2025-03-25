import asyncio
import time
from datetime import datetime, timezone
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
    tasks_semaphore = asyncio.Semaphore(100)  # Max task concorrenti

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use class_name.get_instance() instead")
        return super().__new__(cls)

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
        timeframe_seconds = timeframe.to_seconds()

        while True:
            try:
                current_real = now_utc().timestamp()
                # Calcola prossimo tick basato sul tempo corrente
                next_tick_time = ((int(current_real) // timeframe_seconds) + 1) * timeframe_seconds
                current_mono = time.monotonic()

                # Converti a deadline monotonic
                mono_deadline = current_mono + (next_tick_time - current_real)

                await asyncio.sleep(max(0, mono_deadline - time.monotonic()))

                # Verifica eventuali ticks persi
                elapsed = now_utc().timestamp() - next_tick_time
                num_ticks = int(elapsed // timeframe_seconds) + 1 if elapsed >= 0 else 0

                for i in range(num_ticks):
                    tick_time = datetime.fromtimestamp(next_tick_time + i * timeframe_seconds, tz=timezone.utc)
                    await self._notify_observers(timeframe, tick_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.error(f"Error: {e}", exec_info=e)
                await asyncio.sleep(min(5, timeframe_seconds))

    async def _safe_notify_wrapper(self, observer_id: str, callback: ObserverCallback,
                                   timeframe: Timeframe, tick_time: datetime):
        """Execute callback with concurrency control and timeout handling."""
        async with self.tasks_semaphore:
            observer_timeout = 30.0
            try:
                await asyncio.wait_for(callback(timeframe, tick_time), observer_timeout)
                self.debug(f"Successfully notified {observer_id} for {timeframe.name}")
            except asyncio.TimeoutError:
                self.error(f"Observer {observer_id} timed out after {observer_timeout}s")
                await self._auto_unregister(observer_id, timeframe)
            except Exception as e:
                self.error(f"Error in observer {observer_id}: {str(e)}", exec_info=True)
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
