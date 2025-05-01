# notifiers/notifier_tick_updates.py

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Callable, Awaitable, Optional

from brokers.broker_proxy import Broker # Needed to get broker offset

from misc_utils.config import ConfigReader
from misc_utils.enums import Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc

# Type alias for the observer callback function
ObserverCallback = Callable[[Timeframe, datetime], Awaitable[None]]

class TickObserver:
    """Represents an observer for a tick of a specific timeframe."""
    def __init__(self, callback: ObserverCallback):
        self.callback = callback

class NotifierTickUpdates(LoggingMixin):
    """
    Singleton class that manages tick notifications for different timeframes.
    It notifies observers upon registration with the last completed tick
    and subsequently at the end of each timeframe interval.
    Provides improved precision and robustness, accounting for broker timezone.
    This version only notifies for the single most recently completed tick.
    """
    _instance: Optional['NotifierTickUpdates'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()
    tasks_semaphore = asyncio.Semaphore(100)

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            raise RuntimeError("Use NotifierTickUpdates.get_instance() to obtain the instance")
        return super().__new__(cls)

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierTickUpdates':
        """Gets the singleton instance of the NotifierTickUpdates."""
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierTickUpdates(config)
                # No need to initialize attributes here, it's done in __init__
            return cls._instance

    def __init__(self, config: ConfigReader):
        """Initializes the NotifierTickUpdates instance."""
        if getattr(self, "_initialized", False):
            return

        super().__init__(config)
        self._observers_lock = asyncio.Lock()
        self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}
        self.tasks: Dict[Timeframe, asyncio.Task] = {}
        self.config = config
        self.agent = "NotifierTickUpdates"
        self._max_observers = 100
        # Attributes for broker offset cache are correctly initialized here
        self._broker_offset_cache: Optional[int] = None
        self._broker_offset_last_checked: Optional[float] = None
        self._broker_offset_cache_ttl: float = 3600.0 # Cache offset for 1 hour
        self.last_notified_times: Dict[Timeframe, Optional[datetime]] = {}
        # Define interval_seconds, used primarily for error recovery sleep
        self.interval_seconds: float = 60.0 # Default retry interval if offset fails
        self._initialized = True

    async def _get_broker_offset(self) -> Optional[int]:
        """Fetches the broker timezone offset, using a cache."""
        now = time.monotonic()
        if (self._broker_offset_cache is None or
                self._broker_offset_last_checked is None or
                (now - self._broker_offset_last_checked > self._broker_offset_cache_ttl)):
            try:
                broker_instance = Broker().with_context("*")
                if not broker_instance.is_initialized:
                     self.warning("Broker not yet initialized when trying to get offset.")
                     return self._broker_offset_cache

                offset = await broker_instance.get_broker_timezone_offset()
                if offset is not None:
                    self._broker_offset_cache = offset
                    self._broker_offset_last_checked = now
                    self.info(f"Retrieved and cached broker offset: {offset} hours")
                else:
                    self.error("Failed to retrieve broker offset (get_broker_timezone_offset returned None). Using last cached value if available.")
            except Exception as e:
                self.error("Error fetching broker offset. Using last cached value if available.", exc_info=e)
        else:
             self.debug(f"Using cached broker offset: {self._broker_offset_cache}")

        return self._broker_offset_cache

    async def _get_last_completed_tick_time(self, timeframe: Timeframe) -> Optional[datetime]:
        """
        Calculates the naive UTC datetime of the most recently completed timeframe tick
        in the BROKER'S timezone, then converts it back to UTC.
        Returns None if broker offset cannot be determined.
        """
        broker_offset = await self._get_broker_offset()
        if broker_offset is None:
            self.error(f"Cannot determine broker offset for {timeframe.name}. Cannot calculate last tick time.")
            return None

        now_utc_dt = now_utc()
        broker_now_dt = now_utc_dt + timedelta(hours=broker_offset)
        broker_now_ts = broker_now_dt.timestamp()

        timeframe_seconds = timeframe.to_seconds()
        last_broker_tick_ts = (int(broker_now_ts) // timeframe_seconds) * timeframe_seconds
        last_utc_tick_ts = last_broker_tick_ts - (broker_offset * 3600)

        return datetime.fromtimestamp(last_utc_tick_ts, tz=timezone.utc).replace(tzinfo=None)

    @exception_handler
    async def register_observer(self,
                                timeframe: Timeframe,
                                callback: ObserverCallback,
                                observer_id: str):
        """
        Registers a new observer and triggers initial notification.
        """
        async with self._observers_lock:
            if timeframe not in self.observers:
                self.observers[timeframe] = {}

            if len(self.observers[timeframe]) >= self._max_observers:
                self.error(f"Cannot register observer {observer_id}: Maximum observer limit ({self._max_observers}) reached for timeframe {timeframe.name}")
                raise RuntimeError(f"Too many observers for timeframe {timeframe.name}")

            new_observer = TickObserver(callback)
            self.observers[timeframe][observer_id] = new_observer
            self.info(f"Registered observer {observer_id} for {timeframe.name}")

            try:
                last_tick_time = await self._get_last_completed_tick_time(timeframe)
                if last_tick_time is not None:
                    self.info(f"Triggering initial notification for {observer_id} ({timeframe.name}) with last tick time: {last_tick_time}")
                    self.last_notified_times[timeframe] = last_tick_time
                    asyncio.create_task(
                        self._safe_notify_wrapper(observer_id, new_observer.callback, timeframe, last_tick_time)
                    )
                else:
                    self.error(f"Could not trigger initial notification for {observer_id} ({timeframe.name}) because broker offset is unknown.")
            except Exception as e:
                self.error(f"Failed to trigger initial notification for {observer_id} ({timeframe.name})", exc_info=e)

            if timeframe not in self.tasks or self.tasks[timeframe].done():
                if timeframe in self.tasks and self.tasks[timeframe].done():
                     try:
                         exc = self.tasks[timeframe].exception()
                         if exc:
                             self.warning(f"Restarting monitor task for {timeframe.name} after previous failure: {exc}")
                     except (asyncio.InvalidStateError, asyncio.CancelledError):
                         pass
                self.info(f"Starting monitor task for {timeframe.name}")
                self.tasks[timeframe] = asyncio.create_task(self._monitor_timeframe(timeframe))
            else:
                self.debug(f"Monitor task for {timeframe.name} is already running.")

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer for a timeframe and stops monitoring if no observers remain."""
        async with self._observers_lock:
            if timeframe in self.observers and observer_id in self.observers[timeframe]:
                del self.observers[timeframe][observer_id]
                self.info(f"Unregistered observer {observer_id} for {timeframe.name}")
                if not self.observers[timeframe]:
                    del self.observers[timeframe]
                    self.info(f"No observers left for {timeframe.name}. Stopping monitoring.")
                    await self._stop_monitoring_timeframe(timeframe)
            else:
                self.warning(f"Observer {observer_id} not found for timeframe {timeframe.name} during unregistration.")

    async def _stop_monitoring_timeframe(self, timeframe: Timeframe):
        """Stops the monitoring task for a specific timeframe."""
        if timeframe in self.tasks:
            task = self.tasks[timeframe]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    self.info(f"Monitoring task for {timeframe.name} cancelled successfully.")
                except Exception as e:
                    self.error(f"Error waiting for task cancellation for {timeframe.name}", exc_info=e)
            del self.tasks[timeframe]
            self.info(f"Stopped monitoring {timeframe.name}")
            self.last_notified_times.pop(timeframe, None) # Clean up tracking

    async def _get_observers_copy(self, timeframe: Timeframe) -> Dict[str, TickObserver]:
        """Returns a copy of the observers for a given timeframe for safe iteration."""
        async with self._observers_lock:
            return self.observers.get(timeframe, {}).copy()

    async def _notify_observers(self, timeframe: Timeframe, tick_time: datetime):
        """Notifies all registered observers for a given timeframe about a new tick."""
        observers = await self._get_observers_copy(timeframe)
        self.debug(f"Notifying {len(observers)} observers for {timeframe.name} tick at {tick_time}")
        for observer_id, observer in observers.items():
            asyncio.create_task(
                self._safe_notify_wrapper(observer_id, observer.callback, timeframe, tick_time)
            )

    async def _safe_notify_wrapper(self, observer_id: str, callback: ObserverCallback,
                                   timeframe: Timeframe, tick_time: datetime):
        """
        Executes an observer's callback safely within a semaphore lock and handles timeouts/errors.
        """
        async with self.tasks_semaphore:
            observer_timeout = 30.0
            try:
                self.debug(f"Executing callback for observer {observer_id} ({timeframe.name})")
                await asyncio.wait_for(callback(timeframe, tick_time), observer_timeout)
                self.debug(f"Successfully notified {observer_id} for {timeframe.name}")
            except asyncio.TimeoutError:
                self.error(f"Observer {observer_id} ({timeframe.name}) timed out after {observer_timeout}s. Auto-unregistering.")
                await self._auto_unregister(observer_id, timeframe)
            except Exception as e:
                self.error(f"Error in observer {observer_id} ({timeframe.name}): {str(e)}. Auto-unregistering.", exc_info=True)
                await self._auto_unregister(observer_id, timeframe)

    async def _auto_unregister(self, observer_id: str, timeframe: Timeframe):
        """Attempts to automatically unregister a problematic observer."""
        try:
            await self.unregister_observer(timeframe, observer_id)
            self.info(f"Auto-unregistered observer {observer_id} ({timeframe.name}) due to errors or timeout.")
        except Exception as e:
            self.error(f"Failed to auto-unregister observer {observer_id} ({timeframe.name}): {str(e)}", exc_info=e)

    async def _monitor_timeframe(self, timeframe: Timeframe):
        """
        Main monitoring loop. Calculates next broker tick time, sleeps,
        then notifies ONLY for the single most recently completed tick.
        """
        timeframe_seconds = timeframe.to_seconds()
        self.info(f"Monitor loop started for {timeframe.name} (interval: {timeframe_seconds}s), considering broker timezone. Will notify only last tick.")

        while True: # Loop indefinitely until cancelled
            try:
                # --- Calculate NEXT wake-up time (based on broker timezone) ---
                broker_offset = await self._get_broker_offset()
                if broker_offset is None:
                    self.error(f"Cannot monitor {timeframe.name}: broker offset is unknown. Retrying in {self.interval_seconds}s...")
                    await asyncio.sleep(self.interval_seconds)
                    continue

                current_real_ts = now_utc().timestamp()
                broker_now_ts = current_real_ts + (broker_offset * 3600)
                next_broker_tick_ts = ((int(broker_now_ts) // timeframe_seconds) + 1) * timeframe_seconds
                next_utc_target_ts = next_broker_tick_ts - (broker_offset * 3600)
                # -------------------------------------------------------------

                # --- Sleep until the next target time ---
                sleep_duration = max(0, next_utc_target_ts - current_real_ts)
                current_mono = time.monotonic()
                mono_deadline = current_mono + sleep_duration

                if sleep_duration > 0:
                    self.debug(f"Sleeping for {sleep_duration:.2f}s until next broker tick for {timeframe.name} at UTC {datetime.fromtimestamp(next_utc_target_ts, tz=timezone.utc).replace(tzinfo=None)}")
                    await asyncio.sleep(max(0.0, mono_deadline - time.monotonic()))
                else:
                    self.debug(f"Next tick time for {timeframe.name} already passed or now. Proceeding immediately.")

                # --- After waking up, determine the MOST RECENT completed tick ---
                last_tick_time_to_notify = await self._get_last_completed_tick_time(timeframe)

                if last_tick_time_to_notify is None:
                    self.warning(f"Could not determine last tick time for {timeframe.name} after waking up. Skipping notification.")
                    continue # Try again next cycle

                # --- Check if this tick time was already notified ---
                last_notified = self.last_notified_times.get(timeframe)

                if last_notified is not None and last_tick_time_to_notify <= last_notified:
                    self.debug(f"Tick time {last_tick_time_to_notify} for {timeframe.name} is not newer than last notified ({last_notified}). Skipping.")
                else:
                    # --- Notify for this single, most recent tick ---
                    self.debug(f"Notifying observers for {timeframe.name} tick at {last_tick_time_to_notify} (most recent completed)")
                    await self._notify_observers(timeframe, last_tick_time_to_notify)
                    # --- Update last notified time ---
                    self.last_notified_times[timeframe] = last_tick_time_to_notify

            except asyncio.CancelledError:
                # Exit loop cleanly if the task is cancelled
                self.info(f"Monitoring task for {timeframe.name} cancelled.")
                break
            except Exception as e:
                # Log unexpected errors and wait before retrying
                self.error(f"Error in monitor loop for {timeframe.name}: {e}", exc_info=True)
                # Use the defined interval_seconds for error recovery sleep
                await asyncio.sleep(min(self.interval_seconds, timeframe_seconds / 2))

    async def shutdown(self):
        """Stops all monitoring tasks and clears observer lists."""
        self.info("Shutting down NotifierTickUpdates...")
        tasks_to_await = []
        async with self._observers_lock:
            for timeframe, task in self.tasks.items():
                if not task.done():
                    task.cancel()
                    tasks_to_await.append(task)
            if tasks_to_await:
                await asyncio.gather(*tasks_to_await, return_exceptions=True)
            self.tasks.clear()
            self.observers.clear()
            self.last_notified_times.clear()
        self.info("NotifierTickUpdates shutdown completed.")