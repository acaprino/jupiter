import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Callable, Awaitable, Optional

from misc_utils.config import ConfigReader
from misc_utils.enums import Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc # Assuming now_utc returns naive UTC datetime

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
    Provides improved precision and robustness.
    """
    _instance: Optional['NotifierTickUpdates'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()
    # Semaphore to limit concurrent observer callback executions
    tasks_semaphore = asyncio.Semaphore(100)

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use NotifierTickUpdates.get_instance() to obtain the instance")
        return super().__new__(cls)

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierTickUpdates':
        """Gets the singleton instance of the NotifierTickUpdates."""
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierTickUpdates(config)
            return cls._instance

    def __init__(self, config: ConfigReader):
        """Initializes the NotifierTickUpdates instance."""
        # Prevent re-initialization if already done
        if getattr(self, "_initialized", False):
            return

        super().__init__(config)
        self._observers_lock = asyncio.Lock()
        # Dictionary structure: {Timeframe: {observer_id: TickObserver}}
        self.observers: Dict[Timeframe, Dict[str, TickObserver]] = {}
        # Dictionary to hold monitoring tasks: {Timeframe: asyncio.Task}
        self.tasks: Dict[Timeframe, asyncio.Task] = {}
        self.config = config
        self.agent = "NotifierTickUpdates"
        # Maximum number of observers allowed per timeframe
        self._max_observers = 100
        self._initialized = True

    def _get_last_completed_tick_time(self, timeframe: Timeframe) -> datetime:
        """
        Calculates the naive UTC datetime of the most recently completed timeframe tick.
        """
        # Get current naive UTC timestamp
        now_ts = now_utc().timestamp()
        timeframe_seconds = timeframe.to_seconds()
        # Calculate the timestamp of the start of the current interval,
        # which is the end/close time of the last completed interval.
        last_tick_ts = (int(now_ts) // timeframe_seconds) * timeframe_seconds
        # Convert UNIX timestamp back to naive UTC datetime
        return datetime.fromtimestamp(last_tick_ts, tz=timezone.utc).replace(tzinfo=None)

    @exception_handler
    async def register_observer(self,
                                timeframe: Timeframe,
                                callback: ObserverCallback,
                                observer_id: str):
        """
        Registers a new observer for a timeframe and triggers an initial notification
        with the last completed tick time. Starts the monitoring task if not running.
        """
        async with self._observers_lock:
            # Initialize observer dictionary for the timeframe if it doesn't exist
            if timeframe not in self.observers:
                self.observers[timeframe] = {}

            # Check if the maximum number of observers for this timeframe has been reached
            if len(self.observers[timeframe]) >= self._max_observers:
                self.error(f"Cannot register observer {observer_id}: Maximum observer limit ({self._max_observers}) reached for timeframe {timeframe.name}")
                raise RuntimeError(f"Too many observers for timeframe {timeframe.name}")

            # Create and store the new observer
            new_observer = TickObserver(callback)
            self.observers[timeframe][observer_id] = new_observer
            self.info(f"Registered observer {observer_id} for {timeframe.name}")

            # --- Initial Notification Logic ---
            try:
                # Calculate the timestamp of the last completed tick
                last_tick_time = self._get_last_completed_tick_time(timeframe)
                self.info(f"Triggering initial notification for {observer_id} ({timeframe.name}) with last tick time: {last_tick_time}")
                # Schedule the initial notification using the safe wrapper
                asyncio.create_task(
                    self._safe_notify_wrapper(observer_id, new_observer.callback, timeframe, last_tick_time)
                )
            except Exception as e:
                # Log an error if the initial notification fails, but continue registration
                self.error(f"Failed to trigger initial notification for {observer_id} ({timeframe.name})", exc_info=e)
            # --- End Initial Notification Logic ---

            # Start the monitoring task for this timeframe if it's not already running or has finished
            if timeframe not in self.tasks or self.tasks[timeframe].done():
                 # Check if the previous task for this timeframe ended with an error
                if timeframe in self.tasks and self.tasks[timeframe].done():
                     try:
                         exc = self.tasks[timeframe].exception()
                         if exc:
                             self.warning(f"Restarting monitor task for {timeframe.name} after previous failure: {exc}")
                     except (asyncio.InvalidStateError, asyncio.CancelledError):
                         # Ignore if task is not done or was cancelled
                         pass

                self.info(f"Starting monitor task for {timeframe.name}")
                self.tasks[timeframe] = asyncio.create_task(self._monitor_timeframe(timeframe))
            else:
                # Log if the task is already running
                self.debug(f"Monitor task for {timeframe.name} is already running.")

    @exception_handler
    async def unregister_observer(self, timeframe: Timeframe, observer_id: str):
        """Removes an observer for a timeframe and stops monitoring if no observers remain."""
        async with self._observers_lock:
            # Check if the timeframe and observer exist
            if timeframe in self.observers and observer_id in self.observers[timeframe]:
                # Remove the observer
                del self.observers[timeframe][observer_id]
                self.info(f"Unregistered observer {observer_id} for {timeframe.name}")

                # If no observers are left for this timeframe, stop the monitoring task
                if not self.observers[timeframe]:
                    # Remove the timeframe entry from observers dict
                    del self.observers[timeframe]
                    self.info(f"No observers left for {timeframe.name}. Stopping monitoring.")
                    await self._stop_monitoring_timeframe(timeframe)
            else:
                # Log a warning if trying to unregister a non-existent observer
                self.warning(f"Observer {observer_id} not found for timeframe {timeframe.name} during unregistration.")

    async def _stop_monitoring_timeframe(self, timeframe: Timeframe):
        """Stops the monitoring task for a specific timeframe."""
        # Check if a task exists for the timeframe
        if timeframe in self.tasks:
            task = self.tasks[timeframe]
            # Cancel the task if it's not already done
            if not task.done():
                task.cancel()
                try:
                    # Wait for the cancellation to complete
                    await task
                except asyncio.CancelledError:
                    # Expected exception upon cancellation
                    self.info(f"Monitoring task for {timeframe.name} cancelled successfully.")
                except Exception as e:
                    # Log any other exceptions during cancellation
                    self.error(f"Error waiting for task cancellation for {timeframe.name}", exc_info=e)
            # Remove the task from the dictionary
            del self.tasks[timeframe]
            self.info(f"Stopped monitoring {timeframe.name}")

    async def _get_observers_copy(self, timeframe: Timeframe) -> Dict[str, TickObserver]:
        """Returns a copy of the observers for a given timeframe for safe iteration."""
        async with self._observers_lock:
            # Return a copy to avoid modification issues during iteration
            return self.observers.get(timeframe, {}).copy()

    async def _notify_observers(self, timeframe: Timeframe, tick_time: datetime):
        """Notifies all registered observers for a given timeframe about a new tick."""
        # Get a safe copy of the observers for this timeframe
        observers = await self._get_observers_copy(timeframe)
        self.debug(f"Notifying {len(observers)} observers for {timeframe.name} tick at {tick_time}")

        # Schedule notification tasks for each observer
        for observer_id, observer in observers.items():
            asyncio.create_task(
                self._safe_notify_wrapper(observer_id, observer.callback, timeframe, tick_time)
            )

    async def _safe_notify_wrapper(self, observer_id: str, callback: ObserverCallback,
                                   timeframe: Timeframe, tick_time: datetime):
        """
        Executes an observer's callback safely within a semaphore lock and handles timeouts/errors.
        Automatically unregisters the observer on persistent failures or timeouts.
        """
        # Acquire the semaphore to limit concurrency
        async with self.tasks_semaphore:
            # Define a timeout for observer execution
            observer_timeout = 30.0
            try:
                self.debug(f"Executing callback for observer {observer_id} ({timeframe.name})")
                # Execute the callback with a timeout
                await asyncio.wait_for(callback(timeframe, tick_time), observer_timeout)
                self.debug(f"Successfully notified {observer_id} for {timeframe.name}")
            except asyncio.TimeoutError:
                # Log timeout error and trigger auto-unregistration
                self.error(f"Observer {observer_id} ({timeframe.name}) timed out after {observer_timeout}s. Auto-unregistering.")
                await self._auto_unregister(observer_id, timeframe)
            except Exception as e:
                # Log any other error during callback execution and trigger auto-unregistration
                self.error(f"Error in observer {observer_id} ({timeframe.name}): {str(e)}. Auto-unregistering.", exc_info=True)
                await self._auto_unregister(observer_id, timeframe)

    async def _auto_unregister(self, observer_id: str, timeframe: Timeframe):
        """Attempts to automatically unregister a problematic observer."""
        try:
            # Call the standard unregister method
            await self.unregister_observer(timeframe, observer_id)
            self.info(f"Auto-unregistered observer {observer_id} ({timeframe.name}) due to errors or timeout.")
        except Exception as e:
            # Log failure during the auto-unregister process
            self.error(f"Failed to auto-unregister observer {observer_id} ({timeframe.name}): {str(e)}", exc_info=e)

    async def _monitor_timeframe(self, timeframe: Timeframe):
        """
        The main monitoring loop for a specific timeframe. Calculates the next tick time,
        sleeps until then, and notifies observers. Handles potential missed ticks.
        """
        timeframe_seconds = timeframe.to_seconds()
        self.info(f"Monitor loop started for {timeframe.name} (interval: {timeframe_seconds}s)")

        while True:
            try:
                # Get current time (monotonic for sleep, real for calculation)
                current_mono = time.monotonic()
                current_real_ts = now_utc().timestamp() # Use naive UTC timestamp

                # Calculate the timestamp of the next tick's closing time
                next_tick_time_ts = ((int(current_real_ts) // timeframe_seconds) + 1) * timeframe_seconds

                # Calculate the sleep duration needed to reach the next tick time
                # using monotonic clock for accurate sleep interval
                sleep_duration = max(0, (next_tick_time_ts - current_real_ts))
                mono_deadline = current_mono + sleep_duration

                # Sleep until the calculated deadline using monotonic clock
                await asyncio.sleep(max(0.0, mono_deadline - time.monotonic()))

                # --- Post-Sleep Check and Notification ---
                # Check current time again after waking up
                now_ts_after_sleep = now_utc().timestamp()
                # Calculate how much time has passed since the intended wake-up time
                elapsed_since_intended_tick = now_ts_after_sleep - next_tick_time_ts

                num_ticks_to_notify = 0
                if elapsed_since_intended_tick >= -1.0: # Allow a small tolerance (e.g., 1 second early)
                     # Calculate how many full intervals have passed since the intended tick time
                     # Add 1 to include the tick we just reached
                    num_ticks_to_notify = int(elapsed_since_intended_tick // timeframe_seconds) + 1
                else:
                    # Woke up significantly earlier than expected, log and recalculate
                    self.warning(f"Woke up too early for {timeframe.name}? elapsed={elapsed_since_intended_tick:.2f}s. Recalculating sleep.")
                    await asyncio.sleep(0.1) # Short sleep before retry
                    continue # Skip notification and recalculate in the next loop iteration

                # Log if missed ticks are detected
                if num_ticks_to_notify > 1:
                    self.warning(f"Detected {num_ticks_to_notify - 1} missed tick(s) for {timeframe.name}. Notifying for all.")
                elif num_ticks_to_notify == 0:
                    self.warning(f"Calculated 0 ticks to notify for {timeframe.name}. This should not happen often. Skipping notification.")
                    continue


                # Notify for each tick that should have occurred
                for i in range(num_ticks_to_notify):
                    # Calculate the exact naive UTC timestamp for this notification
                    notify_ts = next_tick_time_ts + i * timeframe_seconds
                    tick_time_naive_utc = datetime.fromtimestamp(notify_ts, tz=timezone.utc).replace(tzinfo=None)

                    self.debug(f"Notifying observers for {timeframe.name} tick at {tick_time_naive_utc}")
                    # Trigger notifications for this specific tick time
                    await self._notify_observers(timeframe, tick_time_naive_utc)

            except asyncio.CancelledError:
                # Exit loop cleanly if the task is cancelled
                self.info(f"Monitoring task for {timeframe.name} cancelled.")
                break
            except Exception as e:
                # Log unexpected errors and wait before retrying
                self.error(f"Error in monitor loop for {timeframe.name}: {e}", exc_info=e)
                # Wait for a short period before the next attempt to avoid tight error loops
                await asyncio.sleep(min(5, timeframe_seconds / 2))

    async def shutdown(self):
        """Stops all monitoring tasks and clears observer lists."""
        self.info("Shutting down NotifierTickUpdates...")
        tasks_to_await = []
        async with self._observers_lock:
            # Cancel all running monitoring tasks
            for timeframe, task in self.tasks.items():
                if not task.done():
                    task.cancel()
                    tasks_to_await.append(task)
            # Wait for all tasks to complete cancellation
            if tasks_to_await:
                await asyncio.gather(*tasks_to_await, return_exceptions=True)

            # Clear internal state
            self.tasks.clear()
            self.observers.clear()
        self.info("NotifierTickUpdates shutdown completed.")
