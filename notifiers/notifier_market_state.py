import asyncio
import time
from typing import Dict, Optional, Callable, Awaitable

from brokers.broker_proxy import Broker
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[str, bool, Optional[float], Optional[float], bool], Awaitable[None]]


class MarketStateObserver:
    """Represents an observer for a symbol's market state."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback
        self.market_open: Optional[bool] = None  # Last known state
        self.market_closed_time: Optional[float] = None
        self.market_opened_time: Optional[float] = None


class NotifierMarketState(LoggingMixin):
    """Singleton class that manages market state monitoring for different symbols."""

    _instance: Optional['NotifierMarketState'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use class_name.get_instance() instead")
        return super().__new__(cls)

    def __init__(self, config: ConfigReader):
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self.config = config

        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._start_lock: asyncio.Lock = asyncio.Lock()

        self.observers: Dict[str, Dict[str, MarketStateObserver]] = {}

        self.agent = "MarketStateManager"
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._polling_interval: float = 60.0
        self._min_sleep_time: float = 0.1  # New: Minimum sleep to prevent tight loop

        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierMarketState':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierMarketState(config)
            return cls._instance

    @exception_handler
    async def register_observer(self, symbol: str, callback: ObserverCallback, observer_id: str):
        """Registers an observer for a symbol's market state."""
        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}
            # Prevent overwriting existing observer without unregistering first (optional but good practice)
            if observer_id in self.observers.get(symbol, {}):
                self.warning(f"Observer {observer_id} for symbol {symbol} already exists. Overwriting.")
            observer = MarketStateObserver(callback)
            self.observers[symbol][observer_id] = observer
            self.info(f"Registered observer {observer_id} for symbol {symbol}")

        # Initial state check and notification - pass is_initial_check=True
        await self._update_and_notify_single_symbol(symbol, observer_id, observer, is_initial_check=True)
        await self.start()  # Ensure monitoring loop is running

    @exception_handler
    async def unregister_observer(self, symbol: str, observer_id: str):
        """Removes an observer for a symbol."""
        async with self._observers_lock:
            if symbol in self.observers and observer_id in self.observers[symbol]:
                del self.observers[symbol][observer_id]
                self.info(f"Unregistered observer {observer_id} for symbol {symbol}")

                if not self.observers[symbol]:
                    del self.observers[symbol]
                    self.info(f"Removed monitoring for symbol {symbol}")

    @exception_handler
    async def start(self):
        """Starts the market state monitoring loop (idempotent)."""
        async with self._start_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.info("Market state monitoring started")

    @exception_handler
    async def stop(self):
        """Stops the market state monitoring loop."""
        async with self._start_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self.info("Market state monitoring stopped")
                    self._task = None

    async def shutdown(self):
        """Stops the monitoring and clears resources."""
        await self.stop()
        async with self._observers_lock:
            self.observers.clear()
            self.info("NotifierMarketState shutdown complete.")

    async def _get_observers_copy(self) -> Dict[str, Dict[str, MarketStateObserver]]:
        """Creates a deep copy of the observers dictionary."""
        async with self._observers_lock:
            return {
                symbol: {obs_id: obs for obs_id, obs in observers.items()}
                for symbol, observers in self.observers.items()
            }

    async def _update_and_notify_single_symbol(self, symbol: str, observer_id: str, observer: MarketStateObserver, is_initial_check: bool = False):
        """Checks and updates the market state for a single symbol and notifies the observer."""
        try:
            market_is_open = await Broker().with_context(f"{symbol}").is_market_open(symbol)
            current_timestamp = now_utc().timestamp()
            previous_market_open = observer.market_open  # Store previous state before potential update

            # Determine if a notification should happen:
            # - If it's the initial check (always notify with current state)
            # - Or if the state has actually changed since the last check
            should_notify = is_initial_check or (previous_market_open is not None and previous_market_open != market_is_open)

            if should_notify:
                state_changed = previous_market_open != market_is_open
                is_first_ever_check = previous_market_open is None

                # Update state and timestamps ONLY if state changed or it's the very first check
                if state_changed or is_first_ever_check:
                    if market_is_open:
                        observer.market_opened_time = current_timestamp
                        observer.market_closed_time = None
                        if state_changed:  # Log only actual changes, not initial state reporting
                            self.info(f"Market status change for symbol {symbol}: Closed -> Opened")
                    else:
                        observer.market_closed_time = current_timestamp
                        observer.market_opened_time = None
                        if state_changed:  # Log only actual changes
                            self.info(f"Market status change for symbol {symbol}: Opened -> Closed")

                    # Update the observer's tracked state *after* determining changes/initial times
                    observer.market_open = market_is_open

                # Call the callback using the potentially updated observer state
                # Pass the is_initial_check flag correctly
                await observer.callback(
                    symbol,
                    observer.market_open,  # Use the current state stored in the observer
                    observer.market_closed_time,
                    observer.market_opened_time,
                    is_initial_check  # Pass the flag correctly
                )

        except Exception as e:
            # Add observer_id to the error message for better debugging
            self.error(f"Error processing symbol {symbol} for observer {observer_id}", exec_info=e)

    async def _notify_observers(self, symbol: str, observers: Dict[str, MarketStateObserver]):
        """Notifies observers for a given symbol about market state changes."""
        notification_tasks = []
        for observer_id, observer in observers.items():
            # Pass is_initial_check=False for regular updates from the monitor loop
            notification_tasks.append(
                self._update_and_notify_single_symbol(symbol, observer_id, observer, is_initial_check=False)
            )

        if notification_tasks:
            results = await asyncio.gather(*notification_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Get observer_id corresponding to the failed task
                    failed_observer_id = list(observers.keys())[i]
                    self.error(f"Error in observer callback for {symbol}/{failed_observer_id}: {result}", exec_info=result)

    async def _monitor_loop(self):
        """Main monitoring loop with improved timing logic."""
        next_wake = time.monotonic()
        while self._running:
            try:
                # Process all symbols
                observers_copy = await self._get_observers_copy()
                for symbol, observers in observers_copy.items():
                    await self._notify_observers(symbol, observers)

                now = time.monotonic()
                sleep_duration = max(
                    self._min_sleep_time,
                    next_wake - now  # Could be negative, so use min_sleep_time
                )
                await asyncio.sleep(sleep_duration)

                # Maintain fixed interval schedule
                next_wake += self._polling_interval

            except Exception as e:
                self.error("Error in market state monitor loop", exec_info=e)
                # Reset timing to prevent error cascade
                next_wake = time.monotonic() + self._polling_interval
                await asyncio.sleep(5)  # Emergency cooldown