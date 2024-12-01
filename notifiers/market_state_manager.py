import asyncio
import threading
import json
from typing import Dict, Optional, Callable, Awaitable, List
from datetime import datetime

from brokers.broker_interface import BrokerAPI
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[bool, Optional[float], Optional[float], bool], Awaitable[None]]


class MarketStateObserver:
    """Represents an observer for a symbol's market state."""

    def __init__(self, callback: ObserverCallback):
        self.callback = callback
        self.market_open: Optional[bool] = None  # Last known state
        self.market_closed_time: Optional[float] = None
        self.market_opened_time: Optional[float] = None
        self.session_active: Optional[bool] = None  # Last known session state


class MarketStateManager:
    """Singleton class that manages market state monitoring for different symbols."""

    _instance: Optional['MarketStateManager'] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'MarketStateManager':
        with cls._instance_lock:
            if cls._instance is None:
                instance = super(MarketStateManager, cls).__new__(cls)
                instance.__initialized = False
                cls._instance = instance
            return cls._instance

    def __init__(self):
        if not getattr(self, '__initialized', False):
            # Locks to protect shared resources
            self._observers_lock: asyncio.Lock = asyncio.Lock()
            self._state_lock: asyncio.Lock = asyncio.Lock()

            # Dictionary of observers: {symbol: {observer_id: MarketStateObserver}}
            self.observers: Dict[str, Dict[str, MarketStateObserver]] = {}

            self.logger = BotLogger.get_logger("MarketStateManager")

            self._running: bool = False
            self._task: Optional[asyncio.Task] = None
            self.broker: Optional[BrokerAPI] = None
            self.check_interval_seconds = 60  # Check every minute

            # Cached market hours data
            self.market_hours: Dict[str, List[Dict]] = {}
            self.market_hours_file = "symbol_sessions.json"
            self.semaphore_file = "symbol_sessions.lock"

            self.__initialized = True

    @exception_handler
    async def register_observer(self,
                                symbol: str,
                                broker: BrokerAPI,
                                callback: ObserverCallback,
                                observer_id: str):
        """Registers a new observer for a symbol."""
        start_needed = False

        async with self._observers_lock:
            if symbol not in self.observers:
                self.observers[symbol] = {}

            observer = MarketStateObserver(callback)
            self.observers[symbol][observer_id] = observer

            self.logger.info(f"Registered observer {observer_id} for symbol {symbol}")

            async with self._state_lock:
                if not self.broker:
                    self.broker = broker
                if not self._running:
                    self._running = True
                    start_needed = True

            if start_needed:
                await self.start()

        # Notify the observer with the current state if available
        market_is_open = await self.broker.is_market_open(symbol)
        current_timestamp = now_utc().timestamp()

        observer.market_open = market_is_open
        if market_is_open:
            observer.market_opened_time = current_timestamp
            observer.market_closed_time = None
        else:
            observer.market_closed_time = current_timestamp
            observer.market_opened_time = None

        await callback(
            market_is_open,
            observer.market_closed_time,
            observer.market_opened_time,
            True
        )

    async def _read_market_hours(self):
        """Read and cache the market hours from the JSON file."""
        try:
            # Wait for semaphore to be removed
            while self._is_semaphore_active():
                self.logger.info("Semaphore file active. Waiting for file access...")
                await asyncio.sleep(1)  # Wait 1 second before checking again

            # Read the JSON file once the semaphore is cleared
            with open(self.market_hours_file, "r") as f:
                data = json.load(f)
                self.market_hours = {
                    item['symbol']: item['sessions']
                    for item in data.get('symbols', [])
                }
                self.logger.info("Market hours updated successfully.")
        except Exception as e:
            self.logger.error(f"Error reading market hours file: {e}")

    def _is_semaphore_active(self) -> bool:
        """Check if the semaphore file exists."""
        try:
            import os
            return os.path.exists(self.semaphore_file)
        except Exception as e:
            self.logger.error(f"Error checking semaphore file: {e}")
            return True

    def _is_session_active(self, symbol: str, current_time: datetime) -> bool:
        """Check if the current time is within a trading session for the symbol."""
        sessions = self.market_hours.get(symbol, [])
        for session in sessions:
            day = session['day']
            start_time = datetime.strptime(session['start_time'], "%H:%M").time()
            end_time = datetime.strptime(session['end_time'], "%H:%M").time()

            if current_time.strftime('%A') == day:
                if start_time <= current_time.time() <= end_time:
                    return True
        return False

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while True:
            try:
                # Update market hours from the file
                await self._read_market_hours()

                current_time = now_utc()

                # Create a safe copy of observers
                async with self._observers_lock:
                    observers_copy = {symbol: observers.copy() for symbol, observers in self.observers.items()}

                # For each symbol
                for symbol, observers in observers_copy.items():
                    try:
                        market_is_open = await self.broker.is_market_open(symbol)
                        session_active = self._is_session_active(symbol, current_time)

                        current_timestamp = current_time.timestamp()

                        # For each observer of the symbol
                        notification_tasks = []
                        for observer_id, observer in observers.items():
                            state_changed = (
                                observer.market_open != market_is_open or
                                observer.session_active != session_active or
                                observer.market_open is None
                            )

                            if state_changed:
                                observer.market_open = market_is_open
                                observer.session_active = session_active

                                if market_is_open:
                                    observer.market_opened_time = current_timestamp
                                    observer.market_closed_time = None
                                else:
                                    observer.market_closed_time = current_timestamp
                                    observer.market_opened_time = None

                                # Prepare the callback
                                notification_tasks.append(
                                    observer.callback(
                                        market_is_open and session_active,
                                        observer.market_closed_time,
                                        observer.market_opened_time,
                                        False
                                    )
                                )

                        # Notify observers
                        if notification_tasks:
                            await asyncio.gather(*notification_tasks, return_exceptions=True)
                            self.logger.debug(
                                f"Notified observers for symbol {symbol} state change"
                            )

                    except Exception as e:
                        self.logger.error(f"Error processing symbol {symbol}: {e}")

                # Sleep until next check
                await asyncio.sleep(self.check_interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in market state monitor loop: {e}")
                await asyncio.sleep(5)
