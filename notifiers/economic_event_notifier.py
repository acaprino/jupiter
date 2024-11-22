import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Callable, Awaitable, List, Dict, Optional

from brokers.broker_interface import BrokerAPI
from misc_utils.error_handler import exception_handler
from misc_utils.bot_logger import BotLogger
from misc_utils.utils_functions import now_utc


class EconomicEventNotifier:
    """
    Monitors and notifies about upcoming economic events for specific countries
    based on provided symbol, importance level, and interval.
    """

    def __init__(self, worker_id: str, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.worker_id = worker_id
        self.logger = BotLogger.get_logger(worker_id)
        self.broker = broker
        self.symbol = symbol
        self.execution_lock = execution_lock

        self.interval_seconds = 60 * 5
        self.importance = 3
        self.processed_events = {}  # Tracks already processed events

        self.sandbox_dir = None
        self.json_file_path = None

        self._running = False
        self._task = None
        self._on_economic_event_callbacks: List[Callable[[Dict], Awaitable[None]]] = []

    @exception_handler
    async def start(self):
        """Starts the notifier by initializing settings and launching the monitoring loop."""
        if not self._running:
            self.sandbox_dir = await self.broker.get_working_directory()
            self.json_file_path = os.path.join(self.sandbox_dir, 'economic_calendar.json')

            self._running = True
            self._task = asyncio.create_task(self._run())
            self.logger.info(f"EconomicEventNotifier started for {self.symbol}.")

    def register_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        """Registers a callback to be called when an economic event occurs."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_economic_event_callbacks.append(callback)
        self.logger.debug(f"Callback registered for economic event notifications.")

    def unregister_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        """Unregisters a previously registered callback."""
        if callback in self._on_economic_event_callbacks:
            self._on_economic_event_callbacks.remove(callback)
            self.logger.debug(f"Callback unregistered from economic event notifications.")

    def get_next_run_time(self, now: datetime) -> datetime:
        """Calculates the next time to check for economic events based on interval."""
        interval_minutes = self.interval_seconds / 60
        discard = timedelta(
            minutes=now.minute % interval_minutes,
            seconds=now.second,
            microseconds=now.microsecond
        )
        return now + timedelta(minutes=interval_minutes) - discard

    @exception_handler
    async def _run(self):
        """Main loop for checking economic events based on interval and market status."""
        while self._running:
            try:
                if not await self.broker.is_market_open(self.symbol):
                    self.logger.info(f"Market closed for {self.symbol}. Waiting for {self.interval_seconds} seconds.")
                    await self.wait_next_run()
                    continue

                now = now_utc().replace(microsecond=0)
                next_run = self.get_next_run_time(now)
                self.logger.debug(f"Checking events from {now} to {next_run}.")

                self._cleanup_processed_events(now)

                events = await self._load_events()
                if not events:
                    self.logger.warning("No events loaded. Retrying in next interval.")
                    await self.wait_next_run()
                    continue

                countries = self.get_symbol_countries_of_interest(self.symbol)
                self.logger.debug(f"Countries of interest for {self.symbol}: {countries}")

                # Filter events based on countries, importance, and timing
                filtered_events = [
                    event for event in events
                    if event.get('country_code') in countries
                       and event.get('event_importance') == self.importance
                       and now <= event.get('event_time') <= next_run
                       and event.get('event_id') not in self.processed_events
                ]

                if not filtered_events:
                    self.logger.debug("No relevant events found for this cycle.")
                else:
                    self.logger.debug(f"Filtered {len(filtered_events)} relevant events for this cycle.")

                    for event in filtered_events:
                        await self._handle_event(event)

                await self.wait_next_run()
            except Exception as e:
                self.logger.error(f"Error while monitoring events: {e}")

    @exception_handler
    async def wait_next_run(self):
        # Wait until the next check interval
        now = now_utc()
        next_run = self.get_next_run_time(now)
        sleep_duration = (next_run - now).total_seconds()
        await asyncio.sleep(max(sleep_duration, self.interval_seconds))

    def _cleanup_processed_events(self, current_time: datetime):
        """Removes expired processed events from tracking dictionary."""
        expired_events = {event_id: event_time for event_id, event_time in self.processed_events.items()
                          if event_time <= current_time}
        for event_id in expired_events:
            del self.processed_events[event_id]
        if expired_events:
            self.logger.debug(f"Removed expired events: {expired_events}")

    @exception_handler
    async def _load_events(self) -> List[Dict]:
        """Loads and parses economic events from JSON file after checking for file lock."""
        self.logger.debug(f"Loading events from JSON file at: {self.json_file_path}")

        lock_file_path = os.path.join(self.sandbox_dir, 'lock.sem')
        self._wait_until_lock_file_removed(lock_file_path)

        if not os.path.exists(self.json_file_path):
            self.logger.error(f"Economic events file not found: {self.json_file_path}")
            return []

        if os.path.getsize(self.json_file_path) == 0:
            self.logger.error("Economic events file is empty.")
            return []

        try:
            timezone_offset = await self.broker.get_broker_timezone_offset(self.symbol)
            with open(self.json_file_path, 'r') as file:
                events = json.load(file)
                for event in events:
                    event['event_time'] = datetime.strptime(event['event_time'], '%Y.%m.%d %H:%M') - timedelta(hours=timezone_offset)
            self.logger.debug("Economic events loaded successfully.")
            return events
        except json.JSONDecodeError:
            self.logger.error("Error decoding economic events JSON file.")
            return []
        except Exception as e:
            self.logger.error(f"Error loading economic events: {e}")
            return []

    def _wait_until_lock_file_removed(self, lock_file_path: str, check_interval: int = 5, timeout: int = 300):
        """Waits for the lock file to be removed, indicating file is ready for access."""
        start_time = time.time()
        while os.path.exists(lock_file_path):
            if time.time() - start_time > timeout:
                self.logger.warning(f"Timeout reached. Lock file still exists: {lock_file_path}")
                break
            self.logger.debug(f"Waiting for lock file removal: {lock_file_path}")
            time.sleep(check_interval)
        self.logger.debug(f"Lock file {lock_file_path} removed or timeout reached.")

    @exception_handler
    async def _handle_event(self, event: Dict):
        """Processes a single economic event and triggers callbacks."""
        event_id = event.get('event_id')
        event_name = event.get('event_name')
        event_time = event.get('event_time')

        self.logger.info(f"Handling event '{event_name}' (ID: {event_id}) scheduled at {event_time}.")

        seconds_until_event = (event_time - now_utc()).total_seconds()
        event['seconds_until_event'] = seconds_until_event

        self.processed_events[event_id] = event_time
        self.logger.debug(f"Event {event_id} marked as processed.")

        await self._notify_callbacks(event)

    @exception_handler
    async def _notify_callbacks(self, notification: Dict):
        """Notifies registered callbacks of an economic event."""
        tasks = [callback(notification) for callback in self._on_economic_event_callbacks]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.debug(f"Callbacks notified for event ID: {notification.get('event_id')}")

    def get_symbol_countries_of_interest(self, symbol: str) -> List[str]:
        """Gets a list of countries associated with the provided symbol."""
        try:
            pair = self.get_pair(symbol)
            countries = pair.get("countries", []) if pair else []
            self.logger.debug(f"Countries of interest for {symbol}: {countries}")
            return countries
        except Exception as e:
            self.logger.error(f"Error determining countries of interest: {e}")
            return []

    def get_pairs(self) -> List[Dict]:
        """Loads pairs and their associated countries from pairs.json file."""
        try:
            cur_script_directory = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(os.path.dirname(cur_script_directory), 'pairs.json')

            with open(file_path, 'r') as file:
                data = json.load(file)
            return data
        except Exception as e:
            self.logger.error(f"Error loading pairs data: {e}")
            return []

    def get_pair(self, symbol: str) -> Optional[Dict]:
        """Fetches the pair data for the specified symbol."""
        pairs = self.get_pairs()
        for pair in pairs:
            if pair["symbol"] == symbol:
                return pair
        self.logger.error(f"Symbol '{symbol}' not found in pairs.json")
        return None

    @exception_handler
    async def stop(self):
        """Stops the notifier by canceling the monitoring loop."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info(f"EconomicEventNotifier for {self.symbol} stopped.")
