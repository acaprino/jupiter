import asyncio
import os
import threading

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Awaitable, Tuple
from brokers.broker_proxy import Broker
from dto.EconomicEvent import EconomicEvent, EventImportance
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc

ObserverCallback = Callable[[EconomicEvent], Awaitable[None]]


class CountryEventObserver:
    """Represents an observer for events of a country."""

    def __init__(self, country: str, importance: EventImportance, callback: ObserverCallback):
        self.country = country
        self.importance = importance
        self.callback = callback
        self.notified_events: set[str] = set()  # Keep track of notified events


class NotifierEconomicEvents(LoggingMixin):
    _instance: Optional['NotifierEconomicEvents'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader) -> None:
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self._observers_lock: asyncio.Lock = asyncio.Lock()
        self._start_lock: asyncio.Lock = asyncio.Lock()

        self.observers: Dict[Tuple[str, EventImportance], Dict[str, CountryEventObserver]] = {}

        self.config = config
        self.agent = "EconomicEventManager"

        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self.interval_seconds: float = 300.0
        self._min_sleep_time: float = 1.0
        self.processed_events: Dict[str, datetime] = {}
        self.sandbox_dir = None
        self.json_file_path = None
        self.broker = None
        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierEconomicEvents':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierEconomicEvents(config)
            return cls._instance

    def _get_observer_key(self, country: str, importance: EventImportance) -> Tuple[str, EventImportance]:
        """Creates a unique key for the observer."""
        return country, importance

    @exception_handler
    async def register_observer(self,
                                countries: List[str],
                                callback: ObserverCallback,
                                observer_id: str,
                                importance: EventImportance = EventImportance.HIGH):
        """Registers a new observer for a list of countries."""

        async with self._observers_lock:
            for country in countries:
                key = self._get_observer_key(country, importance)
                if key not in self.observers:
                    self.observers[key] = {}

                observer = CountryEventObserver(country, importance, callback)
                self.observers[key][observer_id] = observer

                self.info(f"Registered observer {observer_id} for country {country} with importance {importance.name}")

            if not self.broker:
                self.broker = Broker()
                self.sandbox_dir = await self.broker.get_working_directory()
                self.json_file_path = os.path.join(self.sandbox_dir, 'economic_calendar.json')

        await self.start() # start is idempotent, safe to call it here

    @exception_handler
    async def unregister_observer(self, countries: List[str], importance: EventImportance, observer_id: str):
        """Removes an observer for a list of countries."""
        async with self._observers_lock:
          await self._remove_observer_and_cleanup(countries, importance, observer_id)

    async def _remove_observer_and_cleanup(self, countries: List[str], importance: EventImportance, observer_id: str):
        """Removes observers and stops monitoring if no observers remain."""

        async with self._observers_lock:
            for country in countries:
                key = self._get_observer_key(country, importance)
                if key in self.observers:
                    if observer_id in self.observers[key]:
                        del self.observers[key][observer_id]
                        self.info(f"Unregistered observer {observer_id} for country {country}")

                    if not self.observers[key]:
                        del self.observers[key]
                        self.info(f"Removed monitoring for country {country}, importance {importance.name}")

            if not any(self.observers.values()) and self._running:
                await self.stop()

    async def start(self):
        """Starts the event monitoring."""
        async with self._start_lock:  # Use a lock, even if it's often a no-op.  More consistent.
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.info("Economic event monitoring started")

    async def stop(self):
        """Stops the event monitoring."""
        async with self._start_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self._task = None  # Set _task to None after cancellation.
                self.info("Economic event monitoring stopped")

    async def shutdown(self):
        """Stops monitoring and cleans up resources."""
        await self.stop()  # Idempotent, so safe to call here.
        async with self._observers_lock:
            self.observers.clear()
            self.processed_events.clear()
            self.info("NotifierEconomicEvents shutdown complete.")

    async def _load_events(self) -> Optional[List[EconomicEvent]]:
        """Loads and parses economic events."""
        try:
            events: List[EconomicEvent] = []
            async with self._observers_lock:
                countries = list({key[0] for key in self.observers.keys()})  # Efficient unique country list

            broker_offset_hours = await self.broker.get_broker_timezone_offset()
            _from = now_utc()
            _to = _from + timedelta(days=1)  # Fetch events for the next 24 hours

            for country in countries:
                events_tmp: List[EconomicEvent] = await self.broker.with_context(f"economic_calendar.{country}").get_economic_calendar(country, _from, _to)
                if events_tmp:
                    for event in events_tmp:
                         # Correct for broker time zone offset.  IMPORTANT.
                        event.time = event.time - broker_offset_hours
                    events.extend(events_tmp)

            return events
        except Exception as e:
            self.error(f"Error loading economic events: {e}")
            return None

    def _cleanup_processed_events(self):
        """Removes expired processed events."""
        current_time = now_utc()
        expired_events = [
            event_id for event_id, event_time in self.processed_events.items()
            if event_time <= current_time
        ]
        for event_id in expired_events:
            del self.processed_events[event_id]

    def _cleanup_notified_events(self):
        """Cleans up expired notified events from all observers."""
        cutoff_time = now_utc() - timedelta(hours=24)

        async with self._observers_lock:  # Lock is needed here
            for country_observers in self.observers.values():
                for observer in country_observers.values():
                    expired_events = {
                        event_id for event_id in observer.notified_events
                        if event_id in self.processed_events and self.processed_events[event_id] < cutoff_time
                    }
                    observer.notified_events.difference_update(expired_events)

    async def _get_relevant_events(self, events: List[EconomicEvent], next_run:datetime) -> List[EconomicEvent]:
        """Filters events relevant for the current monitoring period."""

        now = now_utc()
        return [
            event for event in events
            if (now <= event.time <= next_run and
                event.event_id not in self.processed_events)
        ]

    async def _notify_observers_for_event(self, event: EconomicEvent):
        """Notifies observers for a specific event."""
        notification_tasks = []

        async with self._observers_lock: # Important: lock while accessing self.observers
            for (country, importance), observers in self.observers.items():
                if country == event.country and event.importance.value <= importance.value:
                    for observer_id, observer in observers.items():
                        if event.event_id not in observer.notified_events:
                            observer.notified_events.add(event.event_id)  # Mark as notified *before* the callback
                            notification_tasks.append(observer.callback(event))

        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            self.processed_events[event.event_id] = event.time # only if notify is ok

    async def _calculate_sleep_time(self) -> float:
        """Calculates the time to sleep until the next check."""
        now = now_utc()
        next_check_time = (now + timedelta(seconds=self.interval_seconds)).timestamp()
        next_check_time -= (next_check_time % self.interval_seconds)

        sleep_duration = next_check_time - now.timestamp()

        return max(sleep_duration, self._min_sleep_time)  # Ensure minimum sleep

    async def _monitor_loop(self):
        """Main monitoring loop."""
        try:
            while self._running:
                now = now_utc()

                self._cleanup_processed_events()  # Clean up old processed events
                self._cleanup_notified_events()   # Clean up old notified events.

                events = await self._load_events()
                if events:
                    next_run = now + timedelta(seconds=self.interval_seconds)
                    relevant_events = await self._get_relevant_events(events, next_run)
                    for event in relevant_events:
                         await self._notify_observers_for_event(event)

                sleep_duration = await self._calculate_sleep_time()
                await asyncio.sleep(sleep_duration)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.error(f"Error in monitor loop: {e}")
            await asyncio.sleep(5)