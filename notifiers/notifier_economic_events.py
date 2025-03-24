import asyncio
import os

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
        self.notified_events: Dict[str, datetime] = {}  # Tracks event IDs with timestamps


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
        self.sandbox_dir = None
        self.json_file_path = None
        self._initialized = True

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'NotifierEconomicEvents':
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = NotifierEconomicEvents(config)
            return cls._instance

    def _get_observer_key(self, country: str, importance: EventImportance) -> Tuple[str, EventImportance]:
        return (country, importance)

    @exception_handler
    async def register_observer(self,
                                countries: List[str],
                                callback: ObserverCallback,
                                observer_id: str,
                                importance: EventImportance = EventImportance.HIGH):
        async with self._observers_lock:
            for country in countries:
                key = self._get_observer_key(country, importance)
                if key not in self.observers:
                    self.observers[key] = {}

                observer = CountryEventObserver(country, importance, callback)
                self.observers[key][observer_id] = observer
                self.info(f"Registered observer {observer_id} for {country} ({importance.name})")

        await self.start()

    @exception_handler
    async def unregister_observer(self, countries: List[str], importance: EventImportance, observer_id: str):
        async with self._observers_lock:
            for country in countries:
                key = self._get_observer_key(country, importance)
                if key in self.observers and observer_id in self.observers[key]:
                    del self.observers[key][observer_id]
                    self.info(f"Unregistered observer {observer_id} for {country}")

                    if not self.observers[key]:
                        del self.observers[key]
                        self.info(f"Stopped monitoring for {country} ({importance.name})")

            if not any(self.observers.values()) and self._running:
                await self.stop()

    async def start(self):
        async with self._start_lock:
            if not self._running:
                self._running = True
                self._task = asyncio.create_task(self._monitor_loop())
                self.info("Monitoring started")

    async def stop(self):
        async with self._start_lock:
            if self._running:
                self._running = False
                if self._task:
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                    self._task = None
                self.info("Monitoring stopped")

    async def shutdown(self):
        await self.stop()
        async with self._observers_lock:
            self.observers.clear()
            self.info("Full shutdown completed")

    async def _load_events(self, processing_window: datetime) -> Optional[List[EconomicEvent]]:
        try:
            async with self._observers_lock:
                countries = list({key[0] for key in self.observers.keys()})

            if not countries:
                return None

            broker_offset = await Broker().with_context("*").get_broker_timezone_offset()
            now = now_utc()

            tasks = []
            for country in countries:
                tasks.append(
                    Broker().with_context("*").get_economic_calendar(
                        country=country,
                        _from=now,
                        _to=processing_window + timedelta(hours=1)
                    )
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)
            events = []

            for country, result in zip(countries, results):
                if isinstance(result, Exception):
                    self.error(f"Error loading events for {country}", exec_info=result)
                    continue

                # Type narrowing esplicito per il type checker
                events_list: List[EconomicEvent] = result  # type: ignore[assignment]
                for event in events_list:
                    try:
                        event.time = event.time - broker_offset
                        if event.time >= now:
                            events.append(event)
                    except Exception as e:
                        self.error(f"Error processing event for {country}", exec_info=e)

            return sorted(events, key=lambda x: x.time)

        except Exception as e:
            self.error("Critical error loading events", exec_info=e)
            return None

    async def _cleanup_notified_events(self):
        cutoff = now_utc() - timedelta(hours=24)
        async with self._observers_lock:
            for key in self.observers:
                for observer in self.observers[key].values():
                    expired = [eid for eid, ts in observer.notified_events.items() if ts < cutoff]
                    for eid in expired:
                        del observer.notified_events[eid]

    async def _get_relevant_events(self, events: List[EconomicEvent], next_run: datetime) -> List[EconomicEvent]:
        now = now_utc()
        return [event for event in events if now <= event.time <= next_run]

    async def _notify_observers_for_event(self, event: EconomicEvent):
        async with self._observers_lock:
            observers = []
            for (country, importance), obs_dict in self.observers.items():
                if country == event.country and event.importance.value >= importance.value:
                    observers.extend(obs_dict.values())

        for observer in observers:
            event_id = event.event_id
            if event_id not in observer.notified_events:
                try:
                    await observer.callback(event)
                    observer.notified_events[event_id] = event.time
                    self.info(f"Notified {observer.country} observer for {event.event_id}")
                except Exception as e:
                    self.error(f"Notification failed for {event_id}", exec_info=e)

    async def _calculate_sleep_time(self) -> float:
        now = now_utc().timestamp()
        next_check = ((now // self.interval_seconds) + 1) * self.interval_seconds
        return max(0.0, next_check - now)

    async def _monitor_loop(self):
        while self._running:
            start_time = now_utc()

            # Phase 1: Cleanup
            await self._cleanup_notified_events()

            # Phase 2: Data Loading
            processing_window = start_time + timedelta(seconds=self.interval_seconds * 2)
            events = await self._load_events(processing_window)

            # Phase 3: Event Processing
            if events:
                current_window_end = now_utc() + timedelta(seconds=self.interval_seconds)
                relevant_events = await self._get_relevant_events(events, current_window_end)

                for event in relevant_events:
                    await self._notify_observers_for_event(event)

            # Phase 4: Precise Sleep
            sleep_time = await self._calculate_sleep_time()
            await asyncio.sleep(sleep_time)
