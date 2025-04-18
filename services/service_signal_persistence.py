import asyncio

from datetime import timedelta, datetime, timezone
from typing import Optional, List
from dto.Signal import Signal
from misc_utils.config import ConfigReader
from misc_utils.enums import TradingDirection, Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc, dt_to_unix, to_serializable
from services.service_mongodb import MongoDBService


class SignalPersistenceService(LoggingMixin):
    """Singleton that is thread-safe and async-safe for persisting signals on MongoDB."""

    _instance: Optional['SignalPersistenceService'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            raise RuntimeError("Use SignalPersistenceService.get_instance() to obtain an instance")
        return super().__new__(cls)

    def __init__(self, config: ConfigReader):
        if getattr(self, '_initialized', False):
            return

        # Initialize parent class and configuration
        super().__init__(config)
        self.agent = "SignalPersistenceService"
        self.config = config

        # Async lock for protecting asynchronous operations
        self._async_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        # Event that signals when the DB connection is ready
        self._db_ready = asyncio.Event()

        # Attributes for the database connection
        self.db_service = None
        self.collection = None
        self.collection_name = "signals"

        self._initialized = True
        self._async_initialized = False

        self.debug(f"[{now_utc()}] __init__: Instance initialized, starting background DB connection task.")
        # Start the DB connection initialization in background
        asyncio.create_task(self._init_db_connection())

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'SignalPersistenceService':
        """
        Returns the singleton instance.
        The DB connection is initialized in background.
        """
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = SignalPersistenceService(config)
                cls._instance.debug(f"[{now_utc()}] get_instance: New instance created.")
            else:
                cls._instance.debug(f"[{now_utc()}] get_instance: Existing instance returned.")
            return cls._instance

    async def _init_db_connection(self):
        """
        Initializes the connection to the MongoDB and prepares the collection index.
        """
        async with self._start_lock:
            if not self._async_initialized:
                self.debug(f"[{now_utc()}] _init_db_connection: Starting database initialization.")
                try:
                    db_name = self.config.get_mongo_db_name()
                    host = self.config.get_mongo_host()
                    port = self.config.get_mongo_port()
                    username = self.config.get_mongo_username()
                    password = self.config.get_mongo_password()
                    is_cluster = self.config.get_mongo_is_cluster()

                    self.db_service = MongoDBService(
                        config=self.config,
                        host=host,
                        port=port,
                        username=username,
                        password=password,
                        db_name=db_name,
                        is_cluster=is_cluster
                    )

                    # Start connection and create index on signal_id
                    await self.start()
                    self._async_initialized = True
                    self._db_ready.set()
                    self.debug(f"[{now_utc()}] _init_db_connection: Database connection initialized successfully.")
                except Exception as e:
                    self.critical(f"[{now_utc()}] _init_db_connection: Failed to initialize DB connection.", exec_info=e)
                    raise

    async def _ensure_db_ready(self):
        """
        Ensure the DB connection is ready before proceeding.
        """
        self.debug(f"[{now_utc()}] _ensure_db_ready: Waiting for DB readiness signal.")
        await self._db_ready.wait()
        self.debug(f"[{now_utc()}] _ensure_db_ready: DB is ready.")

    @exception_handler
    async def save_signal(self, signal: Signal) -> bool:
        """
        Saves (or upserts) a signal in the database based on signal_id.
        """
        self.debug(f"[{now_utc()}] save_signal: Initiating save for Signal {signal.signal_id}.")
        await self._ensure_db_ready()
        payload = to_serializable(signal)
        try:
            async with self._async_lock:
                await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=payload
                )
            self.info(f"[{now_utc()}] Signal {signal.signal_id} saved successfully.")
            self.debug(f"[{now_utc()}] save_signal: Save process completed for Signal {signal.signal_id}.")
            return True
        except Exception as e:
            self.critical(f"[{now_utc()}] save_signal: Error saving Signal {signal.signal_id}.", exec_info=e)
            return False

    @exception_handler
    async def update_signal_status(self, signal: Signal) -> bool:
        """
        Updates the status of the signal identified by signal_id.
        """
        self.debug(f"[{now_utc()}] update_signal_status: Initiating update for Signal {signal.signal_id}.")
        await self._ensure_db_ready()
        try:
            async with self._async_lock:
                result = await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=to_serializable(signal)
                )
            if result and len(result) > 0:
                self.info(f"[{now_utc()}] Signal {signal.signal_id} updated to status: {signal.confirmed}.")
                self.debug(f"[{now_utc()}] update_signal_status: Update process completed for Signal {signal.signal_id}.")
                return True
            else:
                self.error(f"[{now_utc()}] update_signal_status: Signal {signal.signal_id} not found.", exec_info=False)
                return False
        except Exception as e:
            self.critical(f"[{now_utc()}] update_signal_status: Error updating Signal {signal.signal_id}.", exec_info=e)
            return False

    @exception_handler
    async def retrieve_active_signals(
            self,
            symbol: str,
            timeframe: Timeframe,
            direction: TradingDirection,
            agent: Optional[str] = None,
            time_ref: Optional[datetime] = None
    ) -> List[Signal]:
        """
        Retrieves signals from the database considered "active" relative to a specific reference time.

        The reference time (`time_ref` or current time) is always treated as a naive datetime
        representing UTC time for internal calculations.

        A signal associated with a candle closing at `cur_candle.time_close` (stored as Unix timestamp UTC)
        is considered active if the reference time is before the signal's theoretical expiration time
        (`cur_candle.time_close + timeframe`).

        To avoid potential race conditions, a 60-second buffer is applied. Signals are retrieved
        only if their closing time meets the condition:
        `cur_candle.time_close > unix_timestamp(reference_time_naive_utc - timeframe + 60 seconds)`.

        Args:
            symbol (str): The trading symbol to filter signals for.
            timeframe (Timeframe): The timeframe associated with the signals.
            direction (TradingDirection): The trading direction (e.g., LONG, SHORT) to filter by.
            agent (Optional[str]): The name of the agent requesting the signals
                (primarily for logging/debugging purposes). Defaults to None.
            time_ref (Optional[datetime]): The reference time against which signal activity
                is checked. If provided, it will be interpreted as naive UTC. If it's
                timezone-aware, it will be converted to naive UTC. If None, the current
                UTC time will be used as a naive datetime (`datetime.utcnow()`).

        Returns:
            List[Signal]: A list of `Signal` objects (confirmed or not) that meet the activity
            criteria based on the naive UTC reference time. Returns an empty list if no signals
            are found or if a critical error occurs during retrieval.

        Requires:
            - `dt_to_unix` function must correctly convert naive UTC datetimes to Unix timestamps.
        """
        # Use utcnow() for consistent internal logging timestamp (aware)
        log_prefix = f"[{datetime.now(timezone.utc)}] retrieve_active_signals ({agent or 'N/A'} - {symbol}/{timeframe.name}/{direction.name}):"
        self.debug(f"{log_prefix} Starting retrieval.")

        await self._ensure_db_ready()

        try:
            # Determine the reference time, ensuring it's naive and represents UTC.
            if time_ref is not None:
                # User provided a time_ref
                if time_ref.tzinfo is not None:
                    # It's aware, convert to UTC then make naive
                    self.debug(f"{log_prefix} Received aware time_ref {time_ref.isoformat()}, converting to naive UTC.")
                    reference_dt_naive_utc = time_ref.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    # It's already naive, assume it's UTC
                    self.debug(f"{log_prefix} Received naive time_ref {time_ref.isoformat()}, assuming UTC.")
                    reference_dt_naive_utc = time_ref
            else:
                # No time_ref provided, get current UTC time as naive
                self.debug(f"{log_prefix} No time_ref provided, using datetime.utcnow().")
                reference_dt_naive_utc = now_utc()

            self.debug(f"{log_prefix} Using reference time (naive UTC): {reference_dt_naive_utc.isoformat()}")

            # Calculate the earliest acceptable close time for a signal to be considered active.
            # All calculations use the naive UTC datetime.
            timeframe_duration_sec = timeframe.to_seconds()
            buffer_sec = 60
            earliest_close_time_dt_naive_utc = reference_dt_naive_utc - timedelta(seconds=timeframe_duration_sec) + timedelta(seconds=buffer_sec)

            # Convert the calculated naive UTC datetime threshold to a Unix timestamp.
            # dt_to_unix MUST correctly interpret the naive datetime as UTC.
            earliest_close_time_unix = dt_to_unix(earliest_close_time_dt_naive_utc)
            self.debug(f"{log_prefix} Calculated earliest acceptable close time (unix): {earliest_close_time_unix} (from naive UTC: {earliest_close_time_dt_naive_utc.isoformat()})")

            # Build the MongoDB filter query (assumes 'cur_candle.time_close' is stored as Unix timestamp UTC)
            find_filter = {
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "cur_candle.time_close": {"$gt": earliest_close_time_unix}
            }

            self.debug(f"{log_prefix} Filter for query - {find_filter}")

            async with self._async_lock:
                 if self.db_service:
                     documents = await self.db_service.find_many(
                         collection=self.collection_name,
                         filter=find_filter
                     )
                 else:
                     self.logger.warning(f"{log_prefix} DB service not available. Simulating empty result.")
                     documents = [] # Simulate empty result if no DB service

            signals = []
            if documents:
                self.debug(f"{log_prefix} {len(documents)} documents retrieved from DB.")
                for doc in documents:
                    try:
                        signal = Signal.from_json(doc) # Assumes this works
                        signals.append(signal)
                        signal_id = doc.get('signal_id', doc.get('_id', 'N/A')) # Get some ID for logging
                        self.debug(f"{log_prefix} Successfully deserialized signal with id {signal_id}.")
                    except Exception as e:
                        doc_id = doc.get('_id', 'N/A')
                        self.error(f"{log_prefix} Error deserializing signal from DB document ID {doc_id}.", exec_info=e)
                        # Consider logging relevant parts of doc if safe: self.error(f"Failed document snippet: {str(doc)[:200]}")

            self.info(f"{log_prefix} Retrieved {len(signals)} active signals.")
            return signals

        except Exception as e:
            # Catches errors during time calculation, DB interaction (if not handled below), etc.
            self.error(f"{log_prefix} Critical error during retrieval process.", exec_info=e)
            return [] # Return empty list on critical failure

    @exception_handler
    async def get_signal(self, signal_id: str) -> Optional[Signal]:
        """
        Retrieves a signal from the database based on its signal_id.
        """
        self.debug(f"[{now_utc()}] get_signal: Attempting to retrieve Signal {signal_id}.")
        await self._ensure_db_ready()
        try:
            async with self._async_lock:
                document = await self.db_service.find_one(
                    self.collection_name,
                    {"signal_id": signal_id}
                )
            if document:
                self.debug(f"[{now_utc()}] get_signal: Signal {signal_id} found in DB.")
                return Signal.from_json(document)
            self.debug(f"[{now_utc()}] get_signal: Signal {signal_id} not found in DB.")
            return None
        except Exception as e:
            self.error(f"[{now_utc()}] get_signal: Error retrieving Signal {signal_id}.", exec_info=e)
            return None

    @exception_handler
    async def start(self):
        """
        Initializes the connection to the database and creates an index on the 'signal_id' field.
        """
        self.debug(f"[{now_utc()}] start: Starting database connection.")
        try:
            await self.db_service.connect()
            self.debug(f"[{now_utc()}] start: DB connect method returned, testing connection.")
            if not await self.db_service.test_connection():
                raise Exception("Unable to connect to the MongoDB instance.")
            self.debug(f"[{now_utc()}] start: Connection test passed; creating index on 'signal_id'.")
            self.collection = await self.db_service.create_index(
                collection=self.collection_name,
                index_field="signal_id",
                unique=True
            )
            self.info(f"[{now_utc()}] SignalPersistenceManager started. Index created on 'signal_id'.")
            self.debug(f"[{now_utc()}] start: Database initialization completed successfully.")
        except Exception as e:
            self.critical(f"[{now_utc()}] start: Failed to start SignalPersistenceManager.", exec_info=e)
            raise

    @exception_handler
    async def stop(self):
        """
        Closes the connection to the database.
        """
        self.debug(f"[{now_utc()}] stop: Stopping SignalPersistenceManager and closing DB connection.")
        await self._ensure_db_ready()
        async with self._async_lock:
            await self.db_service.disconnect()
            self._async_initialized = False
            self._db_ready.clear()
            self.info(f"[{now_utc()}] SignalPersistenceManager stopped.")
            self.debug(f"[{now_utc()}] stop: DB connection closed and internal flags reset.")
