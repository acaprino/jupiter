from datetime import timedelta
from typing import Optional, List
import asyncio

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

                    self.db_service = MongoDBService(
                        config=self.config,
                        host=host,
                        port=port,
                        username=username,
                        password=password,
                        db_name=db_name
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
            agent: Optional[str] = None
    ) -> List[Signal]:
        """
        Retrieves signals from the database that have NOT YET EXPIRED at the time this
        function is called (typically during an ExecutorAgent restart).

        A signal generated by candle N (closing at time_close_N) expires at time_close_N + timeframe.
        Only retrieves signals where the current time is still before that expiration time.

        Args:
            symbol (str): Symbol to filter by.
            timeframe (Timeframe): Timeframe to filter by.
            direction (TradingDirection): Direction to filter by.
            agent (Optional[str]): Agent name (optional, used for logging/debug).

        Returns:
            List[Signal]: List of signals (confirmed or not) that haven't expired.
        """
        self.debug(f"[{now_utc()}] retrieve_active_signals: Start retrieving signals for {symbol}/{timeframe.name}/{direction.name}.")
        await self._ensure_db_ready()
        try:
            # Calculate threshold: only signals with expiration time (candle.time_close + timeframe)
            # still in the future will be retrieved.
            # We subtract a buffer of 60 seconds to anticipate closure and avoid race conditions.
            now_minus_timeframe_unix = dt_to_unix(
                now_utc() - timedelta(seconds=(timeframe.to_seconds() - 60))
            )

            # Build the MongoDB filter query
            find_filter = {
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                # Retrieve signals regardless of 'confirmed' status.
                "candle.time_close": {"$gt": now_minus_timeframe_unix}
            }

            self.debug(f"[{now_utc()}] retrieve_active_signals: Filter for query - {find_filter}")

            async with self._async_lock:
                documents = await self.db_service.find_many(
                    collection=self.collection_name,
                    filter=find_filter
                )

            signals = []
            if documents:
                self.debug(f"[{now_utc()}] retrieve_active_signals: {len(documents)} documents retrieved from DB.")
                for doc in documents:
                    try:
                        signals.append(Signal.from_json(doc))
                        self.debug(f"[{now_utc()}] retrieve_active_signals: Successfully deserialized signal with id {doc.get('signal_id')}.")
                    except Exception as e:
                        self.error(f"[{now_utc()}] retrieve_active_signals: Error deserializing signal from DB document: {doc}.", exec_info=e)

            self.info(f"[{now_utc()}] Retrieved {len(signals)} active signals for {symbol}/{timeframe.name}/{direction.name}.")
            self.debug(f"[{now_utc()}] retrieve_active_signals: Retrieval process completed.")
            return signals

        except Exception as e:
            self.error(f"[{now_utc()}] retrieve_active_signals: Critical error retrieving active signals for {symbol}/{timeframe.name}/{direction.name}.", exec_info=e)
            return []

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
                    collection=self.collection_name,
                    filter={"signal_id": signal_id}
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
