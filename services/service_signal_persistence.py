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
    """Thread-safe and async-safe singleton service for persisting signals in MongoDB."""

    _instance: Optional['SignalPersistenceService'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use SignalPersistenceService.get_instance() instead")
        return super().__new__(cls)

    def __init__(self, config: ConfigReader):
        # Early return if already initialized
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self.agent = "SignalPersistenceService"
        self.config = config

        # Initialize locks
        self._async_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()

        # Initialize database related attributes
        self.db_service = None
        self.collection = None
        self.collection_name = "signals"

        # Set initialized flag
        self._initialized = True
        self._async_initialized = False

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'SignalPersistenceService':
        """
        Gets or creates a singleton instance of SignalPersistenceService.
        Thread-safe and async-safe implementation.

        Args:
            config: Configuration reader instance

        Returns:
            Singleton instance of SignalPersistenceService
        """
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = SignalPersistenceService(config)
                # Initialize the database connection
                await cls._instance._init_db_connection()
            return cls._instance

    async def _init_db_connection(self):
        """Initialize database connection if not already initialized"""
        async with self._start_lock:
            if not self._async_initialized:
                # Create MongoDB service
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

                # Start the service
                await self.start()

    @exception_handler
    async def save_signal(self, signal: Signal) -> bool:
        """
        Saves (or upserts) a signal in the DB with the given signal_id.
        Thread-safe and async-safe implementation.
        """
        # Ensure async initialization
        if not self._async_initialized:
            await self._init_db_connection()

        dict_upsert = to_serializable(signal)

        try:
            async with self._async_lock:
                await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=dict_upsert
                )
            self.info(f"Signal {signal.signal_id} saved successfully.")
            return True
        except Exception as e:
            self.critical(f"Error saving signal {signal.signal_id}", exec_info=e)
            return False

    @exception_handler
    async def update_signal_status(self, signal: Signal) -> bool:
        """
        Updates the status of the signal identified by signal_id,
        filtered by symbol, timeframe, and direction.
        Thread-safe and async-safe implementation.
        """
        # Ensure async initialization
        if not self._async_initialized:
            await self._init_db_connection()

        try:
            async with self._async_lock:
                result = await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=to_serializable(signal)
                )

            if result and len(result) > 0:
                self.info(f"Signal {signal.signal_id} updated to status: {signal.confirmed}.")
                return True
            else:
                self.error(f"Signal {signal.signal_id} not found.", exec_info=False)
                return False
        except Exception as e:
            self.critical(f"Error updating signal {signal.signal_id}", exec_info=e)
            return False

    @exception_handler
    async def retrieve_active_signals(
            self,
            symbol: str,
            timeframe: Timeframe,
            direction: TradingDirection,
            agent: Optional[str]
    ) -> List[Signal]:
        """
        Returns all signals that are still considered "active",
        i.e. with candle_close_time greater than current_time.
        Thread-safe and async-safe implementation.
        """
        # Ensure async initialization
        if not self._async_initialized:
            await self._init_db_connection()

        try:
            find_filter = {
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "candle.time_close": {"$gt": dt_to_unix(now_utc() - timedelta(seconds=timeframe.to_seconds()))},
                "candle.time_open": {"$lt": dt_to_unix(now_utc() - timedelta(seconds=timeframe.to_seconds()))}
            }
            if agent:
                find_filter["agent"] = agent

            async with self._async_lock:
                signals = await self.db_service.find_many(
                    collection=self.collection_name,
                    filter=find_filter
                )
            return signals
        except Exception as e:
            self.error(f"Error retrieving active signals", exec_info=e)
            return []

    @exception_handler
    async def get_signal(self, signal_id: str) -> Optional[Signal]:
        """
        Retrieves a signal from the MongoDB database based on the signal_id.
        Thread-safe and async-safe implementation.

        :param signal_id: The unique identifier of the signal.
        :return: A Signal instance if found, otherwise None.
        """
        # Ensure async initialization
        if not self._async_initialized:
            await self._init_db_connection()

        try:
            async with self._async_lock:
                document = await self.db_service.find_one(
                    collection=self.collection_name,
                    filter={"signal_id": signal_id}
                )

            if document:
                return Signal.from_json(document)
            return None
        except Exception as e:
            self.error(f"Error retrieving signal {signal_id}", exec_info=e)
            return None

    @exception_handler
    async def start(self):
        """
        Initializes the DB connection and creates an index on the signal_id field.
        Thread-safe and async-safe implementation.
        """
        async with self._start_lock:
            if not self._async_initialized:
                try:
                    await self.db_service.connect()

                    if not await self.db_service.test_connection():
                        raise Exception("Unable to connect to MongoDB instance.")

                    self.collection = await self.db_service.create_index(
                        collection=self.collection_name,
                        index_field="signal_id",
                        unique=True
                    )
                    self._async_initialized = True
                    self.info("SignalPersistenceManager started. Index created on 'signal_id'.")
                except Exception as e:
                    self._async_initialized = False
                    self.critical("Failed to start SignalPersistenceManager", exec_info=e)
                    raise

    @exception_handler
    async def stop(self):
        """
        Disconnects from the DB.
        Thread-safe and async-safe implementation.
        """
        async with self._start_lock:
            if self._async_initialized:
                async with self._async_lock:
                    await self.db_service.disconnect()
                    self._async_initialized = False
                    self.info("SignalPersistenceManager stopped.")