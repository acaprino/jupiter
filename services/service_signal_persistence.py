from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, List

from dto.Signal import Signal
from misc_utils.bot_logger import BotLogger, with_bot_logger
from misc_utils.config import ConfigReader
from misc_utils.enums import TradingDirection, Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc, dt_to_unix, to_serializable
from services.service_mongodb import MongoDBService


@with_bot_logger
class SignalPersistenceService:
    _instance = None
    _lock = Lock()

    def __new__(cls, config: ConfigReader, *args):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    if not cls._instance:
                        cls._instance = super(SignalPersistenceService, cls).__new__(cls, *args)
        return cls._instance

    def __init__(self, config: ConfigReader):
        """
        Initializes the SignalPersistenceManager only once.
        Sets up the database connection but does not create
        any logger instance here.
        """
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self.agent = "SignalPersistenceService"
            self.config = config
            self.logger = BotLogger.get_logger(name=self.config.get_bot_name(), level=self.config.get_bot_logging_level())
            db_name = config.get_mongo_db_name()
            host = config.get_mongo_host()
            port = config.get_mongo_port()

            self.db_service = MongoDBService(
                config=config,
                host=host,
                port=port,
                db_name=db_name
            )
            self.collection = None
            self.collection_name = "signals"

    @exception_handler
    async def save_signal(
            self,
            signal: Signal
    ) -> bool:
        """
        Saves (or upserts) a signal in the DB with the given signal_id.
        """
        dict_upsert = to_serializable(signal)

        try:
            await self.db_service.upsert(collection=self.collection_name, id_object={"signal_id": signal.signal_id}, payload=dict_upsert)
            self.info(f"Signal {signal.signal_id} saved successfully.")
            return True
        except Exception as e:
            self.critical(f"Error saving signal {signal.signal_id}: {e}")
            return False

    @exception_handler
    async def update_signal_status(
            self,
            signal: Signal
    ) -> bool:
        """
        Updates the status of the signal identified by signal_id,
        filtered by symbol, timeframe, and direction.
        """
        try:
            result = await self.db_service.upsert(collection=self.collection_name, id_object={"signal_id": signal.signal_id}, payload=to_serializable(signal))
            if result > 0:
                self.info(f"Signal {signal.signal_id} updated to status: {signal.confirmed}.")
                return True
            else:
                self.error(f"Signal {signal.signal_id} not found.")
                return False
        except Exception as e:
            self.critical(f"Error updating signal {signal.signal_id}: {e}")
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
        """
        try:
            find_filter = {
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "candle.time_close": {"$gt": dt_to_unix(now_utc() - timedelta(seconds=timeframe.to_seconds()))},
                "candle.time_open": {"$lt": dt_to_unix(now_utc() - timedelta(seconds=timeframe.to_seconds()))}
            }
            return await self.db_service.find_many(collection=self.collection_name, filter=find_filter)
        except Exception as e:
            self.error(f"Error retrieving active signals: {e}")
            return []

    @exception_handler
    async def start(self):
        """
        Initializes the DB connection and creates an index on the signal_id field.
        """
        await self.db_service.connect()

        if not await self.db_service.test_connection():
            raise Exception("Unable to connect to MongoDB instance.")

        self.collection = await self.db_service.create_index(
            collection=self.collection_name,
            index_field="signal_id",
            unique=True
        )
        self.info("SignalPersistenceManager started. Index created on 'signal_id'.")

    @exception_handler
    async def stop(self):
        """
        Disconnects from the DB.
        """
        await self.db_service.disconnect()
        self.info("SignalPersistenceManager stopped.")
