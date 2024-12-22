from datetime import datetime
from threading import Lock
from typing import Optional, List

from dto.Signal import Signal
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.enums import TradingDirection, Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc, dt_to_unix
from services.service_mongodb import MongoDB


class SignalPersistenceManager:
    _instance = None
    _lock = Lock()
    collection_name = "signals"

    def __new__(cls, config: ConfigReader, *args):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    if not cls._instance:
                        cls._instance = super(SignalPersistenceManager, cls).__new__(cls, *args)
        return cls._instance

    def __init__(self, config: ConfigReader):
        """
        Initializes the SignalPersistenceManager only once.
        Sets up the database connection but does not create
        any logger instance here.
        """
        if not hasattr(self, "_initialized"):
            self._initialized = True

            self.config = config
            db_name = config.get_mongo_db_name()
            host = config.get_mongo_host()
            port = config.get_mongo_port()

            self.db_service = MongoDB(
                bot_name=config.get_bot_name(),
                host=host,
                port=port,
                db_name=db_name
            )
            self.collection = None

    def get_agent_name(
            self,
            symbol: str,
            timeframe: Timeframe,
            direction: TradingDirection,
            agent: Optional[str]
    ) -> str:
        """
        Builds the agent name based on symbol, timeframe, direction, and
        an optional agent prefix.
        """
        topic = f"{symbol}.{timeframe.name}.{direction.name}"
        prefix = agent if agent is not None else self.config.get_bot_mode().name
        return f"{prefix}_{topic}"

    @exception_handler
    async def save_signal(
            self,
            signal: Signal
    ) -> bool:
        """
        Saves (or upserts) a signal in the DB with the given signal_id.
        """
        logger = BotLogger.get_logger(
            name=self.get_agent_name(signal.symbol, signal.timeframe, signal.direction, signal.agent),
            level=self.config.get_bot_logging_level()
        )

        dict_upsert = signal.to_json()

        try:
            await self.db_service.upsert(dict_upsert)
            logger.info(f"Signal {signal.signal_id} saved successfully.")
            return True
        except Exception as e:
            logger.critical(f"Error saving signal {signal.signal_id}: {e}")
            return False

    @exception_handler
    async def update_signal_status(
            self,
            signal: Signal,
            symbol: str,
            timeframe: Timeframe,
            direction: TradingDirection,
            agent: Optional[str]
    ) -> bool:
        """
        Updates the status of the signal identified by signal_id,
        filtered by symbol, timeframe, and direction.
        """
        logger = BotLogger.get_logger(
            name=self.get_agent_name(symbol, timeframe, direction, agent),
            level=self.config.get_bot_logging_level()
        )

        filter_query = {
            "signal_id": signal.signal_id,
            "symbol": symbol,
            "timeframe": timeframe.name,
            "direction": direction.name
        }

        update_query = {
            "$set": {
                "confirmed": signal.confirmed,
                "updated_at": signal.update_tms
            }
        }

        try:
            result = await self.db_service.upsert(filter_query, update_query)
            if result > 0:
                logger.info(f"Signal {signal.signal_id} updated to status: {signal.confirmed}.")
                return True
            else:
                logger.error(f"Signal {signal.signal_id} not found.")
                return False
        except Exception as e:
            logger.critical(f"Error updating signal {signal.signal_id}: {e}")
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
        logger = BotLogger.get_logger(
            name=self.get_agent_name(symbol, timeframe, direction, agent),
            level=self.config.get_bot_logging_level()
        )

        try:
            active_signals = list(self.collection.find({
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "candle_close_time": {"$gt": dt_to_unix(now_utc())}
            }))
            return active_signals
        except Exception as e:
            logger.error(f"Error retrieving active signals: {e}")
            return []

    @exception_handler
    async def start(self):
        """
        Initializes the DB connection and creates an index on the signal_id field.
        """
        logger = BotLogger.get_logger(
            name="SignalPersistenceManager",
            level=self.config.get_bot_logging_level()
        )
        await self.db_service.connect()

        if not await self.db_service.test_connection():
            raise Exception("Unable to connect to MongoDB instance.")

        self.collection = await self.db_service.create_index(
            collection=self.collection_name,
            index_field="signal_id",
            unique=True
        )
        logger.info("SignalPersistenceManager started. Index created on 'signal_id'.")

    @exception_handler
    async def stop(self):
        """
        Disconnects from the DB.
        """
        logger = BotLogger.get_logger(
            name="SignalPersistenceManager",
            level=self.config.get_bot_logging_level()
        )

        await self.db_service.disconnect()
        logger.info("SignalPersistenceManager stopped.")
