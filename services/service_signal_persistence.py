from datetime import datetime
from threading import Lock

from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import TradingDirection, Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc, dt_to_unix
from services.service_mongodb import MongoDB


class SignalPersistenceManager:
    _instance = None
    _lock = Lock()
    collection_name = "signals"

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(SignalPersistenceManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, config: ConfigReader, agent: str, symbol: str, timeframe: Timeframe, direction: TradingDirection):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self.topic = f"{symbol}.{timeframe.name}.{direction.name}"
            prefix = agent if agent is not None else config.get_bot_mode().name
            self.agent = f"{prefix}_{self.topic}"
            self.config = config
            self.logger = BotLogger.get_logger(name=f"{self.agent}", level=config.get_bot_logging_level())

            db_name = config.get_mongo_db_name()
            host = config.get_mongo_host()
            port = config.get_mongo_port()

            self.db_service = MongoDB(bot_name=config.get_bot_name(), host=host, port=port, db_name=db_name)
            self.collection = None

    @exception_handler
    async def save_signal(self, signal_id: str, symbol: str, timeframe: Timeframe, direction: TradingDirection, candle_close_time: datetime)-> bool:
        signal_data = {
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "direction": direction,
            "candle_close_time": dt_to_unix(candle_close_time),
            "created_at": now_utc()
        }
        try:
            await self.db_service.upsert(signal_data)
            self.logger.info(f"Signal {signal_id} saved.")
            return True
        except Exception as e:
            self.logger.critical(f"Error saving signal {signal_id}: {e}")
            return False

    @exception_handler
    async def update_signal_status(self, signal_id, new_status) -> bool:
        result = await self.db_service.upsert(
            {"signal_id": signal_id},
            {"$set": {"status": new_status, "updated_at": now_utc()}}
        )
        if result > 0:
            self.logger.info(f"Signal {signal_id} updated to {new_status}.")
            return True
        else:
            self.logger.error(f"Signal {signal_id} not found.")
            return False

    @exception_handler
    async def retrieve_active_signals(self, symbol: str, timeframe: Timeframe, direction: TradingDirection, current_time: datetime):
        try:
            active_signals = list(self.collection.find({
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "candle_close_time": {"$gt": dt_to_unix(current_time)}
            }))
            return active_signals
        except Exception as e:
            self.logger.error(f"Error retrieving active signals: {e}")
            return []

    @exception_handler
    async def start(self):
        if not await self.db_service.test_connection():
            raise Exception("Unable to connect to MongoDB instance.")

        self.db_service.connect()
        self.collection = self.db_service.create_index(collection=self.collection_name, index_field="signal_id", unique=True)

    @exception_handler
    async def stop(self):
        await self.db_service.disconnect()
