import asyncio
import math
from typing import Optional, List

from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.QueueMessage import QueueMessage
from dto.RequestResult import RequestResult
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Timeframe, TradingDirection, OpType, OrderSource, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum, round_to_point, round_to_step, unix_to_datetime, extract_properties, now_utc
from notifiers.closed_deals_manager import ClosedDealsManager
from routines.base_routine import RagistrationAwareRoutine
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_strategy import supertrend_slow_key


class AdrasteaSentinelEventManager():

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        self.config = config
        self.trading_configs = trading_configs
        # Initialize the logger
        self.logger = BotLogger.get_logger(name=f"{self.config.get_bot_name()}_SentinelEventManager", level=config.get_bot_logging_level())

        self.topics = list(
            {f"{config.symbol}.{config.timeframe}.{config.trading_direction}" for config in trading_configs}
        )

    @exception_handler
    async def routine_start(self):
        for topic in self.topics:
            self.logger.info(f"Listening for economic events on {topic}.")
            exchange_name, exchange_type = RabbitExchange.ECONOMIC_EVENTS.name, RabbitExchange.ECONOMIC_EVENTS.exchange_type
            await RabbitMQService.register_listener(
                exchange_name=exchange_name,
                callback=self.on_economic_event,
                routing_key=topic,
                exchange_type=exchange_type)

    @exception_handler
    async def on_economic_event(self, routing_key: str, message: QueueMessage):
        print(f"Received economic event: {message.payload}")
