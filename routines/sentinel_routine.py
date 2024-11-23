import asyncio

from brokers.broker_interface import BrokerAPI
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from notifiers.closed_positions_notifier import ClosedDealsNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_sentinel import AdrasteaSentinel


class SentinelRoutine:

    def __init__(self, worker_id: str, config: ConfigReader, trading_config: TradingConfiguration, broker: BrokerAPI, queue_service: RabbitMQService):
        self.topic = f"{trading_config.get_symbol()}.{trading_config.get_timeframe().name}.{trading_config.get_trading_direction().name}"
        self.worker_id = worker_id
        self.trading_config = trading_config
        self.logger = BotLogger.get_logger(name=f"{self.worker_id}", level=config.get_bot_logging_level().upper())
        self.execution_lock = asyncio.Lock()
        self.broker = broker
        self.queue_service = queue_service

        # Initialize the ClosedPositionNotifier
        self.closed_deals_notifier = ClosedDealsNotifier(worker_id=self.worker_id,
                                                         broker=self.broker,
                                                         symbol=trading_config.get_symbol(),
                                                         magic_number=config.get_bot_magic_number(),
                                                         execution_lock=self.execution_lock)
        self.market_state_notifier = MarketStateNotifier(worker_id=self.worker_id, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)

        self.events_handler = AdrasteaSentinel(worker_id=self.worker_id, config=config, trading_config=trading_config, broker=self.broker, queue_service=queue_service)

        # Register event handlers
        self.closed_deals_notifier.register_on_deal_status_notifier(self.events_handler.on_deal_closed)
        self.market_state_notifier.register_on_market_status_change(self.events_handler.on_market_status_change)

    @exception_handler
    async def start(self):
        # Execute the strategy bootstrap method
        self.logger.info(f"Order placer started for {self.topic}.")
        await self.closed_deals_notifier.start()
        await self.market_state_notifier.start()
        await self.events_handler.start()

        client_registration_message = QueueMessage(
            sender=self.worker_id,
            payload=to_serializable(self.trading_config.get_telegram_config()),
            recipient="middleware")
        await self.queue_service.publish_message(exchange_name=RabbitExchange.REGISTRATION.name,
                                                 exchange_type=RabbitExchange.REGISTRATION.exchange_type,
                                                 routing_key=RabbitExchange.REGISTRATION.routing_key,
                                                 message=client_registration_message)

        exchange_name, exchange_type = RabbitExchange.SIGNALS_CONFIRMATIONS.name, RabbitExchange.SIGNALS_CONFIRMATIONS.exchange_type
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.events_handler.on_signal_confirmation,
            routing_key=self.topic,
            exchange_type=exchange_type)

        exchange_name, exchange_type = RabbitExchange.ENTER_SIGNAL.name, RabbitExchange.ENTER_SIGNAL.exchange_type
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.events_handler.on_enter_signal,
            routing_key=self.topic,
            exchange_type=exchange_type)

    @exception_handler
    async def stop(self):
        await self.closed_deals_notifier.stop()
        await self.market_state_notifier.stop()
