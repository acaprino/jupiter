import asyncio
import uuid

from brokers.broker_interface import BrokerAPI
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from notifiers.economic_event_notifier import EconomicEventNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_strategy import Adrastea


class GeneratorRoutine:

    def __init__(self, worker_id: str, config: ConfigReader, trading_config: TradingConfiguration, broker: BrokerAPI, queue_service: RabbitMQService):
        # self.topic = f"{trading_config.get_symbol()}_{trading_config.get_timeframe().name}_{trading_config.get_trading_direction().name}"
        self.worker_id = worker_id
        self.trading_config = trading_config
        self.id = str(uuid.uuid4())
        self.logger = BotLogger.get_logger(name=f"{self.worker_id}", level=config.get_bot_logging_level().upper())
        self.execution_lock = asyncio.Lock()
        self.broker = broker
        self.queue_service = queue_service
        self.client_registered_event = asyncio.Event()

        # Initialize the MarketStateNotifier
        self.tick_notifier = TickNotifier(worker_id=self.worker_id, timeframe=trading_config.get_timeframe(), execution_lock=self.execution_lock)
        self.market_state_notifier = MarketStateNotifier(worker_id=self.worker_id, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)
        self.economic_event_notifier = EconomicEventNotifier(worker_id=self.worker_id, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)

        # Instantiate the strategy
        self.strategy = Adrastea(worker_id=self.worker_id, broker=self.broker, queue_service=self.queue_service, config=config, trading_config=trading_config, execution_lock=self.execution_lock)

        # Register event handlers
        self.tick_notifier.register_on_new_tick(self.strategy.on_new_tick)
        self.market_state_notifier.register_on_market_status_change(self.strategy.on_market_status_change)
        self.economic_event_notifier.register_on_economic_event(self.strategy.on_economic_event)

    @exception_handler
    async def start(self):
        # Execute the strategy bootstrap method
        await self.queue_service.register_listener(
            exchange_name=RabbitExchange.REGISTRATION_ACK.name,
            callback=self.on_client_registration_ack,
            routing_key=self.id,
            exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

        registration_payload = to_serializable(self.trading_config.get_telegram_config())
        registration_payload['sentinel_id'] = self.id
        client_registration_message = QueueMessage(
            sender=self.worker_id,
            payload=registration_payload,
            recipient="middleware")
        await self.queue_service.publish_message(exchange_name=RabbitExchange.REGISTRATION.name,
                                                 exchange_type=RabbitExchange.REGISTRATION.exchange_type,
                                                 routing_key=RabbitExchange.REGISTRATION.routing_key,
                                                 message=client_registration_message)

        await self.client_registered_event.wait()
        await self.strategy.start()
        asyncio.create_task(self.strategy.bootstrap())
        await self.market_state_notifier.start()
        await self.tick_notifier.start()
        self.logger.info(f"Signal generator {self.worker_id} started")

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        self.client_registered_event.set()

    @exception_handler
    async def stop(self):
        await self.strategy.shutdown()
        await self.market_state_notifier.stop()
        await self.tick_notifier.stop()
        await self.broker.shutdown()
