import asyncio

from brokers.broker_interface import BrokerAPI
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from notifiers.economic_event_notifier import EconomicEventNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_strategy import Adrastea


class GeneratorRoutine:

    def __init__(self, worker_id: str, config: ConfigReader, trading_config: TradingConfiguration, broker: BrokerAPI):
        # self.topic = f"{trading_config.get_symbol()}_{trading_config.get_timeframe().name}_{trading_config.get_trading_direction().name}"
        self.worker_id = worker_id
        self.logger = BotLogger.get_logger(name=f"{self.worker_id}", level=config.get_bot_logging_level().upper())
        self.execution_lock = asyncio.Lock()
        self.broker = broker

        # Initialize the MarketStateNotifier
        self.tick_notifier = TickNotifier(worker_id=self.worker_id, timeframe=trading_config.get_timeframe(), execution_lock=self.execution_lock)
        self.market_state_notifier = MarketStateNotifier(worker_id=self.worker_id, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)
        self.economic_event_notifier = EconomicEventNotifier(worker_id=self.worker_id, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)

        self.publisher = RabbitMQService(
            worker_id=self.worker_id,
            user=config.get_rabbitmq_username(),
            password=config.get_rabbitmq_password(),
            rabbitmq_host=config.get_rabbitmq_host(),
            port=config.get_rabbitmq_port())

        # Instantiate the strategy
        self.strategy = Adrastea(worker_id=self.worker_id, broker=self.broker, publisher=self.publisher, config=config, trading_config=trading_config, execution_lock=self.execution_lock)

        # Register event handlers
        self.tick_notifier.register_on_new_tick(self.strategy.on_new_tick)
        self.market_state_notifier.register_on_market_status_change(self.strategy.on_market_status_change)
        self.economic_event_notifier.register_on_economic_event(self.strategy.on_economic_event)

    @exception_handler
    async def start(self):
        # Execute the strategy bootstrap method
        await self.strategy.start()
        asyncio.create_task(self.strategy.bootstrap())
        await self.market_state_notifier.start()
        await self.tick_notifier.start()
        await self.publisher.start()
        self.logger.info(f"Signal generator {self.worker_id} started")

    @exception_handler
    async def stop(self):
        await self.publisher.stop()
        await self.strategy.shutdown()
        await self.market_state_notifier.stop()
        await self.tick_notifier.stop()
        await self.broker.shutdown()
