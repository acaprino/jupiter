import asyncio

from brokers.broker_interface import BrokerAPI
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from notifiers.economic_event_notifier import EconomicEventNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from routines.base_routine import BaseRoutine
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_strategy import Adrastea


class GeneratorRoutine(BaseRoutine):

    def __init__(self, worker_id: str, config: ConfigReader, trading_config: TradingConfiguration, broker: BrokerAPI, queue_service: RabbitMQService):
        super().__init__(worker_id, config.get_bot_logging_level(), to_serializable(trading_config), queue_service)
        self.trading_config = trading_config
        self.execution_lock = asyncio.Lock()
        self.broker = broker

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
        await self.wait_client_registration()
        await self.strategy.start()
        asyncio.create_task(self.strategy.bootstrap())
        await self.market_state_notifier.start()
        await self.tick_notifier.start()
        self.logger.info(f"Generator {self.worker_id} started")

    @exception_handler
    async def stop(self):
        await self.strategy.shutdown()
        await self.market_state_notifier.stop()
        await self.tick_notifier.stop()
        await self.broker.shutdown()
