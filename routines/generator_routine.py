import asyncio

from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from notifiers.economic_event_notifier import EconomicEventNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from routines.base_routine import BaseRoutine
from strategies.adrastea_strategy import Adrastea


class GeneratorRoutine(BaseRoutine):

    def __init__(self, routine_label: str, config: ConfigReader, trading_config: TradingConfiguration):
        super().__init__(routine_label=routine_label, config=config, trading_config=trading_config)
        self.execution_lock = asyncio.Lock()

        # Initialize the MarketStateNotifier
        self.tick_notifier = TickNotifier(routine_label=self.routine_label, timeframe=trading_config.get_timeframe(), execution_lock=self.execution_lock)
        self.market_state_notifier = MarketStateNotifier(routine_label=self.routine_label, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)
        self.economic_event_notifier = EconomicEventNotifier(routine_label=self.routine_label, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)

        # Instantiate the strategy
        self.strategy = Adrastea(routine_label=self.routine_label, id=self.id, broker=self.broker, config=config, trading_config=trading_config,
                                 execution_lock=self.execution_lock)

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
        self.logger.info(f"Generator {self.routine_label} started")

    @exception_handler
    async def stop(self):
        await self.strategy.shutdown()
        await self.market_state_notifier.stop()
        await self.tick_notifier.stop()
        await self.broker.shutdown()
