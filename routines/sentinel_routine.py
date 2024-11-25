import asyncio

from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from notifiers.closed_positions_notifier import ClosedDealsNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from routines.base_routine import BaseRoutine
from services.rabbitmq_service import RabbitMQService
from strategies.adrastea_sentinel import AdrasteaSentinel


class SentinelRoutine(BaseRoutine):

    def __init__(self, routine_label: str, config: ConfigReader, trading_config: TradingConfiguration):
        super().__init__(routine_label=routine_label, config=config, trading_config=trading_config)
        self.topic = f"{trading_config.get_symbol()}.{trading_config.get_timeframe().name}.{trading_config.get_trading_direction().name}"
        self.execution_lock = asyncio.Lock()

        # Initialize the ClosedPositionNotifier
        self.closed_deals_notifier = ClosedDealsNotifier(routine_label=self.routine_label,
                                                         broker=self.broker,
                                                         symbol=trading_config.get_symbol(),
                                                         magic_number=config.get_bot_magic_number(),
                                                         execution_lock=self.execution_lock)

        self.sentinel = AdrasteaSentinel(routine_label=self.routine_label, id=self.id, config=config, trading_config=trading_config, broker=self.broker)

        # Register event handlers
        self.closed_deals_notifier.register_on_deal_status_notifier(self.sentinel.on_deal_closed)

        self.market_state_notifier = MarketStateNotifier(routine_label=self.routine_label, broker=self.broker, symbol=trading_config.get_symbol(), execution_lock=self.execution_lock)
        self.market_state_notifier.register_on_market_status_change(self.sentinel.on_market_status_change)

    @exception_handler
    async def start(self):
        await self.sentinel.start()
        await self.wait_client_registration()
        await self.closed_deals_notifier.start()
        await self.market_state_notifier.start()

        exchange_name, exchange_type = RabbitExchange.SIGNALS_CONFIRMATIONS.name, RabbitExchange.SIGNALS_CONFIRMATIONS.exchange_type
        await RabbitMQService.register_listener(
            exchange_name=exchange_name,
            callback=self.sentinel.on_signal_confirmation,
            routing_key=self.topic,
            exchange_type=exchange_type)

        exchange_name, exchange_type = RabbitExchange.ENTER_SIGNAL.name, RabbitExchange.ENTER_SIGNAL.exchange_type
        await RabbitMQService.register_listener(
            exchange_name=exchange_name,
            callback=self.sentinel.on_enter_signal,
            routing_key=self.topic,
            exchange_type=exchange_type)

        exchange_name, exchange_type = RabbitExchange.ECONOMIC_EVENTS.name, RabbitExchange.ECONOMIC_EVENTS.exchange_type
        await RabbitMQService.register_listener(
            exchange_name=exchange_name,
            callback=self.sentinel.on_economic_event,
            routing_key=self.topic,
            exchange_type=exchange_type)

    @exception_handler
    async def stop(self):
        await self.closed_deals_notifier.stop()
        await self.market_state_notifier.stop()
