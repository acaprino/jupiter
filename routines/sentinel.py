import asyncio

from brokers.broker_interface import BrokerAPI
from dto.Position import Position
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from notifiers.closed_positions_notifier import ClosedDealsNotifier


class SentinelRoutine:

    def __init__(self, worker_id: str, config: ConfigReader, trading_config: TradingConfiguration, broker: BrokerAPI):
        self.topic = f"{trading_config.get_symbol()}_{trading_config.get_timeframe().name}_{trading_config.get_trading_direction().name}"
        self.worker_id = worker_id
        self.logger = BotLogger.get_logger(name=f"{self.worker_id}", level=config.get_bot_logging_level().upper())
        self.execution_lock = asyncio.Lock()
        self.broker = broker

        # Initialize the ClosedPositionNotifier
        self.closed_deals_notifier = ClosedDealsNotifier(worker_id=self.worker_id,
                                                         broker=self.broker,
                                                         symbol=trading_config.get_symbol(),
                                                         magic_number=config.get_bot_magic_number(),
                                                         execution_lock=self.execution_lock)



        # Register event handlers
        self.closed_deals_notifier.register_on_deal_status_notifier(self.on_deal_closed)

    async def on_deal_closed(self, position: Position):
        pass

    @exception_handler
    async def start(self):
        # Execute the strategy bootstrap method
        self.logger.info(f"Order placer started for {self.topic}.")

        await self.closed_deals_notifier.start()

    @exception_handler
    async def stop(self):
        await self.closed_deals_notifier.stop()
