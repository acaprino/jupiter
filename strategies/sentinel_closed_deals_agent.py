import asyncio
import uuid
from typing import Optional, List

from brokers.broker_proxy import Broker
from dto.Position import Position
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange, OrderSource
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from notifiers.closed_deals_manager import ClosedDealsManager
from services.rabbitmq_service import RabbitMQService


class ClosedDealsAgent:

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        self.config = config
        self.agent = "Closed deals agent"
        self.trading_configs = trading_configs
        self.client_registered_event = asyncio.Event()
        self.logger = BotLogger.get_logger(name=f"{self.config.get_bot_name()}_ClosedDealsAgent", level=config.get_bot_logging_level())
        self.countries_of_interest = {}
        self.clients_registrations = {}
        self.topics = list(
            {f"{config.symbol}.{config.timeframe}.{config.trading_direction}" for config in trading_configs}
        )
        symbol_map = {}
        for config in self.trading_configs:
            symbol = config.symbol
            telegram_config = config.telegram_config
            if symbol not in symbol_map:
                symbol_map[symbol] = set()
            symbol_map[symbol].add(telegram_config)
        self.symbols_to_telegram_configs = {symbol: list(configs) for symbol, configs in symbol_map.items()}

    @exception_handler
    async def routine_start(self):

        symbols = {config.symbol for config in self.trading_configs}

        for symbol, symbol_to_telegram_configs in self.symbols_to_telegram_configs.items():
            self.clients_registrations[symbol] = {}
            for telegram_config in symbol_to_telegram_configs:
                client_id = str(uuid.uuid4())
                self.clients_registrations[symbol][client_id] = telegram_config

                self.logger.info(f"Sending client registration message with id {client_id}")
                registration_payload = to_serializable(telegram_config)
                registration_payload["routine_id"] = client_id

                await RabbitMQService.register_listener(
                    exchange_name=RabbitExchange.REGISTRATION_ACK.name,
                    callback=self.on_client_registration_ack,
                    routing_key=client_id,
                    exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

                self.client_registered_event.clear()
                await self.send_queue_message(exchange=RabbitExchange.REGISTRATION,
                                              routing_key=RabbitExchange.REGISTRATION.routing_key,
                                              symbol=symbol,
                                              payload=registration_payload,
                                              recipient="middleware")

                try:
                    await asyncio.wait_for(self.client_registered_event.wait(), timeout=60)
                    self.logger.info(f"ACK received for {client_id}!")
                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout while waiting for ACK for {client_id}.")

        for symbol in symbols:
            self.logger.info(f"Listening for closed deals on {symbol}.")
            await ClosedDealsManager().register_observer(
                symbol,
                self.config.get_bot_magic_number(),
                Broker(),
                self.on_deal_closed,
                self.agent
            )

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        self.logger.info(f"Client with id {routing_key} successfully registered, calling registration callback.")
        self.client_registered_event.set()

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 symbol: str,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):
        self.logger.info(f"Publishing event message: {payload}")

        recipient = recipient if recipient is not None else "middleware"

        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        tc = {"symbol": symbol, "timeframe": None, "trading_direction": None, "bot_name": self.config.get_bot_name()}
        await RabbitMQService.publish_message(exchange_name=exchange_name,
                                              message=QueueMessage(sender=self.agent, payload=payload, recipient=recipient, trading_configuration=tc),
                                              routing_key=routing_key,
                                              exchange_type=exchange_type)

    @exception_handler
    async def on_deal_closed(self, position: Position):
        filtered_deals = list(filter(lambda deal: deal.order_source in {OrderSource.STOP_LOSS, OrderSource.TAKE_PROFIT, OrderSource.MANUAL, OrderSource.BOT}, position.deals))

        if not filtered_deals:
            self.logger.info(f"No stop loss or take profit deals found for position {position.position_id}")
            return

        closing_deal = max(filtered_deals, key=lambda deal: deal.time)

        emoji = "🤑" if position.profit > 0 else "😔"

        trade_details = (
            f"<b>Position ID:</b> {position.position_id}\n"
            f"<b>Timestamp:</b> {closing_deal.time.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"<b>Market:</b> {position.symbol}\n"
            f"<b>Volume:</b> {closing_deal.volume}\n"
            f"<b>Price:</b> {closing_deal.execution_price}\n"
            f"<b>Order source:</b> {closing_deal.order_source.name}\n"
            f"<b>Profit:</b> {closing_deal.profit}\n"
            f"<b>Commission:</b> {position.commission}\n"
            f"<b>Swap:</b> {position.swap}"
        )

        await self.send_message_to_all_clients_for_symbol(
            f"{emoji} <b>Deal closed</b>\n\n{trade_details}"
        )

    @exception_handler
    async def send_message_to_all_clients_for_symbol(self, message: str, symbol: str):
        self.logger.info(f"Publishing event message {message} for symbol {symbol}")
        for client_id, client in self.clients_registrations[symbol].items():
            await self.send_queue_message(exchange=RabbitExchange.NOTIFICATIONS, payload={"message": message}, symbol=symbol, routing_key=client_id)
