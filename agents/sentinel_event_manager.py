from collections import defaultdict
from typing import List

from agents.agent_symbol_unified_notifier import SymbolUnifiedNotifier
from brokers.broker_proxy import Broker
from dto.EconomicEvent import get_symbol_countries_of_interest, EconomicEvent
from dto.Position import Position
from dto.QueueMessage import QueueMessage
from dto.RequestResult import RequestResult
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc
from services.service_rabbitmq import RabbitMQService


class EconomicEventsManagerAgent(SymbolUnifiedNotifier):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Economic events manager agent", config, trading_configs)
        self.countries_of_interest = defaultdict(list)

    @exception_handler
    async def start(self):
        all_countries = set()
        for tc in self.trading_configs:
            symbol = tc.get_symbol()
            countries = await get_symbol_countries_of_interest(symbol)
            all_countries.update(countries)
        self.countries_of_interest = list(all_countries)

        self.info(f"Listening for economic events.")
        routing_key = f"event.economic#"
        exchange_name, exchange_type = RabbitExchange.jupiter_events.name, RabbitExchange.jupiter_events.exchange_type
        rabbitmq_s = await RabbitMQService.get_instance()
        await rabbitmq_s.register_listener(
            exchange_name=exchange_name,
            callback=self.on_economic_event,
            routing_key=routing_key,
            exchange_type=exchange_type)

    @exception_handler
    async def stop(self):
        pass

    @exception_handler
    async def on_economic_event(self, routing_key: str, message: QueueMessage):
        self.info(f"Received economic event: {message.payload}")
        broker = Broker()
        event = EconomicEvent.from_json(message.payload)

        event_country = event.country

        event_has_impact = all(event_country in symbol_countries_of_interest for symbol_countries_of_interest in self.countries_of_interest.values())

        if not event_has_impact:
            return

        event_name = event.name
        total_seconds = (event.time - now_utc()).total_seconds()
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)

        # Display result
        if minutes == 0 and seconds == 0:
            when_str = "now."
        elif seconds == 0:
            when_str = f"in {minutes} minutes."
        else:
            when_str = f"in {minutes} minutes and {seconds} seconds."

        message = (
            f"📰🔔 Economic event <b>{event_name}</b> is scheduled to occur {when_str}\n"
        )

        impacted_symbols = [symbol for symbol, symbol_countries_of_interest in self.countries_of_interest.items() if event_country in symbol_countries_of_interest]

        for impacted_symbol in impacted_symbols:
            await self.send_message_to_all_clients_for_symbol(message, impacted_symbol)

        for impacted_symbol in impacted_symbols:

            positions: List[Position] = await broker.get_open_positions(symbol=impacted_symbol)

            if not positions:
                message = f"ℹ️ No open positions found for forced closure due to the economic event <b>{event_name}</b>."
                self.warning(message)
                await self.send_message_to_all_clients_for_symbol(message, impacted_symbol)
            else:
                for position in positions:
                    magic_number = position.deals[0].magic_number
                    # Attempt to close the position
                    result: RequestResult = await broker.close_position(position=position, comment=f"'{event_name}'", magic_number=magic_number)
                    if result and result.success:
                        message = (
                            f"✅ Position {position.position_id} closed successfully due to the economic event <b>{event_name}</b>.\n"
                            f"ℹ️ This action was taken to mitigate potential risks associated with the event's impact on the markets."
                        )
                    else:
                        message = (
                            f"❌ Failed to close position {position.position_id} due to the economic event <b>{event_name}</b>.\n"
                            f"⚠️ Potential risks remain as the position could not be closed."
                        )
                    self.info(message)
                    await self.send_message_to_all_clients_for_symbol(
                        message=message,
                        symbol=impacted_symbol)
