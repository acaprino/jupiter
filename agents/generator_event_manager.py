from typing import List
from agents.agent_symbol_unified_notifier import SymbolUnifiedNotifier
from dto.EconomicEvent import get_symbol_countries_of_interest, EconomicEvent, EventImportance
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from notifiers.notifier_economic_events import NotifierEconomicEvents


class EconomicEventsEmitterAgent(SymbolUnifiedNotifier):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Economic events manager agent", config, trading_configs)
        self.countries_of_interest: List[str] = []

    @exception_handler
    async def start(self):
        all_countries = set()
        for tc in self.trading_configs:
            symbol = tc.get_symbol()
            countries = await get_symbol_countries_of_interest(symbol)
            all_countries.update(countries)
        self.countries_of_interest = list(all_countries)

        e_events_notif = await NotifierEconomicEvents.get_instance(self.config)
        await e_events_notif.register_observer(
            self.countries_of_interest,
            self.on_economic_event,
            self.id,
            EventImportance.HIGH
        )

    @exception_handler
    async def stop(self):
        e_events_notif = await NotifierEconomicEvents.get_instance(self.config)
        await e_events_notif.unregister_observer(
            self.countries_of_interest,
            EventImportance.HIGH,
            self.id
        )


    @exception_handler
    async def on_economic_event(self, event: EconomicEvent):
        """
        Handle an economic event by sending its data to the appropriate RabbitMQ exchange.
        """
        self.info(f"Economic event received: {event.to_json()}")
        routing_key = f"event.economic"
        await self.send_queue_message(exchange=RabbitExchange.jupiter_events, payload=to_serializable(event), routing_key=routing_key)
