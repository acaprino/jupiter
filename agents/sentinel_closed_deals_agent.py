from typing import List

from agents.agent_unique_symbol import SymbolFlatAgent
from brokers.broker_proxy import Broker
from dto.Position import Position
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import OrderSource
from misc_utils.error_handler import exception_handler
from notifiers.notifier_closed_deals import ClosedDealsNotifier


class ClosedDealsAgent(SymbolFlatAgent):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Closed deals agent", config, trading_configs)

    @exception_handler
    async def start(self):
        for symbol in self.symbols:
            self.logger.info(f"Listening for closed deals on {symbol}.")
            await ClosedDealsNotifier().register_observer(
                symbol,
                self.config.get_bot_magic_number(),
                Broker(),
                self.on_deal_closed,
                self.agent
            )

    @exception_handler
    async def stop(self):
        pass

    @exception_handler
    async def registration_ack(self, symbol, telegram_configs):
        pass

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
            f"{emoji} <b>Deal closed</b>\n\n{trade_details}", position.symbol
        )
