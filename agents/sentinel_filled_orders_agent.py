import random

from typing import List
from agents.agent_symbol_unified_notifier import SymbolUnifiedNotifier
from dto.Position import Position
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from notifiers.notifier_filled_orders import FilledOrdersNotifier


class FilledOrdersAgent(SymbolUnifiedNotifier):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Closed deals agent", config, trading_configs)

    @exception_handler
    async def start(self):
        c_deals_notif = await FilledOrdersNotifier.get_instance(self.config)
        for symbol in self.symbols:
            self.info(f"Listening for market state change for {symbol}.")
            await c_deals_notif.register_observer(
                symbol=symbol,
                callback=self.on_order_filled,
                observer_id=self.agent,
                magic_number=None
            )

    @exception_handler
    async def stop(self):
        pass

    @exception_handler
    async def on_order_filled(self, position: Position):
        deal = position.deals[0]
        emoji = random.choice(["🚀", "🎯", "😀", "🎉", "📤", "🆗"])

        def format_number(value, fmt: str = ".2f", default: str = "N/A"):
            return f"{value:{fmt}}" if value is not None else default

        trade_details = (
            f"🆔 ├─ <b>Position ID:</b> {position.position_id}\n"
            f"⏰ ├─ <b>Timestamp:</b> {deal.time.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"💱 ├─ <b>Market:</b> {position.symbol}\n"
            f"📊 ├─ <b>Volume:</b> {format_number(deal.volume)}\n"
            f"💵 ├─ <b>Price:</b> {format_number(deal.execution_price)}\n"
            f"🔧 ├─ <b>Order source:</b> {deal.order_source.name}\n"
            f"🔁 ├─ <b>Swap:</b> {format_number(position.swap)}\n"
            f"✨ └─ <b>Magic Number:</b> {deal.magic_number if deal.magic_number is not None else '-'}"
        )

        await self.send_message_to_all_clients_for_symbol(
            message=f"{emoji} <b>Order filled</b>\n\n{trade_details}",
            symbol=position.symbol)
