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

        trade_details = (
            f"🆔 ├─ <b>Position ID:</b> {position.position_id}\n"
            f"⏰ ├─ <b>Timestamp:</b> {deal.time.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"💱 ├─ <b>Market:</b> {position.symbol}\n"
            f"📊 ├─ <b>Volume:</b> {deal.volume:.2f}\n"
            f"💵 ├─ <b>Price:</b> {deal.execution_price:.2f}\n"
            f"🔧 ├─ <b>Order source:</b> {deal.order_source.name}\n"
            f"🔁 ├─ <b>Swap:</b> {position.swap:.2f}\n"
            f"✨ └─ <b>Magic Number:</b> {deal.magic_number if deal.magic_number is not None else '-'}"
        )

        # for tc in self.config.get_trading_configurations():
        #    if tc.get_magic_number() == deal.magic_number:
        #        trade_details += "\n"
        #        trade_details += f"💻 <b>Bot:</b> {self.config.get_bot_name()}\n"
        #        trade_details += f"💱 ├─ <b>Symbol:</b> {tc.get_symbol()}\n"
        #        trade_details += f"📊 ├─ <b>Timeframe:</b> {tc.get_timeframe().name}\n"
        #        direction_emoji = "📈" if tc.get_trading_direction().name == "LONG" else "📉"
        #        trade_details += f"{direction_emoji} └─ <b>Direction:</b> {tc.get_trading_direction().name}\n"
        #        break

        await self.send_message_to_all_clients_for_symbol(
            message=f"{emoji} <b>Order filled</b>\n\n{trade_details}",
            symbol=position.symbol)
