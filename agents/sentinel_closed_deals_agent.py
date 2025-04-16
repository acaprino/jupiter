import random

from typing import List
from agents.agent_symbol_unified_notifier import SymbolUnifiedNotifier
from dto.Position import Position
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import OrderSource
from misc_utils.error_handler import exception_handler
from notifiers.notifier_closed_deals import ClosedDealsNotifier


class ClosedDealsAgent(SymbolUnifiedNotifier):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Closed deals agent", config, trading_configs)

    @exception_handler
    async def start(self):
        c_deals_notif = await ClosedDealsNotifier.get_instance(self.config)
        for symbol in self.symbols:
            self.info(f"Listening for market state change for {symbol}.")
            await c_deals_notif.register_observer(
                symbol=symbol,
                callback=self.on_deal_closed,
                observer_id=self.agent,
                magic_number=None
            )

    @exception_handler
    async def stop(self):
        pass

    @exception_handler
    async def on_deal_closed(self, position: Position):
        filtered_deals = list(filter(lambda deal: deal.order_source in {OrderSource.STOP_LOSS, OrderSource.TAKE_PROFIT, OrderSource.MANUAL, OrderSource.BOT}, position.deals))

        if not filtered_deals:
            self.info(f"No stop loss or take profit deals found for position {position.position_id}")
            return

        closing_deal = max(filtered_deals, key=lambda deal: deal.time)

        if position.profit > 0:
            emoji = random.choice(["🤑", "🚀", "😀", "💰", "🎉", "🥂", "🔝"])
        elif position.profit < 0:
            emoji = random.choice(["😔", "💥", "😐", "👎", "😢", "😟", "😩", "🤯", "💔", "🔻"])
        else:
            emoji = random.choice(["😐", "😶", "➖"])

        def format_number(value, fmt: str = ".2f", default: str = "N/A"):
            return f"{value:{fmt}}" if value is not None else default

        trade_details = (
            f"🆔 ├─ <b>Position ID:</b> {position.position_id}\n"
            f"⏰ ├─ <b>Timestamp:</b> {closing_deal.time.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"💱 ├─ <b>Market:</b> {position.symbol}\n"
            f"📊 ├─ <b>Volume:</b> {format_number(closing_deal.volume)}\n"
            f"💵 ├─ <b>Price:</b> {format_number(closing_deal.execution_price)}\n"
            f"🔧 ├─ <b>Order source:</b> {closing_deal.order_source.name}\n"
            f"📈 ├─ <b>Profit:</b> {format_number(closing_deal.profit)}\n"
            f"💸 ├─ <b>Commission:</b> {format_number(position.commission)}\n"
            f"🔁 └─  {format_number(position.swap)}"
        )

        await self.send_message_to_all_clients_for_symbol(
            message=f"{emoji} <b>Deal closed</b>\n\n{trade_details}",
            symbol=position.symbol)
