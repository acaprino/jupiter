from typing import List
from agents.agent_symbol_unified_notifier import SymbolUnifiedNotifier
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import unix_to_datetime
from notifiers.notifier_market_state import NotifierMarketState


class MarketStateNotifierAgent(SymbolUnifiedNotifier):

    def __init__(self, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        super().__init__("Market state notifier agent", config, trading_configs)

    @exception_handler
    async def start(self):
        m_state_notif = await NotifierMarketState.get_instance(self.config)
        for symbol in self.symbols:
            self.info(f"Listening for market state change for {symbol}.")
            await m_state_notif.register_observer(
                symbol,
                self.on_market_status_change,
                self.id
            )

    @exception_handler
    async def stop(self):
        pass

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        time_ref = opening_time if is_open else closing_time
        self.info(f"Market for {symbol} has {'opened' if is_open else 'closed'} at {unix_to_datetime(time_ref)}.")

        open_prefix = "🟢" + ("" if initializing else "⏰")
        close_prefix = "⏸️" + ("" if initializing else "🌙")

        if is_open:
            if initializing:
                message = f"{open_prefix} Market for {symbol} is <b>open</b> on broker."
            else:
                message = f"{open_prefix} Market for {symbol} has just <b>opened</b> on broker. Resuming trading activities."
        else:
            if initializing:
                message = f"{close_prefix} Market for {symbol} is <b>closed</b> on broker."
            else:
                message = f"{close_prefix} Market for {symbol} has just <b>closed</b> on broker. Pausing trading activities."

        await self.request_broadcast_notification(
            message_content=message,
            symbol=symbol
        )
