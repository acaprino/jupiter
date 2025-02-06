import asyncio
from typing import Optional, List

from agents.agent_registration_aware import RegistrationAwareAgent
from dto.OrderRequest import OrderRequest
from dto.QueueMessage import QueueMessage
from dto.Signal import Signal
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Timeframe, TradingDirection, OpType, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum, round_to_point, round_to_step, unix_to_datetime, extract_properties
from notifiers.notifier_closed_deals import ClosedDealsNotifier
from services.service_rabbitmq import RabbitMQService
from services.service_signal_persistence import SignalPersistenceService


class ExecutorAgent(RegistrationAwareAgent):

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        super().__init__(config, trading_config)
        self.signal_confirmations: List[Signal] = []
        self.market_open_event = asyncio.Event()

        # >>> Istanzia il persistence manager <<<
        self.persistence_manager = SignalPersistenceService(self.config)

    @exception_handler
    async def start(self):
        self.info(f"Events handler started for {self.topic}.")

        # >>> Avvia il persistence manager (connessione a Mongo, creazione indici, ecc.) <<<
        await self.persistence_manager.start()

        # >>> Carica i segnali esistenti da MongoDB (se serve filtrare per symbol/timeframe/direction) e la cui chiusura della candela è inferiore ad adesso - timeframe <<<
        symbol = self.trading_config.get_symbol()
        timeframe = self.trading_config.get_timeframe()
        direction = self.trading_config.get_trading_direction()

        loaded_signals = await self.persistence_manager.retrieve_active_signals(symbol, timeframe, direction, self.agent)
        self.signal_confirmations = [Signal.from_json(signal) for signal in (loaded_signals or [])]

        self.info(f"Listening for signals and confirmations on {self.topic}.")
        await RabbitMQService.register_listener(
            exchange_name=RabbitExchange.SIGNALS_CONFIRMATIONS.name,
            callback=self.on_signal_confirmation,
            routing_key=self.topic,
            exchange_type=RabbitExchange.SIGNALS_CONFIRMATIONS.exchange_type
        )
        self.info(f"Listening for market enter signals on {self.topic}.")
        await RabbitMQService.register_listener(
            exchange_name=RabbitExchange.ENTER_SIGNAL.name,
            callback=self.on_enter_signal,
            routing_key=self.topic,
            exchange_type=RabbitExchange.ENTER_SIGNAL.exchange_type
        )
        self.info(f"Listening for closed deals on {self.trading_config.get_symbol()}.")

    @exception_handler
    async def stop(self):
        self.info(f"Events handler stopped for {self.topic}.")
        await ClosedDealsNotifier(self.config).unregister_observer(self.trading_config.get_symbol(), self.config.get_bot_magic_number(), self.id)

    @exception_handler
    async def on_signal_confirmation(self, router_key: str, message: QueueMessage):
        signal = Signal.from_json(message.get("signal"))

        self.info(f"Received signal confirmation: {signal}")

        candle_open_time = signal.candle.get("time_open")
        candle_close_time = signal.candle.get("time_close")

        # Check if an older confirmation exists
        existing_confirmation = next(
            (conf for conf in self.signal_confirmations
             if
             conf.symbol == signal.symbol
             and conf.timeframe == signal.timeframe
             and conf.direction == signal.direction
             and conf.candle['time_open'] == candle_open_time
             and conf.candle['time_close'] == candle_close_time),
            None
        )

        if existing_confirmation:
            # Compare confirmation times and update if the new one is more recent
            if (not existing_confirmation.update_tms and signal.update_tms) or (signal.update_tms > existing_confirmation.update_tms):
                self.info(f"Updating older confirmation for {signal.symbol} - {signal.timeframe} - {candle_open_time} - {candle_close_time}")
                self.signal_confirmations.remove(existing_confirmation)
                self.signal_confirmations.append(signal)
            else:
                self.info(f"Received older confirmation ignored for {signal.symbol} - {signal.timeframe}")
        else:
            # Add the new confirmation if none exists
            self.info(f"Adding new confirmation for {signal.symbol} {signal.timeframe}")
            self.signal_confirmations.append(signal)

    @exception_handler
    async def on_enter_signal(self, routing_key: str, message: QueueMessage):
        self.info(f"Received enter signal for {routing_key}: {message.payload}")

        symbol = self.trading_config.get_symbol()
        timeframe = self.trading_config.get_timeframe()

        if not self.market_open_event.is_set():
            self.info(f"Market is closed. Ignoring signal for {symbol} {timeframe}")
            return

        async with self.execution_lock:
            cur_candle = message.get("candle")
            prev_candle = message.get("prev_candle")

            symbol = message.get_symbol()
            timeframe = string_to_enum(Timeframe, message.get_timeframe())
            direction = string_to_enum(TradingDirection, message.get_direction())
            candle_open_time = prev_candle.get("prev_candle").get("time_open")
            candle_close_time = prev_candle.get("prev_candle").get("time_close")

            existing_confirmation: Optional[Signal] = next(
                (conf for conf in self.signal_confirmations
                 if
                 conf.symbol == symbol
                 and conf.timeframe == timeframe
                 and conf.direction == direction
                 and conf.candle["time_open"] == candle_open_time
                 and conf.candle["time_close"] == candle_close_time),
                None
            )

            candle_open_time_dt = unix_to_datetime(candle_open_time)
            candle_close_time_dt = unix_to_datetime(candle_close_time)
            candle_open_time_str = candle_open_time_dt.strftime("%H:%M")
            candle_close_time_str = candle_close_time_dt.strftime("%H:%M")

            if existing_confirmation:
                if existing_confirmation.confirmed:
                    self.info(f"Confirmation found for {symbol} - {timeframe} - {direction} - {candle_open_time_str} - {candle_close_time_str}")
                    order = await self.prepare_order_to_place(cur_candle)

                    if order is None:
                        self.error(f"Error while preparing order for signal of {candle_open_time_str} - {candle_close_time_str}")
                        return

                    await self.place_order(order)
                else:
                    self.warning(f"Signal is not confirmed for {symbol} - {timeframe} - {direction} - {candle_open_time_str} - {candle_close_time_str}")
                    await self.send_message_update(f"❌ Signal of {candle_open_time_str} - {candle_close_time_str} has been blocked.")
            else:
                self.warning(f"No confirmation found for {symbol} - {timeframe} - {direction} - {candle_open_time_str} - {candle_close_time_str}")
                await self.send_message_update(f"ℹ️ No choice made for signal of {candle_open_time_str} - {candle_close_time_str}")

    @exception_handler
    async def place_order(self, order: OrderRequest) -> bool:
        self.info(f"[place_order] Placing order: {order}")

        response = await self.broker.place_order(order)

        self.debug(f"[place_order] Result of order placement: {response.success}")

        self.logger.message = f"{response.server_response_code} - {response.server_response_message}"

        order_details = (
            f"<b>Type:</b> {order.order_type.name}\n"
            f"<b>Market:</b> {order.symbol}\n"
            f"<b>Price:</b> {order.order_price}\n"
            f"<b>Volume:</b> {order.volume}\n"
            f"<b>Stop Loss:</b> {order.sl}\n"
            f"<b>Take Profit:</b> {order.tp}\n"
            f"<b>Comment:</b> {order.comment}\n"
            f"<b>Filling Mode:</b> {order.filling_mode.value if order.filling_mode else 'N/A'}\n"
            f"<b>Magic Number:</b> {order.magic_number if order.magic_number else 'N/A'}"
        )

        if response.success:
            self.info(f"[place_order] Order successfully placed. Broker log: \"{response.server_response_message}\"")
            await self.send_message_update(f"✅ <b>Order successfully placed with Deal ID {response.deal}:</b>\n\n{order_details}")
        else:
            self.error("[place_order] Error while placing the order.")
            await self.send_message_update(f"🚫 <b>Error while placing the order:</b>\n\n{order_details}\n<b>Broker message</b>: \"{response.server_response_message}\"")

        return response.success

    def get_take_profit(self, cur_candle: dict, order_price, symbol_point, timeframe, trading_direction):
        atr_periods = 5 if trading_direction == TradingDirection.SHORT else 2
        atr_key = f'ATR_{atr_periods}'
        atr = cur_candle[atr_key]
        multiplier = 1 if timeframe == Timeframe.M30 else 2
        multiplier = multiplier * -1 if trading_direction == TradingDirection.SHORT else multiplier
        take_profit_price = order_price + (multiplier * atr)

        # Return the take profit price rounded to the symbol's point value
        return round_to_point(take_profit_price, symbol_point)

    def get_stop_loss(self, cur_candle: dict, symbol_point, trading_direction):
        # Ensure 'supertrend_slow_key' is defined or passed to this function
        from agents.agent_strategy_adrastea import supertrend_slow_key
        supertrend_slow = cur_candle[supertrend_slow_key]

        # Calculate stop loss adjustment factor
        adjustment_factor = 0.003 / 100

        # Adjust stop loss based on trading direction
        if trading_direction == TradingDirection.LONG:
            sl = supertrend_slow - (supertrend_slow * adjustment_factor)
        elif trading_direction == TradingDirection.SHORT:
            sl = supertrend_slow + (supertrend_slow * adjustment_factor)
        else:
            raise ValueError("Invalid trading direction")

        # Return the stop loss rounded to the symbol's point value
        return round_to_point(sl, symbol_point)

    def get_order_price(self, cur_candle: dict, symbol_point, trading_direction) -> float:
        """
        This function calculates the order price for a trade based on the trading direction and a small adjustment factor.

        Parameters:
        - candle (dict): A dictionary containing the OHLC (Open, High, Low, Close) values for a specific time period.
        - symbol_point (float): The smallest price change for the trading symbol.
        - trading_direction (TradingDirection): An enum value indicating the trading direction (LONG or SHORT).

        Returns:
        - float: The adjusted order price, rounded to the symbol's point value.

        The function first determines the base price based on the trading direction. If the direction is LONG, the base price is the high price of the Heikin Ashi candle; if the direction is SHORT, the base price is the low price of the Heikin Ashi candle.

        Then, it calculates a small adjustment to the base price. The adjustment is a fixed percentage (0.003%) of the base price. The adjustment is added to the base price for LONG trades and subtracted from the base price for SHORT trades.

        Finally, the function returns the adjusted price, rounded to the symbol's point value.
        """
        # Determine the base price based on trading direction.
        base_price_key = 'HA_high' if trading_direction == TradingDirection.LONG else 'HA_low'
        base_price = cur_candle[base_price_key]

        # Calculate the price adjustment.
        adjustment_factor = 0.003 / 100
        adjustment = adjustment_factor * base_price
        adjusted_price = base_price + adjustment if trading_direction == TradingDirection.LONG else base_price - adjustment

        # Return the price rounded to the symbol's point value.
        return round_to_point(adjusted_price, symbol_point)

    def get_volume_NEW(self, account_balance, symbol_info, entry_price, stop_loss_price):
        risk_percent = self.trading_config.get_risk_percent()
        self.info(
            f"Calculating volume for account balance {account_balance}, symbol info {symbol_info}, entry price {entry_price}, stop loss price {stop_loss_price}, and risk percent {risk_percent}")
        risk_amount = account_balance * risk_percent
        stop_loss_pips = abs(entry_price - stop_loss_price) / symbol_info.point
        pip_value = symbol_info.trade_contract_size * symbol_info.point
        volume = risk_amount / (stop_loss_pips * pip_value)
        # Adjust volume to meet broker's constraints
        adjusted_volume = max(
            symbol_info.volume_min,
            min(symbol_info.volume_max, round_to_step(volume, symbol_info.volume_step))
        )
        return adjusted_volume

    def get_volume(self, account_balance, symbol_info: SymbolInfo, leverage, entry_price):
        """
        Calculate the lot size based on a fixed percentage of the account balance, adjusted for leverage,
        and ensuring compliance with the broker's lot size constraints.
        """
        # Calculate the capital to be invested in the trade
        capital_to_invest = account_balance * 0.20 * leverage

        # Calculate the lot size directly (volume in lotti)
        lot_size = capital_to_invest / (entry_price * symbol_info.trade_contract_size)

        # Adjust the lot size to meet the broker's minimum and maximum requirements
        adjusted_lot_size = max(symbol_info.volume_min, min(symbol_info.volume_max, round_to_step(lot_size, symbol_info.volume_step)))

        # Log warnings if necessary
        if lot_size < symbol_info.volume_min:
            self.warning(f"Adjusted lot size to {adjusted_lot_size} to meet minimum requirement of {symbol_info.volume_min} for {symbol_info.symbol}.")
        if lot_size > symbol_info.volume_max:
            self.warning(f"Adjusted lot size to {adjusted_lot_size} to meet maximum requirement of {symbol_info.volume_max} for {symbol_info.symbol}.")

        return adjusted_lot_size


    @exception_handler
    async def prepare_order_to_place(self, cur_candle: dict) -> Optional[OrderRequest]:
        symbol = self.trading_config.get_symbol()
        trading_direction = self.trading_config.get_trading_direction()
        order_type_enter = OpType.BUY if trading_direction == TradingDirection.LONG else OpType.SELL
        timeframe = self.trading_config.get_timeframe()
        magic_number = self.config.get_bot_magic_number()

        symbol_info = await self.broker.get_market_info(symbol)

        if symbol_info is None:
            self.error("[place_order] Symbol info not found.")
            await self.send_message_update("🚫 Symbol info not found for placing the order.")
            raise Exception(f"Symbol info {symbol} not found.")

        point = symbol_info.point
        volume_min = symbol_info.volume_min

        price = self.get_order_price(cur_candle, point, trading_direction)
        sl = self.get_stop_loss(cur_candle, point, trading_direction)
        tp = self.get_take_profit(cur_candle, price, point, timeframe, trading_direction)

        account_balance = await self.broker.get_account_balance()
        leverage = await self.broker.get_account_leverage()

        # volume = self.get_volume(account_balance=account_balance, symbol_info=symbol_info, entry_price=price, stop_loss_price=sl)
        volume = self.get_volume(account_balance=account_balance, symbol_info=symbol_info, leverage=leverage, entry_price=price)

        self.info(f"[place_order] Account balance retrieved: {account_balance}, Calculated volume for the order on {symbol} at price {price}: {volume}")

        if volume < volume_min:
            self.warning(f"[place_order] Volume of {volume} is less than minimum of {volume_min}")
            await self.send_message_update(f"❗ Volume of {volume} is less than the minimum of {volume_min} for {symbol}.")
            return None

        filling_mode = await self.broker.get_filling_mode(symbol)
        self.debug(f"Filling mode for {symbol}: {filling_mode}")

        return OrderRequest(order_type=order_type_enter,
                            symbol=symbol,
                            order_price=price,
                            volume=volume,
                            sl=sl,
                            tp=tp,
                            comment="bot-enter-signal",
                            filling_mode=filling_mode,
                            magic_number=magic_number)

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):
        self.info(f"Publishing event message: {payload}")

        recipient = recipient if recipient is not None else "middleware"

        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction", "bot_name"])
        await RabbitMQService.publish_message(exchange_name=exchange_name,
                                              message=QueueMessage(sender=self.agent, payload=payload, recipient=recipient, trading_configuration=tc),
                                              routing_key=routing_key,
                                              exchange_type=exchange_type)

    @exception_handler
    async def send_message_update(self, message: str):
        bot_token = self.trading_config.get_telegram_config().token
        self.info(f"Publishing event message {message} for queue {bot_token}")
        await self.send_queue_message(exchange=RabbitExchange.NOTIFICATIONS, payload={"message": message}, routing_key=self.id)

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        async with self.execution_lock:
            symbol = self.trading_config.get_symbol()
            time_ref = opening_time if is_open else closing_time
            self.info(f"Market for {symbol} has {'opened' if is_open else 'closed'} at {unix_to_datetime(time_ref)}.")
            if is_open:
                self.market_open_event.set()
            else:
                self.market_open_event.clear()
