import asyncio
from datetime import timedelta, datetime
from typing import Optional, List

from agents.agent_registration_aware import RegistrationAwareAgent
from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.QueueMessage import QueueMessage
from dto.RequestResult import RequestResult
from dto.Signal import Signal
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Timeframe, TradingDirection, OpType, RabbitExchange, Action, PositionType
from misc_utils.error_handler import exception_handler
from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import round_to_point, round_to_step, unix_to_datetime, extract_properties, now_utc
from notifiers.notifier_closed_deals import ClosedDealsNotifier
from services.service_rabbitmq import RabbitMQService
from services.service_signal_persistence import SignalPersistenceService


class ExecutorAgent(RegistrationAwareAgent):
    """
    ExecutorAgent handles trading signal processing, order placement, and position reporting
    for a given trading configuration.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initialize the ExecutorAgent with configuration and trading settings.

        Parameters:
            config (ConfigReader): Global configuration.
            trading_config (TradingConfiguration): Specific trading parameters.
        """
        super().__init__(config=config, trading_config=trading_config)
        self.signal_confirmations: List[Signal] = []
        self.market_open_event = asyncio.Event()
        self.persistence_manager = SignalPersistenceService(self.config)
        self.rabbitmq_s = None

    @exception_handler
    async def start(self):
        """
        Start the ExecutorAgent:
         - Initialize persistence and load active signals.
         - Obtain the RabbitMQ service instance.
         - Register message listeners for signal confirmations, entries, emergency closes,
           and position list requests.
        """
        self.info(f"Starting event handler for topic {self.topic}.")
        await self.persistence_manager.start()
        self.rabbitmq_s = await RabbitMQService.get_instance()

        # Load active signals matching symbol, timeframe, and direction
        symbol = self.trading_config.get_symbol()
        timeframe = self.trading_config.get_timeframe()
        direction = self.trading_config.get_trading_direction()
        loaded_signals = await self.persistence_manager.retrieve_active_signals(symbol, timeframe, direction, self.agent)
        self.signal_confirmations = [Signal.from_json(signal) for signal in (loaded_signals or [])]

        mode_prefix = self.config.get_bot_mode().name[:3]

        async def register_listener_with_log(subscription_name: str, exchange, callback, routing_key: str, exchange_type: str, queue_name: str):
            self.info(f"Registering [{subscription_name}] listener on topic '{self.topic}' with routing key '{routing_key}' and queue '{queue_name}'.")
            await self.rabbitmq_s.register_listener(
                exchange_name=exchange.name,
                callback=callback,
                routing_key=routing_key,
                exchange_type=exchange_type,
                queue_name=queue_name
            )

        # Listener for signal confirmations
        routing_key_confirmation = "event.signal.confirmation"
        full_routing_key_confirmation = f"{routing_key_confirmation}.{self.topic}"
        queue_name_confirmation = f"{routing_key_confirmation}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Signal Confirmations",
            exchange=RabbitExchange.jupiter_events,
            callback=self.on_signal_confirmation,
            routing_key=full_routing_key_confirmation,
            exchange_type=RabbitExchange.jupiter_events.exchange_type,
            queue_name=queue_name_confirmation
        )

        # Listener for market entry signals
        routing_key_enter = "event.signal.enter"
        full_routing_key_enter = f"{routing_key_enter}.{self.topic}"
        queue_name_enter = f"{routing_key_enter}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Market Entry Signals",
            exchange=RabbitExchange.jupiter_events,
            callback=self.on_enter_signal,
            routing_key=full_routing_key_enter,
            exchange_type=RabbitExchange.jupiter_events.exchange_type,
            queue_name=queue_name_enter
        )

        # Listener for emergency close commands
        routing_key_emergency_close = "command.emergency_close"
        full_routing_key_emergency = f"{routing_key_emergency_close}.{self.topic}"
        queue_name_emergency = f"{routing_key_emergency_close}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Emergency Close Commands",
            exchange=RabbitExchange.jupiter_commands,
            callback=self.on_emergency_close,
            routing_key=full_routing_key_emergency,
            exchange_type=RabbitExchange.jupiter_commands.exchange_type,
            queue_name=queue_name_emergency
        )

        # Listener for open positions request commands
        routing_key_list_positions = "command.list_open_positions"
        full_routing_key_list_positions = f"{routing_key_list_positions}.{self.id}"
        queue_name_list_positions = f"{routing_key_list_positions}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Open Positions Request Commands",
            exchange=RabbitExchange.jupiter_commands,
            callback=self.on_list_open_positions,
            routing_key=full_routing_key_list_positions,
            exchange_type=RabbitExchange.jupiter_commands.exchange_type,
            queue_name=queue_name_list_positions
        )

        self.info(f"All listeners registered on {self.topic}.")

    @exception_handler
    async def stop(self):
        """
        Stop the ExecutorAgent by unregistering observers and terminating persistence.
        """
        self.info(f"Stopping event handler for topic {self.topic}.")
        c_deals_notif = await ClosedDealsNotifier.get_instance(self.config)
        await c_deals_notif.unregister_observer(
            self.trading_config.get_symbol(),
            self.trading_config.get_magic_number(),
            self.id
        )

    @exception_handler
    async def on_signal_confirmation(self, router_key: str, message: QueueMessage):
        """
        Update internal signal confirmations based on received confirmation data.

        A new confirmation replaces an existing one if it is more recent.
        """
        signal = Signal.from_json(message.get("signal"))
        self.info(f"Received signal confirmation: {signal}")

        candle_open_time = signal.candle.get("time_open")
        candle_close_time = signal.candle.get("time_close")

        existing_confirmation = next(
            (conf for conf in self.signal_confirmations
             if conf.symbol == signal.symbol
             and conf.timeframe == signal.timeframe
             and conf.direction == signal.direction
             and conf.candle['time_open'] == candle_open_time
             and conf.candle['time_close'] == candle_close_time),
            None
        )

        if existing_confirmation:
            if (not existing_confirmation.update_tms and signal.update_tms) or (signal.update_tms > existing_confirmation.update_tms):
                self.info(f"Updating confirmation for {signal.symbol} {signal.timeframe} at {candle_open_time} - {candle_close_time}.")
                self.signal_confirmations.remove(existing_confirmation)
                self.signal_confirmations.append(signal)
            else:
                self.info(f"Ignored outdated confirmation for {signal.symbol} {signal.timeframe}.")
        else:
            self.info(f"Adding new confirmation for {signal.symbol} {signal.timeframe}.")
            self.signal_confirmations.append(signal)

    @exception_handler
    async def on_list_open_positions(self, routing_key: str, message: QueueMessage):
        """
        Retrieve open positions based on the agent's configuration and send individual updates.

        If an error occurs or no positions are found, a corresponding message is sent.
        """
        symbol = self.trading_config.get_symbol()
        magic_number = self.trading_config.get_magic_number()
        timeframe = self.trading_config.get_timeframe()
        direction = self.trading_config.get_trading_direction()

        self.info(f"Generating open positions report for {symbol} ({timeframe.name}, {direction.name}, Magic: {magic_number}).")
        positions: List[Position] = []
        error_msg = None

        try:
            positions = await self.broker().get_open_positions(symbol=symbol, magic_number=magic_number)
            self.info(f"Broker returned {len(positions)} position(s) for {symbol}/{magic_number}.")
        except Exception as e:
            error_msg = f"❌ Error fetching positions for {symbol}/{magic_number}: {e}"
            self.error(error_msg, exec_info=e)

        if error_msg:
            await self.send_message_update(error_msg)
            self.info("Sent error message for open positions request.")
            return

        if not positions:
            self.info(f"No open positions found for {symbol}/{magic_number}.")
            return

        self.info(f"Sending messages for {len(positions)} open position(s).")
        for pos in positions:
            try:
                pos_id = getattr(pos, 'position_id', 'N/A')
                pos_ticket = getattr(pos, 'ticket', pos_id)
                pos_symbol = getattr(pos, 'symbol', 'N/A')
                pos_type = getattr(pos, 'position_type', PositionType.OTHER)
                pos_volume = getattr(pos, 'volume', 0.0)
                pos_open_price = getattr(pos, 'price_open', 0.0)
                pos_current_price = getattr(pos, 'price_current', 0.0)
                pos_time_dt = getattr(pos, 'time', None)
                pos_time_str = pos_time_dt.strftime('%d/%m/%Y %H:%M:%S') if isinstance(pos_time_dt, datetime) else 'N/A'
                pos_profit = getattr(pos, 'profit', 0.0)
                pos_sl = getattr(pos, 'sl', 0.0)
                pos_tp = getattr(pos, 'tp', 0.0)
                pos_swap = getattr(pos, 'swap', 0.0)
                pos_comment = getattr(pos, 'comment', '')
                pos_magic = magic_number
                deals = getattr(pos, 'deals', [])
                if deals and hasattr(deals[0], 'magic_number'):
                    pos_magic = getattr(deals[0], 'magic_number', magic_number)
                profit_emoji = "💰" if pos_profit >= 0 else "🔻"

                def format_number(value, fmt: str = ".2f", default: str = "N/A"):
                    return f"{value:{fmt}}" if value is not None else default

                detail = (
                    f"🆔 ┌─ <b>Ticket:</b> {pos_ticket}\n"
                    f"✨ ├─ <b>Magic:</b> {pos_magic}\n"
                    f"💱 ├─ <b>Market:</b> {pos_symbol}\n"
                    f"📊 ├─ <b>Volume:</b> {format_number(pos_volume)}\n"
                    f"📈 ├─ <b>Open Price:</b> {format_number(pos_open_price)}\n"
                    f"📉 ├─ <b>Current Price:</b> {format_number(pos_current_price)}\n"
                    f"⏱️ ├─ <b>Open Time:</b> {pos_time_str}\n"
                    f"{profit_emoji} ├─ <b>Profit:</b> {format_number(pos_profit)}\n"
                    f"🛑 ├─ <b>Stop Loss:</b> {format_number(pos_sl)}\n"
                    f"🎯 ├─ <b>Take Profit:</b> {format_number(pos_tp)}\n"
                    f"💬 ├─ <b>Comment:</b> {pos_comment}\n"
                    f"🔁 ├─ <b>Swap:</b> {format_number(pos_swap)}\n"
                    f"📊 ├─ <b>Timeframe:</b> {timeframe.name}\n"
                    f"{'📈' if direction.name == 'LONG' else '📉'} └─ <b>Direction:</b> {direction.name}\n"
                )
                await self.send_message_update(detail)
                self.debug(f"Sent update for position with ticket {pos_ticket}.")
                await asyncio.sleep(0.2)
            except Exception as format_e:
                error_detail = f"⚠️ Error formatting details for Ticket {getattr(pos, 'ticket', 'N/A')}: {format_e}"
                self.error(error_detail, exec_info=format_e)
                await self.send_message_update(error_detail)
                await asyncio.sleep(0.1)
        self.info(f"Completed sending messages for {len(positions)} positions.")

    @exception_handler
    async def on_emergency_close(self, routing_key: str, message: QueueMessage):
        """
        Process an emergency close command by closing all open positions for the specified symbol,
        timeframe, and direction.
        """
        try:
            symbol = message.get_meta_inf().get_symbol()
            timeframe = message.get_meta_inf().get_timeframe()
            direction = message.get_meta_inf().get_direction()

            self.info(f"Processing emergency close for {symbol} ({timeframe}, {direction}) via {routing_key}.")

            if not self.market_open_event.is_set():
                self.info(f"Market closed. Ignoring emergency close for {symbol} ({timeframe.name}).")
                await self.send_message_update(f"❗ Market is closed. Ignoring emergency close for {symbol}/{timeframe.name}.")
                return

            magic_number = self.trading_config.get_magic_number()
            positions = await self.broker().with_context(symbol).get_open_positions(symbol=symbol, magic_number=magic_number)

            if not positions:
                msg = f"❗ No open positions for {symbol} ({timeframe.name}, {direction.name}, Magic: {magic_number})."
                self.info(msg)
                return

            successful_closes = 0
            failed_closes = 0

            for position in positions:
                try:
                    self.info(f"Closing position {position.position_id} for {symbol}.")
                    result = await self.broker().close_position(position=position, comment="Emergency close", magic_number=magic_number)
                    if result and result.success:
                        successful_closes += 1
                        self.info(f"Closed position {position.position_id} for {symbol}.")
                    else:
                        failed_closes += 1
                        error_msg = result.server_response_message if result else "No response"
                        self.error(f"Failed to close {position.position_id}. Broker response: {error_msg}", exec_info=False)
                except Exception as e:
                    failed_closes += 1
                    self.error(f"Error closing position {position.position_id}.", exec_info=e)

            if successful_closes > 0 and failed_closes == 0:
                result_message = f"✅ Emergency close complete: All {successful_closes} positions for {symbol} closed."
            elif successful_closes > 0:
                result_message = f"⚠️ Partial emergency close: {successful_closes} closed, {failed_closes} failed for {symbol}."
            else:
                result_message = f"❌ Emergency close failed: {failed_closes} positions for {symbol} could not be closed."
            self.info(result_message)
            await self.send_message_update(result_message)
        except Exception as e:
            error_message = f"❌ Critical error during emergency close for {message.get_meta_inf().get_symbol() if message else 'unknown symbol'}: {str(e)}"
            self.error(error_message, exec_info=e)
            await self.send_message_update(error_message)

    @exception_handler
    async def on_enter_signal(self, routing_key: str, message: QueueMessage):
        """
        Process a market entry signal:
         - Verify if the market is open.
         - Check if the signal has been confirmed.
         - Prepare and place an order if confirmed.
         - Provide feedback if no confirmation is found.
        """
        self.info(f"Received entry signal for {routing_key}: {message.payload}")

        if not self.market_open_event.is_set():
            self.info(f"Market closed. Ignoring entry signal for {self.trading_config.get_symbol()} ({self.trading_config.get_timeframe()}).")
            return

        async with self.execution_lock:
            cur_candle = message.get("candle")
            prev_candle = message.get("prev_candle")

            symbol = message.get_meta_inf().get_symbol()
            timeframe = message.get_meta_inf().get_timeframe()
            direction = message.get_meta_inf().get_direction()
            candle_open_time = prev_candle.get("time_open")
            candle_close_time = prev_candle.get("time_close")

            existing_confirmation: Optional[Signal] = next(
                (conf for conf in self.signal_confirmations
                 if conf.symbol == symbol
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
                if existing_confirmation.confirmed is True:
                    self.info(f"Confirmed signal for {symbol} ({timeframe}, {direction}) at {candle_open_time_str} - {candle_close_time_str}.")
                    order = await self.prepare_order_to_place(cur_candle)
                    if order is None:
                        self.error(f"Error preparing order for signal at {candle_open_time_str} - {candle_close_time_str}.", exec_info=False)
                        return
                    await self.place_order(order)
                else:
                    self.warning(f"Unconfirmed signal for {symbol} ({timeframe}, {direction}) at {candle_open_time_str} - {candle_close_time_str}.")
                    await self.send_message_update(f"❌ Signal at {candle_open_time_str} - {candle_close_time_str} blocked.")
            else:
                self.warning(f"No confirmation for signal for {symbol} ({timeframe}, {direction}) at {candle_open_time_str} - {candle_close_time_str}.")
                await self.send_message_update(f"ℹ️ No confirmation for signal at {candle_open_time_str} - {candle_close_time_str}.")

    @exception_handler
    async def place_order(self, order: OrderRequest) -> bool:
        """
        Place an order using the broker and notify the outcome via messaging.

        Returns:
            bool: True if the order was successfully placed; otherwise, False.
        """
        self.info(f"[place_order] Placing order: {order}")
        response: RequestResult = await self.broker().place_order(order)
        self.debug(f"[place_order] Order result: {response.success}")
        self.logger.message = f"{response.server_response_code} - {response.server_response_message}"

        order_details = (
            f"🏷️ ├─ <b>Type:</b> {order.order_type.name}\n"
            f"🏛️ ├─ <b>Market:</b> {order.symbol}\n"
            f"💲 ├─ <b>Price:</b> {order.order_price:.2f}\n"
            f"📊 ├─ <b>Volume:</b> {order.volume:.2f}\n"
            f"🛑 ├─ <b>Stop Loss:</b> {order.sl:.2f}\n"
            f"💹 ├─ <b>Take Profit:</b> {order.tp:.2f}\n"
            f"💬 ├─ <b>Comment:</b> {order.comment if order.filling_mode else '-'}\n"
            f"⚙️ ├─ <b>Filling Mode:</b> {order.filling_mode.value if order.filling_mode else '-'}\n"
            f"✨ └─ <b>Magic Number:</b> {order.magic_number if order.magic_number else '-'}"
        )

        if response.success:
            self.info(f"[place_order] Order placed successfully. Broker log: \"{response.server_response_message}\"")
            await self.send_message_update(f"✅ <b>Order placed successfully with ID {response.order}:</b>\n\n{order_details}")
        else:
            self.error("[place_order] Order placement failed.")
            await self.send_message_update(
                f"🚫 <b>Order placement error:</b>\n\n{order_details}\n<b>Broker message:</b> \"{response.server_response_message}\""
            )

        return response.success

    def get_take_profit(self, cur_candle: dict, order_price, symbol_point, timeframe, trading_direction):
        """
        Compute the take profit level using the ATR value and a timeframe multiplier.

        Returns:
            float: The calculated take profit level rounded to the allowed price point.
        """
        atr_periods = 5 if trading_direction == TradingDirection.SHORT else 2
        atr_key = f'ATR_{atr_periods}'
        atr = cur_candle[atr_key]
        multiplier = 1 if timeframe == Timeframe.M30 else 2
        multiplier = multiplier * -1 if trading_direction == TradingDirection.SHORT else multiplier
        take_profit_price = order_price + (multiplier * atr)
        return round_to_point(take_profit_price, symbol_point)

    def get_stop_loss(self, cur_candle: dict, symbol_point, trading_direction):
        """
        Determine the stop loss level using a supertrend indicator and an adjustment factor.

        Returns:
            float: The stop loss level rounded to the allowed price point.
        """
        from agents.agent_strategy_adrastea import supertrend_slow_key
        supertrend_slow = cur_candle[supertrend_slow_key]
        adjustment_factor = 0.003 / 100
        if trading_direction == TradingDirection.LONG:
            sl = supertrend_slow - (supertrend_slow * adjustment_factor)
        elif trading_direction == TradingDirection.SHORT:
            sl = supertrend_slow + (supertrend_slow * adjustment_factor)
        else:
            raise ValueError("Invalid trading direction")
        return round_to_point(sl, symbol_point)

    def get_order_price(self, cur_candle: dict, symbol_point, trading_direction) -> float:
        """
        Calculate the order price based on Heikin Ashi candle data and a small adjustment factor.

        Returns:
            float: The adjusted order price rounded to the allowed price point.
        """
        base_price_key = 'HA_high' if trading_direction == TradingDirection.LONG else 'HA_low'
        base_price = cur_candle[base_price_key]
        adjustment_factor = 0.003 / 100
        adjustment = adjustment_factor * base_price
        adjusted_price = base_price + adjustment if trading_direction == TradingDirection.LONG else base_price - adjustment
        return round_to_point(adjusted_price, symbol_point)

    def get_volume_NEW(self, account_balance, symbol_info, entry_price, stop_loss_price):
        """
        Calculate the trade volume (lot size) based on account risk management.

        Returns:
            float: The adjusted volume that meets broker constraints.
        """
        risk_percent = self.trading_config.get_invest_percent()
        self.info(f"Calculating volume with balance {account_balance}, symbol info {symbol_info}, entry {entry_price}, SL {stop_loss_price}, risk {risk_percent}")
        risk_amount = account_balance * risk_percent
        stop_loss_pips = abs(entry_price - stop_loss_price) / symbol_info.point
        pip_value = symbol_info.trade_contract_size * symbol_info.point
        volume = risk_amount / (stop_loss_pips * pip_value)
        adjusted_volume = max(symbol_info.volume_min, min(symbol_info.volume_max, round_to_step(volume, symbol_info.volume_step)))
        return adjusted_volume

    @exception_handler
    async def get_exchange_rate(self, base: str, counter: str) -> float | None:
        """
        Retrieve the exchange rate for a currency pair.

        Parameters:
            base (str): Base currency.
            counter (str): Counter currency.
        Returns:
            float: Exchange rate if available; otherwise, None.
        """
        if base == counter:
            return 1.0

        symbol = f"{base.upper()}{counter.upper()}"
        symbol_info = await self.broker().get_market_info(symbol)
        if symbol_info is None:
            print(f"Symbol {symbol} not found")
            return None

        price = await self.broker().get_symbol_price(symbol)
        return price.ask

    def get_volume(
            self,
            account_balance: float,
            order_price: float,
            symbol_info: SymbolInfo,
            invest_percent: float = 0.20,
            leverage: float = 1.0,
            account_currency: str = "USD",
            conversion_rate: float = 1.0
    ):
        """
        Determine the trade volume (lot size) based on the account balance, investment percentage,
        leverage, and market conditions.

        Returns:
            float: The volume rounded to the allowed trading step.
        """
        invest_amount = account_balance * invest_percent
        if account_currency.upper() != symbol_info.quote.upper():
            invest_amount *= conversion_rate
        notional_value = invest_amount * leverage
        cost_per_lot = order_price * symbol_info.trade_contract_size
        volume = notional_value / cost_per_lot
        volume = round_to_step(volume, symbol_info.volume_step)
        return volume

    @exception_handler
    async def prepare_order_to_place(self, cur_candle: dict) -> Optional[OrderRequest]:
        """
        Prepare an OrderRequest by:
         - Retrieving market info.
         - Computing order price, stop loss, and take profit.
         - Calculating trade volume based on risk management.
         - Validating volume against the minimum allowed.
        Returns:
            OrderRequest if all parameters are valid; otherwise, None.
        """
        symbol = self.trading_config.get_symbol()
        trading_direction = self.trading_config.get_trading_direction()
        order_type_enter = OpType.BUY_STOP if trading_direction == TradingDirection.LONG else OpType.SELL_STOP
        timeframe = self.trading_config.get_timeframe()
        magic_number = self.trading_config.get_magic_number()

        symbol_info = await self.broker().get_market_info(symbol)
        if symbol_info is None:
            self.error("[place_order] Symbol info not found.")
            await self.send_message_update("🚫 Symbol info not found for placing the order.")
            raise Exception(f"Symbol info {symbol} not found.")

        point = symbol_info.point
        volume_min = symbol_info.volume_min

        price = self.get_order_price(cur_candle, point, trading_direction)
        sl = self.get_stop_loss(cur_candle, point, trading_direction)
        tp = self.get_take_profit(cur_candle, price, point, timeframe, trading_direction)

        account_balance = await self.broker().get_account_balance()
        account_currency = await self.broker().get_account_currency()
        leverage = await self.broker().get_account_leverage()
        conversion_rate = await self.get_exchange_rate(account_currency, symbol_info.quote)
        invest_percent = self.trading_config.get_invest_percent()

        volume = self.get_volume(account_balance=account_balance,
                                 order_price=price,
                                 symbol_info=symbol_info,
                                 account_currency=account_currency,
                                 conversion_rate=conversion_rate,
                                 leverage=leverage,
                                 invest_percent=invest_percent)

        self.info(f"[place_order] Balance: {account_balance}, Volume for {symbol} at {price}: {volume}")

        if volume < volume_min:
            self.warning(f"[place_order] Volume {volume} is below minimum {volume_min} for {symbol}.")
            await self.send_message_update(f"❗ Volume of {volume} is less than the minimum of {volume_min} for {symbol}.")
            return None

        filling_mode = await self.broker().get_filling_mode(symbol)
        self.debug(f"Filling mode for {symbol}: {filling_mode}")

        return OrderRequest(
            order_type=order_type_enter,
            symbol=symbol,
            order_price=price,
            volume=volume,
            sl=sl,
            tp=tp,
            comment=f"bot-{self.topic}",
            filling_mode=filling_mode,
            magic_number=magic_number
        )

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):
        """
        Publish a message to a RabbitMQ exchange with standardized metadata.
        """
        self.info(f"Publishing event message: {payload}")
        recipient = recipient if recipient is not None else "middleware"
        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction", "bot_name"])
        meta_inf = MessageMetaInf(
            bot_name=self.config.get_bot_name(),
            instance_name=self.config.get_instance_name(),
            routine_id=self.id,
            agent_name=self.agent,
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            direction=self.trading_config.get_trading_direction()
        )
        await self.rabbitmq_s.publish_message(
            exchange_name=exchange_name,
            message=QueueMessage(sender=self.agent, payload=payload, recipient=recipient, meta_inf=meta_inf),
            routing_key=routing_key,
            exchange_type=exchange_type
        )

    @exception_handler
    async def send_message_update(self, message: str):
        """
        Send an update message via Telegram using the agent's configured token.
        """
        bot_token = self.trading_config.get_telegram_config().token
        self.info(f"Sending update message to bot {bot_token}: {message}")
        await self.send_queue_message(
            exchange=RabbitExchange.jupiter_notifications,
            payload={"message": message},
            routing_key=f"notification.user.{self.id}"
        )

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        """
        Update the market open status based on a change event.

        Sets or clears the market_open_event accordingly.
        """
        async with self.execution_lock:
            # Use the trading configuration symbol for logging consistency
            symbol = self.trading_config.get_symbol()
            time_ref = opening_time if is_open else closing_time
            self.info(f"Market for {symbol} has {'opened' if is_open else 'closed'} at {unix_to_datetime(time_ref)}.")
            if is_open:
                self.market_open_event.set()
            else:
                self.market_open_event.clear()
