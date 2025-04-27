import asyncio
from datetime import datetime
from typing import Optional, List, Any, Callable

# Local application/library specific imports
from agents.agent_registration_aware import RegistrationAwareAgent
from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.QueueMessage import QueueMessage
from dto.RequestResult import RequestResult
from dto.Signal import Signal, SignalStatus
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Timeframe, TradingDirection, OpType, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import round_to_point, round_to_step, unix_to_datetime
from notifiers.notifier_closed_deals import ClosedDealsNotifier
from services.service_amqp import AMQPService
from services.service_signal_persistence import SignalPersistenceService


class ExecutorAgent(RegistrationAwareAgent):
    """
    The ExecutorAgent is responsible for executing trading actions based on signals.

    It listens for signal confirmations and entry signals, manages order placement
    based on received signals, handles emergency close commands, and responds to
    requests for listing open positions. It interacts with the broker for trading
    operations and with the persistence service to manage signal states.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initializes the ExecutorAgent instance.

        Args:
            config: The application's configuration object.
            trading_config: The specific trading configuration for this agent instance
                            (symbol, timeframe, direction, etc.).
        """
        super().__init__(config=config, trading_config=trading_config)
        # Event to signal when the market for the configured symbol is open.
        self.market_open_event = asyncio.Event()
        # Service for interacting with the signal persistence layer (e.g., MongoDB).
        self.persistence_manager: Optional[SignalPersistenceService] = None
        # AMQP service instance, initialized during start.
        self.amqp_s: Optional[AMQPService] = None
        # Define agent name for logging and identification
        self.agent = f"ExecutorAgent_{self.id}"

    @exception_handler
    async def start(self):
        """
        Starts the ExecutorAgent's main routine.

        Initializes the persistence manager, obtains the AMQP service instance,
        and registers listeners for various AMQP messages relevant to its operation:
        - Signal confirmations
        - Signal entry commands
        - Emergency close commands
        - List open positions commands
        Finally, signals its readiness to the middleware.
        """
        self.info(f"Starting event handler for topic {self.topic}.")

        # Initialize the persistence manager
        self.persistence_manager = await SignalPersistenceService.get_instance(self.config)
        # Ensure the persistence service is started (connects to DB, etc.)
        await self.persistence_manager.start()

        # Get the AMQP service instance
        self.amqp_s = await AMQPService.get_instance()

        # Define prefix for queue names based on bot mode
        mode_prefix = self.config.get_bot_mode().name[:3]

        async def register_listener_with_log(
                subscription_name: str,
                exchange: RabbitExchange,
                callback: Callable[[str, QueueMessage], Any],
                routing_key: str,
                queue_name: str
        ):
            """Helper function to register AMQP listeners with logging."""
            self.info(f"Registering [{subscription_name}] listener on topic '{self.topic}' "
                      f"with routing key '{routing_key}' and queue '{queue_name}'.")
            await self.amqp_s.register_listener(
                exchange_name=exchange.name,
                callback=callback,
                routing_key=routing_key,
                exchange_type=exchange.exchange_type,
                queue_name=queue_name
            )

        # --- Register AMQP Listeners ---

        # Listener for Signal Confirmations (from Middleware)
        routing_key_confirmation = "event.signal.confirmation"
        full_routing_key_confirmation = f"{routing_key_confirmation}.{self.topic}"
        queue_name_confirmation = f"{routing_key_confirmation}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Signal Confirmations",
            exchange=RabbitExchange.jupiter_events,
            callback=self.on_signal_confirmation,
            routing_key=full_routing_key_confirmation,
            queue_name=queue_name_confirmation
        )

        # Listener for Market Entry Signals (from Generator)
        routing_key_enter = "event.signal.enter"
        full_routing_key_enter = f"{routing_key_enter}.{self.topic}"
        queue_name_enter = f"{routing_key_enter}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Market Entry Signals",
            exchange=RabbitExchange.jupiter_events,
            callback=self.on_enter_signal,
            routing_key=full_routing_key_enter,
            queue_name=queue_name_enter
        )

        # Listener for Emergency Close Commands (from Middleware)
        routing_key_emergency_close = "command.emergency_close"
        full_routing_key_emergency = f"{routing_key_emergency_close}.{self.topic}"
        queue_name_emergency = f"{routing_key_emergency_close}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Emergency Close Commands",
            exchange=RabbitExchange.jupiter_commands,
            callback=self.on_emergency_close,
            routing_key=full_routing_key_emergency,
            queue_name=queue_name_emergency
        )

        # Listener for List Open Positions Commands (from Middleware)
        routing_key_list_positions = "command.list_open_positions"
        full_routing_key_list_positions = f"{routing_key_list_positions}.{self.id}" # Routed by routine_id
        queue_name_list_positions = f"{routing_key_list_positions}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
        await register_listener_with_log(
            subscription_name="Open Positions Request Commands",
            exchange=RabbitExchange.jupiter_commands,
            callback=self.on_list_open_positions,
            routing_key=full_routing_key_list_positions,
            queue_name=queue_name_list_positions
        )

        self.info(f"All listeners registered on {self.topic}.")
        # Notify middleware that this agent is ready
        await self.agent_is_ready()

    @exception_handler
    async def stop(self):
        """
        Stops the ExecutorAgent.

        Unregisters from the ClosedDealsNotifier and stops the persistence manager.
        """
        self.info(f"Stopping event handler for topic {self.topic}.")
        # Unregister from closed deals notifications
        try:
            c_deals_notif = await ClosedDealsNotifier.get_instance(self.config)
            await c_deals_notif.unregister_observer(
                symbol=self.trading_config.get_symbol(),
                magic_number=self.trading_config.get_magic_number(),
                observer_id=self.id # Use unique agent ID
            )
            self.info("Unregistered from ClosedDealsNotifier.")
        except Exception as e:
            self.error("Error unregistering from ClosedDealsNotifier.", exc_info=e)

        # Stop persistence manager if it was initialized
        if self.persistence_manager:
            try:
                await self.persistence_manager.stop()
                self.info("Persistence manager stopped.")
            except Exception as e:
                self.error("Error stopping persistence manager.", exc_info=e)

        self.info(f"ExecutorAgent {self.id} stopped.")

    @exception_handler
    async def on_signal_confirmation(self, routing_key: str, message: QueueMessage):
        """
        Handles signal confirmation messages received from the middleware.

        Updates the status of the signal in the persistence layer and sends a
        notification back to the user about the confirmation status.

        Args:
            routing_key: The AMQP routing key of the message.
            message: The QueueMessage containing the signal confirmation details.
        """
        self.debug(f"Received raw signal confirmation message. RK: '{routing_key}', Payload: {message.payload}")

        signal_id = message.payload.get("signal_id")
        if not signal_id:
            self.error(f"Received signal confirmation message without 'signal_id'. Payload: {message.payload}")
            return

        self.info(f"Processing confirmation for signal_id: {signal_id}")

        # Retrieve the signal from persistence
        signal: Optional[Signal] = await self.persistence_manager.get_signal(signal_id)
        if not signal:
            self.error(f"Signal {signal_id} not found in persistence for confirmation.")
            # Notify user that the signal was not found?
            # await self.send_message_update(f"⚠️ Could not process confirmation: Signal {signal_id} not found.")
            return

        self.debug(f"Signal {signal_id} retrieved: Status={signal.status.name}, Confirmed={signal.confirmed}")

        # Extract candle times for notification message
        # Ensure opportunity_candle exists and is a dictionary
        opportunity_candle = getattr(signal, 'opportunity_candle', None)
        if not opportunity_candle or not isinstance(opportunity_candle, dict):
            self.error(f"Signal {signal_id} is missing valid 'opportunity_candle' data.")
            # Use signal_id in the notification if candle times are unavailable
            candle_open_time_str = f"Signal {signal_id}"
            candle_close_time_str = "N/A"
        else:
            candle_open_time_unix = opportunity_candle.get("time_open")
            candle_close_time_unix = opportunity_candle.get("time_close")
            candle_open_time_str = unix_to_datetime(candle_open_time_unix).strftime('%Y-%m-%d %H:%M UTC') if candle_open_time_unix else "N/A"
            candle_close_time_str = unix_to_datetime(candle_close_time_unix).strftime('%H:%M UTC') if candle_close_time_unix else "N/A"


        # Determine confirmation status and user
        confirmation_status = signal.confirmed
        user = signal.user if signal.user else 'Unknown User'

        # Prepare notification message based on confirmation status
        if confirmation_status is True:
            notification_message = (
                f"✅ Signal <b>{signal_id}</b> for {signal.symbol} ({signal.timeframe.name} {signal.direction.name})\n"
                f"Candle: {candle_open_time_str} - {candle_close_time_str}\n"
                f"<b>Confirmed</b> by {user}. Preparing for potential entry."
            )
            self.info(f"Signal {signal_id} confirmed by user {user}. Sending notification.")
        elif confirmation_status is False:
            notification_message = (
                f"🚫 Signal <b>{signal_id}</b> for {signal.symbol} ({signal.timeframe.name} {signal.direction.name})\n"
                f"Candle: {candle_open_time_str} - {candle_close_time_str}\n"
                f"<b>Blocked</b> by {user}. Entry will be ignored."
            )
            self.warning(f"Signal {signal_id} blocked by user {user}. Sending notification.")
        else: # confirmation_status is None or unexpected
             notification_message = (
                f"❓ Signal <b>{signal_id}</b> for {signal.symbol} ({signal.timeframe.name} {signal.direction.name})\n"
                f"Candle: {candle_open_time_str} - {candle_close_time_str}\n"
                f"Confirmation status is unclear ({confirmation_status}). Check middleware/user action."
            )
             self.warning(f"Signal {signal_id} has unclear confirmation status ({confirmation_status}). Sending notification.")

        # Send notification to the user via the middleware
        await self.send_message_update(notification_message)
        self.debug(f"Finished processing confirmation for signal_id: {signal_id}")

    @exception_handler
    async def on_list_open_positions(self, routing_key: str, message: QueueMessage):
        """
        Handles requests to list open positions for this agent's configuration.

        Retrieves open positions from the broker matching the agent's symbol and
        magic number, then sends formatted details for each position back to the user.

        Args:
            routing_key: The AMQP routing key (contains the routine_id).
            message: The QueueMessage containing the request.
        """
        symbol = self.trading_config.get_symbol()
        magic_number = self.trading_config.get_magic_number()
        timeframe = self.trading_config.get_timeframe() # For logging/notification context
        direction = self.trading_config.get_trading_direction() # For logging/notification context

        self.info(f"Received request to list open positions for {symbol} "
                  f"({timeframe.name}, {direction.name}, Magic: {magic_number}). RK: {routing_key}")

        positions: List[Position] = []
        error_msg = None

        try:
            # Fetch open positions from the broker
            positions = await self.broker().get_open_positions(symbol=symbol, magic_number=magic_number)
            self.info(f"Broker returned {len(positions)} position(s) for {symbol}/{magic_number}.")
        except Exception as e:
            error_msg = f"❌ Error fetching positions for {symbol}/{magic_number}: {e}"
            self.error(error_msg, exc_info=e)

        # Send error message if fetching failed
        if error_msg:
            await self.send_message_update(error_msg)
            self.info("Sent error message for open positions request.")
            return

        # Send message if no positions are found
        if not positions:
            no_pos_msg = f"ℹ️ No open positions found for {symbol} ({timeframe.name}, {direction.name}, Magic: {magic_number})."
            self.info(no_pos_msg)
            await self.send_message_update(no_pos_msg)
            return

        # Format and send details for each open position
        self.info(f"Sending messages for {len(positions)} open position(s).")
        for pos in positions:
            try:
                # Safely access position attributes with defaults
                pos_ticket = getattr(pos, 'ticket', 'N/A')
                pos_symbol = getattr(pos, 'symbol', 'N/A')
                pos_volume = getattr(pos, 'volume', 0.0)
                pos_open_price = getattr(pos, 'price_open', 0.0)
                pos_current_price = getattr(pos, 'price_current', 0.0)
                pos_time_dt = getattr(pos, 'time', None)
                # Ensure pos_time_dt is a datetime object before formatting
                pos_time_str = pos_time_dt.strftime('%d/%m/%Y %H:%M:%S UTC') if isinstance(pos_time_dt, datetime) else 'N/A'
                pos_profit = getattr(pos, 'profit', 0.0)
                pos_sl = getattr(pos, 'sl', 0.0)
                pos_tp = getattr(pos, 'tp', 0.0)
                pos_swap = getattr(pos, 'swap', 0.0)
                pos_comment = getattr(pos, 'comment', '')
                pos_magic = magic_number # Use the agent's magic number

                profit_emoji = "💰" if pos_profit >= 0 else "🔻"

                # Helper for formatting numbers
                def format_number(value, fmt: str = ".5f", default: str = "N/A"):
                    # Format only if value is not None
                    return f"{value:{fmt}}" if value is not None else default

                # Build the details string
                detail = (
                    f"🆔 ┌─ <b>Ticket:</b> {pos_ticket}\n"
                    f"✨ ├─ <b>Magic:</b> {pos_magic}\n"
                    f"💱 ├─ <b>Market:</b> {pos_symbol}\n"
                    f"📊 ├─ <b>Volume:</b> {format_number(pos_volume, '.2f')}\n" # Format volume with 2 decimals
                    f"📈 ├─ <b>Open Price:</b> {format_number(pos_open_price)}\n"
                    f"📉 ├─ <b>Current Price:</b> {format_number(pos_current_price)}\n"
                    f"⏱️ ├─ <b>Open Time:</b> {pos_time_str}\n"
                    f"{profit_emoji} ├─ <b>Profit:</b> {format_number(pos_profit, '.2f')}\n" # Format profit with 2 decimals
                    f"🛑 ├─ <b>Stop Loss:</b> {format_number(pos_sl)}\n"
                    f"🎯 ├─ <b>Take Profit:</b> {format_number(pos_tp)}\n"
                    f"💬 ├─ <b>Comment:</b> {pos_comment}\n"
                    f"🔁 ├─ <b>Swap:</b> {format_number(pos_swap, '.2f')}\n" # Format swap with 2 decimals
                    f"📊 ├─ <b>Timeframe:</b> {timeframe.name}\n" # Add config TF
                    f"{'📈' if direction == TradingDirection.LONG else '📉'} └─ <b>Direction:</b> {direction.name}\n" # Add config Direction
                )
                # Send the formatted details
                await self.send_message_update(detail)
                self.debug(f"Sent update for position with ticket {pos_ticket}.")
                await asyncio.sleep(0.2) # Small delay between messages
            except Exception as format_e:
                # Log and send error if formatting fails for a position
                error_detail = f"⚠️ Error formatting details for Ticket {getattr(pos, 'ticket', 'N/A')}: {format_e}"
                self.error(error_detail, exc_info=format_e)
                await self.send_message_update(error_detail)
                await asyncio.sleep(0.1)

        self.info(f"Completed sending messages for {len(positions)} positions.")

    @exception_handler
    async def on_emergency_close(self, routing_key: str, message: QueueMessage):
        """
        Handles emergency close commands.

        Closes all open positions matching the agent's symbol and magic number.
        Sends notifications about the success or failure of the operation.

        Args:
            routing_key: The AMQP routing key.
            message: The QueueMessage containing the command.
        """
        symbol = self.trading_config.get_symbol()
        magic_number = self.trading_config.get_magic_number()
        timeframe = self.trading_config.get_timeframe() # For logging/notification
        direction = self.trading_config.get_trading_direction() # For logging/notification

        self.info(f"Processing emergency close command for {symbol} "
                  f"({timeframe.name}, {direction.name}, Magic: {magic_number}). RK: {routing_key}")

        # Check if the market is open before attempting closure
        if not self.market_open_event.is_set():
            self.warning(f"Market closed for {symbol}. Ignoring emergency close command.")
            await self.send_message_update(f"❗ Market is closed. Cannot execute emergency close for {symbol}/{timeframe.name}.")
            return

        successful_closes = 0
        failed_closes = 0
        positions_to_close: List[Position] = []

        try:
            # Fetch open positions to close
            positions_to_close = await self.broker().get_open_positions(symbol=symbol, magic_number=magic_number)

            if not positions_to_close:
                msg = f"ℹ️ No open positions found to emergency close for {symbol} (Magic: {magic_number})."
                self.info(msg)
                await self.send_message_update(msg)
                return

            self.info(f"Attempting to emergency close {len(positions_to_close)} positions for {symbol}/{magic_number}.")

            # Attempt to close each position
            for position in positions_to_close:
                try:
                    self.info(f"Closing position {position.position_id} ({position.volume} lots) for {symbol}.")
                    result: Optional[RequestResult] = await self.broker().close_position(
                        position=position,
                        comment="Emergency close command",
                        magic_number=magic_number # Pass magic number for closure
                    )
                    if result and result.success:
                        successful_closes += 1
                        self.info(f"Successfully closed position {position.position_id} for {symbol}.")
                    else:
                        failed_closes += 1
                        error_msg = result.server_response_message if result else "No response from broker"
                        self.error(f"Failed to close position {position.position_id}. Broker response: {error_msg}", exc_info=False)
                except Exception as close_e:
                    failed_closes += 1
                    self.error(f"Exception closing position {position.position_id}.", exc_info=close_e)

        except Exception as fetch_e:
            # Error fetching positions
            error_message = f"❌ Critical error fetching positions for emergency close ({symbol}/{magic_number}): {fetch_e}"
            self.error(error_message, exc_info=fetch_e)
            await self.send_message_update(error_message)
            return # Stop processing if we can't even get the positions

        # Send final status update
        if successful_closes > 0 and failed_closes == 0:
            result_message = f"✅ Emergency close complete: All {successful_closes} positions for {symbol} (Magic: {magic_number}) closed."
        elif successful_closes > 0:
            result_message = f"⚠️ Partial emergency close: {successful_closes} closed, {failed_closes} failed for {symbol} (Magic: {magic_number})."
        else: # successful_closes == 0
            result_message = f"❌ Emergency close failed: Could not close any of the {failed_closes} open positions for {symbol} (Magic: {magic_number})."

        self.info(result_message)
        await self.send_message_update(result_message)

    @exception_handler
    async def on_enter_signal(self, routing_key: str, message: QueueMessage):
        """
        Handles market entry signals received from a generator.

        Checks market status, validates signal confirmation, prepares, and places
        the order if conditions are met. Sends notifications about the outcome.

        Args:
            routing_key: The AMQP routing key.
            message: The QueueMessage containing the entry signal details (signal_id).
        """
        signal_id_log = message.payload.get('signal_id', 'N/A') # For logging even if processing fails early
        self.info(f"Received entry signal via RK '{routing_key}'. Signal ID: {signal_id_log}")
        self.debug(f"Full Entry Signal Payload: {message.payload}")

        # 1. Check Market Status
        if not self.market_open_event.is_set():
            market_symbol = self.trading_config.get_symbol()
            self.warning(f"Market closed for {market_symbol}. Ignoring entry signal '{signal_id_log}'.")
            return

        # 2. Acquire Lock to prevent concurrent order placement for this agent instance
        async with self.execution_lock:
            self.debug(f"Execution lock acquired for signal '{signal_id_log}'.")
            try:
                # 3. Extract Signal ID and Fetch Signal from Persistence
                signal_id = message.payload.get("signal_id")
                if not signal_id:
                    self.error(f"Entry signal message missing 'signal_id'. Discarding. Payload: {message.payload}")
                    return # Exit locked section early

                self.info(f"Processing entry for signal_id: {signal_id}")
                signal: Optional[Signal] = await self.persistence_manager.get_signal(signal_id)

                if not signal:
                    self.error(f"Signal {signal_id} for entry not found in persistence.")
                    return # Exit locked section early

                # Log key signal info
                self.debug(f"Signal {signal_id} retrieved: Status={signal.status.name}, Confirmed={signal.confirmed}")

                # 4. Validate Signal State for Entry
                if signal.status != SignalStatus.FIRED:
                    self.warning(f"Signal {signal_id} is not in FIRED state (current: {signal.status.name}). Ignoring entry.")
                    return

                # 5. Check Confirmation Status
                if signal.confirmed is None:
                    self.warning(f"Confirmation decision pending for signal {signal_id}. Ignoring entry.")
                    await self.send_message_update(f"ℹ️ Entry ignored: Confirmation pending for signal {signal_id}.")
                    return
                elif signal.confirmed is False:
                    self.warning(f"Signal {signal_id} was explicitly blocked. Ignoring entry.")
                    await self.send_message_update(f"ℹ️ Entry ignored: Signal {signal_id} was blocked.")
                    # Update signal status to BLOCKED to prevent reprocessing
                    signal.status = SignalStatus.BLOCKED
                    await self.persistence_manager.update_signal_status(signal)
                    return

                # 6. Signal is Confirmed and FIRED - Prepare and Place Order
                self.info(f"Signal {signal_id} is confirmed and FIRED. Preparing order...")

                # 'signal_candle' contains the data of the candle triggering state 5 (entry)
                entry_trigger_candle = signal.signal_candle
                if not entry_trigger_candle or not isinstance(entry_trigger_candle, dict):
                     self.error(f"Signal {signal_id} is missing valid 'signal_candle' data for entry. Aborting.")
                     await self.send_message_update(f"⚠️ Entry failed: Missing trigger candle data for signal {signal_id}.")
                     return

                order: Optional[OrderRequest] = await self.prepare_order_to_place(entry_trigger_candle)

                if order is None:
                    self.error(f"Failed to prepare order for confirmed signal {signal_id}.", exc_info=False)
                    await self.send_message_update(f"⚠️ Entry failed: Could not prepare order for signal {signal_id}.")
                    return

                self.info(f"Placing order for signal {signal_id}: {order}")
                order_placed = await self.place_order(order)

                # 7. Update Signal Status after Order Placement Attempt
                if order_placed:
                    signal.status = SignalStatus.CONSUMED_ENTRY
                    self.info(f"Order placed successfully for signal {signal_id}. Updating status to CONSUMED_ENTRY.")
                else:
                    # If order fails, keep status as FIRED? Or add FAILED?
                    # Keeping as FIRED might lead to retries if the condition persists.
                    # Let's keep it FIRED for now. Consider FAILED state later.
                    self.error(f"Order placement failed for signal {signal_id}. Status remains {signal.status.name}.")

                # Persist the final status update
                await self.persistence_manager.update_signal_status(signal)

            except Exception as e_inner:
                self.error(f"Unhandled exception during locked execution for signal '{signal_id_log}': {e_inner}", exc_info=True)
            finally:
                self.debug(f"Releasing execution lock for signal '{signal_id_log}'.")

        self.debug(f"Handler 'on_enter_signal' finished for signal '{signal_id_log}'.")

    @exception_handler
    async def place_order(self, order: OrderRequest) -> bool:
        """
        Places the prepared order using the broker service.

        Sends notifications about the success or failure of the order placement.

        Args:
            order: The OrderRequest object containing order details.

        Returns:
            True if the order was placed successfully, False otherwise.
        """
        self.info(f"Placing order: {order}")
        response: Optional[RequestResult] = None
        try:
            # Place the order via the broker proxy
            response = await self.broker().place_order(order)
            self.debug(f"Order placement attempt result: Success={response.success if response else 'N/A'}")

            # Format order details for notification
            order_details = (
                f"🏷️ ├─ <b>Type:</b> {order.order_type.name}\n"
                f"🏛️ ├─ <b>Market:</b> {order.symbol}\n"
                f"💲 ├─ <b>Price:</b> {order.order_price:.5f}\n"
                f"📊 ├─ <b>Volume:</b> {order.volume:.2f}\n"
                f"🛑 ├─ <b>Stop Loss:</b> {order.sl:.5f}\n"
                f"💹 ├─ <b>Take Profit:</b> {order.tp:.5f}\n"
                f"💬 ├─ <b>Comment:</b> {order.comment if order.comment else '-'}\n"
                f"⚙️ ├─ <b>Filling Mode:</b> {order.filling_mode.name if order.filling_mode else '-'}\n"
                f"✨ └─ <b>Magic Number:</b> {order.magic_number if order.magic_number else '-'}"
            )

            if response and response.success:
                # Extract order ticket/ID from the response
                order_ticket = response.order if response.order else 'N/A'
                self.info(f"Order placed successfully. Ticket: {order_ticket}. Broker log: \"{response.server_response_message}\"")
                # Send success notification
                await self.send_message_update(f"✅ <b>Order placed successfully with Ticket {order_ticket}:</b>\n\n{order_details}")
                return True
            else:
                # Log failure details
                error_code = response.server_response_code if response else 'N/A'
                error_msg = response.server_response_message if response else "No response from broker"
                self.error(f"Order placement failed. Code: {error_code}, Message: \"{error_msg}\"")
                # Send failure notification
                await self.send_message_update(
                    f"🚫 <b>Order placement failed:</b>\n\n{order_details}\n"
                    f"<b>Broker Code:</b> {error_code}\n"
                    f"<b>Broker Message:</b> \"{error_msg}\""
                )
                return False

        except Exception as e:
            # Catch exceptions during the broker call itself
            self.error("Exception during broker.place_order call.", exc_info=e)
            await self.send_message_update(f"💥 <b>Critical error placing order:</b> {str(e)}")
            return False

    def get_take_profit(self, cur_candle: dict, order_price: float, symbol_point: float,
                        timeframe: Timeframe, trading_direction: TradingDirection) -> float:
        """
        Calculates the Take Profit (TP) price based on ATR and trading direction.

        Args:
            cur_candle: Dictionary containing the current candle data including ATR values.
            order_price: The entry price of the order.
            symbol_point: The point value (minimum price fluctuation) for the symbol.
            timeframe: The timeframe of the chart.
            trading_direction: The direction of the trade (LONG or SHORT).

        Returns:
            The calculated Take Profit price, rounded to the symbol's precision.

        Raises:
            ValueError: If ATR key is missing in cur_candle or direction is invalid.
        """
        # Determine ATR period based on direction (Long uses ATR(5), Short uses ATR(2))
        atr_periods = 5 if trading_direction == TradingDirection.LONG else 2
        atr_key = f'ATR_{atr_periods}'

        if atr_key not in cur_candle:
            # Log the missing key for easier debugging
            self.error(f"Required ATR key '{atr_key}' not found in candle data: {list(cur_candle.keys())}")
            raise ValueError(f"Required ATR key '{atr_key}' not found in candle data.")

        atr = cur_candle[atr_key]
        # Determine multiplier based on timeframe (M30 uses 1x ATR, others use 2x ATR)
        multiplier = 1 if timeframe == Timeframe.M30 else 2
        # Adjust multiplier based on trade direction (negative for SHORT)
        multiplier = multiplier * -1 if trading_direction == TradingDirection.SHORT else multiplier

        # Calculate TP price
        take_profit_price = order_price + (multiplier * atr)

        # Round to the symbol's point precision
        return round_to_point(take_profit_price, symbol_point)


    def get_stop_loss(self, cur_candle: dict, symbol_point: float, trading_direction: TradingDirection) -> float:
        """
        Calculates the Stop Loss (SL) price based on the slow SuperTrend indicator.

        Args:
            cur_candle: Dictionary containing the current candle data including SuperTrend values.
            symbol_point: The point value for the symbol.
            trading_direction: The direction of the trade (LONG or SHORT).

        Returns:
            The calculated Stop Loss price, rounded to the symbol's precision.

        Raises:
            ValueError: If the required SuperTrend key is missing or direction is invalid.
        """
        # Define the key locally, ensuring it matches AdrasteaSignalGeneratorAgent
        SUPER_TREND_SLOW_PERIOD = 40
        SUPER_TREND_SLOW_MULTIPLIER = 3.0
        supertrend_slow_key = f"SUPERTREND_{SUPER_TREND_SLOW_PERIOD}_{SUPER_TREND_SLOW_MULTIPLIER}"

        if supertrend_slow_key not in cur_candle:
            self.error(f"Required SuperTrend key '{supertrend_slow_key}' not found in candle data: {list(cur_candle.keys())}")
            raise ValueError(f"Required SuperTrend key '{supertrend_slow_key}' not found in candle data.")

        supertrend_slow = cur_candle[supertrend_slow_key]
        # Adjustment factor (e.g., 0.03% of the SuperTrend value)
        adjustment_factor = 0.0003

        # Calculate SL based on direction
        if trading_direction == TradingDirection.LONG:
            # For LONG, SL is below the SuperTrend line
            sl = supertrend_slow - (supertrend_slow * adjustment_factor)
        elif trading_direction == TradingDirection.SHORT:
            # For SHORT, SL is above the SuperTrend line
            sl = supertrend_slow + (supertrend_slow * adjustment_factor)
        else:
            raise ValueError(f"Invalid trading direction provided: {trading_direction}")

        # Round to the symbol's point precision
        return round_to_point(sl, symbol_point)

    def get_order_price(self, cur_candle: dict, symbol_point: float, trading_direction: TradingDirection) -> float:
        """
        Calculates the entry order price based on Heikin Ashi high/low plus a buffer.

        Uses HA_high for LONG trades and HA_low for SHORT trades.

        Args:
            cur_candle: Dictionary containing Heikin Ashi candle data.
            symbol_point: The point value for the symbol.
            trading_direction: The direction of the trade (LONG or SHORT).

        Returns:
            The calculated order price, rounded to the symbol's precision.

        Raises:
            ValueError: If required HA keys are missing or direction is invalid.
        """
        # Determine the base price key based on direction
        base_price_key = 'HA_high' if trading_direction == TradingDirection.LONG else 'HA_low'

        if base_price_key not in cur_candle:
            self.error(f"Required Heikin Ashi key '{base_price_key}' not found in candle data: {list(cur_candle.keys())}")
            raise ValueError(f"Required Heikin Ashi key '{base_price_key}' not found in candle data.")

        base_price = cur_candle[base_price_key]
        # Small adjustment factor (e.g., 0.03% of the price)
        adjustment_factor = 0.0003
        adjustment = adjustment_factor * base_price

        # Adjust price based on direction (add buffer for LONG entry, subtract for SHORT entry)
        if trading_direction == TradingDirection.LONG:
            adjusted_price = base_price + adjustment
        elif trading_direction == TradingDirection.SHORT:
            adjusted_price = base_price - adjustment
        else:
            raise ValueError(f"Invalid trading direction provided: {trading_direction}")

        # Round to the symbol's point precision
        return round_to_point(adjusted_price, symbol_point)

    @exception_handler
    async def get_exchange_rate(self, base: str, counter: str) -> Optional[float]:
        """
        Retrieves the exchange rate for a currency pair using the broker.

        Args:
            base: Base currency code (e.g., "USD").
            counter: Counter currency code (e.g., "EUR").

        Returns:
            The ask price (exchange rate) if the pair is found, otherwise None.
        """
        if base.upper() == counter.upper():
            self.debug(f"Base and counter currency are the same ({base}). Rate is 1.0.")
            return 1.0

        symbol = f"{base.upper()}{counter.upper()}"
        self.debug(f"Fetching exchange rate for symbol: {symbol}")

        try:
            # Use the broker instance to get market info and price
            symbol_info: Optional[SymbolInfo] = await self.broker().get_market_info(symbol)
            if symbol_info is None:
                self.warning(f"Symbol {symbol} not found by broker for exchange rate.")
                # Try inverse pair
                inverse_symbol = f"{counter.upper()}{base.upper()}"
                self.debug(f"Trying inverse symbol: {inverse_symbol}")
                inverse_symbol_info: Optional[SymbolInfo] = await self.broker().get_market_info(inverse_symbol)
                if inverse_symbol_info is None:
                    self.error(f"Neither {symbol} nor {inverse_symbol} found for exchange rate.")
                    return None
                # Found inverse, get its price and return the reciprocal
                inverse_price = await self.broker().get_symbol_price(inverse_symbol)
                if inverse_price and inverse_price.bid > 0: # Use bid for inverse ask
                    rate = 1.0 / inverse_price.bid
                    self.info(f"Calculated rate for {symbol} from inverse {inverse_symbol}: {rate}")
                    return rate
                else:
                    self.error(f"Could not get valid price for inverse symbol {inverse_symbol}.")
                    return None

            # Original symbol found, get its price
            price = await self.broker().get_symbol_price(symbol)
            if price and price.ask > 0:
                self.info(f"Exchange rate for {symbol}: {price.ask}")
                return price.ask
            else:
                self.error(f"Could not get valid ask price for symbol {symbol}.")
                return None
        except Exception as e:
            self.error(f"Error getting exchange rate for {symbol}: {e}", exc_info=True)
            return None


    def get_volume(self, account_balance: float, order_price: float, symbol_info: SymbolInfo,
                   leverage: float, account_currency: str, conversion_rate: Optional[float]) -> Optional[float]:
        """
        Calculates the trade volume (lot size) based on investment percentage and leverage.

        Args:
            account_balance: Current account balance.
            order_price: The entry price for the order.
            symbol_info: SymbolInfo object containing contract size, steps, limits.
            leverage: Account leverage.
            account_currency: The currency of the trading account.
            conversion_rate: The conversion rate from account currency to the symbol's quote currency.
                             Must be provided if account currency differs from quote currency.

        Returns:
            The calculated volume rounded to the symbol's volume step, or None if calculation fails.

        Raises:
            ValueError: If essential symbol_info attributes are missing.
        """
        invest_percent = self.trading_config.get_invest_percent()
        self.info(f"Calculating volume: Balance={account_balance} {account_currency}, "
                  f"Invest%={invest_percent:.2%}, Leverage={leverage}, OrderPrice={order_price}, "
                  f"Symbol={symbol_info.symbol}, QuoteCurr={symbol_info.quote}, ConvRate={conversion_rate}")

        # Validate required symbol info
        if symbol_info.trade_contract_size is None or symbol_info.volume_step is None:
            self.error("Cannot calculate volume: Missing trade_contract_size or volume_step in SymbolInfo.")
            return None # Indicate failure

        # Calculate investment amount in account currency
        invest_amount_account_ccy = account_balance * invest_percent

        # Convert investment amount to the symbol's quote currency if needed
        invest_amount_quote_ccy = invest_amount_account_ccy
        if account_currency.upper() != symbol_info.quote.upper():
            if conversion_rate is None or conversion_rate <= 0:
                self.error(f"Cannot calculate volume: Conversion rate from {account_currency} to {symbol_info.quote} is missing or invalid ({conversion_rate}).")
                return None # Cannot proceed without conversion rate
            invest_amount_quote_ccy = invest_amount_account_ccy * conversion_rate
            self.debug(f"Converted investment amount: {invest_amount_account_ccy:.2f} {account_currency} -> {invest_amount_quote_ccy:.2f} {symbol_info.quote}")

        # Calculate notional value (total value of the position)
        notional_value = invest_amount_quote_ccy * leverage

        # Calculate cost per lot in quote currency
        cost_per_lot = order_price * symbol_info.trade_contract_size
        if cost_per_lot <= 0:
            self.error(f"Cannot calculate volume: Cost per lot is zero or negative ({cost_per_lot}). Check order_price and contract_size.")
            return None

        # Calculate raw volume
        volume = notional_value / cost_per_lot
        self.debug(f"Calculated raw volume: {volume:.4f} lots")

        # Round volume to the allowed step size
        volume_rounded = round_to_step(volume, symbol_info.volume_step)
        self.debug(f"Volume rounded to step {symbol_info.volume_step}: {volume_rounded:.4f} lots")

        # Clamp volume within min/max limits
        volume_clamped = max(symbol_info.volume_min or 0.0, min(symbol_info.volume_max or float('inf'), volume_rounded))
        if volume_clamped != volume_rounded:
            self.warning(f"Volume clamped from {volume_rounded:.4f} to {volume_clamped:.4f} due to min/max limits ({symbol_info.volume_min}/{symbol_info.volume_max}).")

        # Final check: ensure volume is at least the minimum allowed
        if volume_clamped < (symbol_info.volume_min or 0.0):
             self.warning(f"Final calculated volume {volume_clamped:.4f} is still below minimum {symbol_info.volume_min}. Returning None.")
             return None

        return volume_clamped

    @exception_handler
    async def prepare_order_to_place(self, cur_candle: dict) -> Optional[OrderRequest]:
        """
        Prepares an OrderRequest object for placing a trade.

        Fetches necessary market and account info, calculates order price, SL, TP,
        and volume based on the strategy and risk settings.

        Args:
            cur_candle: Dictionary containing the data for the candle triggering the entry.

        Returns:
            An OrderRequest object ready for placement, or None if preparation fails
            (e.g., market closed, insufficient funds, calculation error).
        """
        symbol = self.trading_config.get_symbol()
        trading_direction = self.trading_config.get_trading_direction()
        timeframe = self.trading_config.get_timeframe()
        magic_number = self.trading_config.get_magic_number()
        order_type_enter = OpType.BUY_STOP if trading_direction == TradingDirection.LONG else OpType.SELL_STOP

        self.info(f"Preparing order for {symbol} {timeframe.name} {trading_direction.name} (Magic: {magic_number})")

        try:
            # 1. Get Symbol Information
            symbol_info: Optional[SymbolInfo] = await self.broker().get_market_info(symbol)
            if not symbol_info:
                self.error(f"Failed to get symbol info for {symbol}. Cannot prepare order.")
                await self.send_message_update(f"⚠️ Entry failed: Could not get market info for {symbol}.")
                return None
            self.debug(f"Symbol Info for {symbol}: Point={symbol_info.point}, VolMin={symbol_info.volume_min}, VolMax={symbol_info.volume_max}, Step={symbol_info.volume_step}, Quote={symbol_info.quote}")

            point = symbol_info.point

            # 2. Calculate Order Price, SL, TP
            order_price = self.get_order_price(cur_candle, point, trading_direction)
            sl_price = self.get_stop_loss(cur_candle, point, trading_direction)
            tp_price = self.get_take_profit(cur_candle, order_price, point, timeframe, trading_direction)
            self.debug(f"Calculated Prices: Entry={order_price:.5f}, SL={sl_price:.5f}, TP={tp_price:.5f}")

            # 3. Get Account Information
            account_balance = await self.broker().get_account_balance()
            account_currency = await self.broker().get_account_currency()
            leverage = await self.broker().get_account_leverage()
            self.debug(f"Account Info: Balance={account_balance} {account_currency}, Leverage={leverage}")

            # 4. Get Conversion Rate if needed
            conversion_rate = 1.0 # Default if currencies match
            if account_currency.upper() != symbol_info.quote.upper():
                conversion_rate = await self.get_exchange_rate(account_currency, symbol_info.quote) # Corrected call
                if conversion_rate is None:
                    self.error(f"Failed to get conversion rate {account_currency}/{symbol_info.quote}. Cannot calculate volume.")
                    await self.send_message_update(f"⚠️ Entry failed: Could not get conversion rate for {account_currency}/{symbol_info.quote}.")
                    return None
                self.debug(f"Conversion Rate {account_currency}/{symbol_info.quote}: {conversion_rate}")

            # 5. Calculate Volume
            volume = self.get_volume(
                account_balance=account_balance,
                order_price=order_price,
                symbol_info=symbol_info,
                leverage=leverage,
                account_currency=account_currency,
                conversion_rate=conversion_rate
            )
            if volume is None:
                 # Error already logged in get_volume
                await self.send_message_update(f"⚠️ Entry failed: Could not calculate trade volume for {symbol}.")
                return None
            self.info(f"Calculated Volume: {volume:.2f} lots")

            # 6. Validate Volume against Minimum
            # get_volume already clamps and checks against minimum, returning None if invalid
            # Redundant check removed here for clarity.

            # 7. Get Filling Mode
            filling_mode = await self.broker().get_filling_mode(symbol)
            self.debug(f"Determined Filling Mode for {symbol}: {filling_mode.name}")

            # 8. Create OrderRequest Object
            order_request = OrderRequest(
                order_type=order_type_enter,
                symbol=symbol,
                order_price=order_price,
                volume=volume,
                sl=sl_price,
                tp=tp_price,
                comment=f"bot-{self.topic}", # Use agent's topic for comment
                filling_mode=filling_mode,
                magic_number=magic_number
            )
            self.info(f"Order prepared successfully: {order_request}")
            return order_request

        except ValueError as ve: # Catch specific calculation errors
            self.error(f"Value error preparing order for {symbol}: {ve}", exc_info=False)
            await self.send_message_update(f"⚠️ Entry failed: Calculation error - {ve}")
            return None
        except Exception as e:
            self.error(f"Unexpected error preparing order for {symbol}", exc_info=e)
            await self.send_message_update(f"💥 Entry failed: Unexpected error preparing order for {symbol}.")
            return None

    @exception_handler
    async def send_message_update(self, message: str):
        """
        Sends an update message to the user via the middleware.

        Args:
            message: The text content of the update message.
        """
        self.info(f"Sending update message: {message}")
        # Use the agent's unique ID for user-specific routing
        routing_key = f"notification.user.{self.id}"
        # Ensure AMQP service is available
        if self.amqp_s is None:
             self.error("Cannot send message update: AMQP service is not initialized.")
             return
        # Use the base class method to construct and send the message
        await self.send_queue_message(
            exchange=RabbitExchange.jupiter_notifications,
            payload={"message": message},
            routing_key=routing_key
        )

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool,
                                      closing_time: Optional[float], opening_time: Optional[float],
                                      initializing: bool):
        """
        Callback executed when the market status changes for the agent's symbol.

        Updates the internal `market_open_event`.

        Args:
            symbol: The symbol whose status changed (should match agent's config).
            is_open: True if the market is now open, False otherwise.
            closing_time: Unix timestamp (UTC) of market close, if applicable.
            opening_time: Unix timestamp (UTC) of market open, if applicable.
            initializing: True if this is an initial status update during startup.
        """
        # Ensure this callback only reacts to its configured symbol
        if symbol != self.trading_config.get_symbol():
             self.debug(f"Ignoring market status change for unrelated symbol {symbol}.")
             return

        # Protect access to market_open_event - Use the agent's execution lock
        async with self.execution_lock:
            # Format timestamp for logging
            time_ref_dt = unix_to_datetime(opening_time if is_open else closing_time) if (opening_time or closing_time) else None
            time_ref_str = time_ref_dt.strftime('%Y-%m-%d %H:%M:%S UTC') if time_ref_dt else "N/A"
            state_str = "Opened" if is_open else "Closed"
            init_str = "(Initializing)" if initializing else ""

            self.info(f"Market status change for {symbol}: {state_str} at {time_ref_str} {init_str}")

            # Update the event state
            if is_open:
                self.market_open_event.set()
                self.debug(f"Market open event SET for {symbol}.")
            else:
                self.market_open_event.clear()
                self.debug(f"Market open event CLEARED for {symbol}.")

