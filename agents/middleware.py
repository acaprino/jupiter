import asyncio
import time
from collections import defaultdict
from datetime import timedelta
from typing import List, Tuple

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram import F
from dto.QueueMessage import QueueMessage
from dto.Signal import Signal
from misc_utils.config import ConfigReader
from misc_utils.enums import RabbitExchange, Timeframe, TradingDirection, Mode
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import unix_to_datetime, to_serializable, dt_to_unix, now_utc, extract_properties, string_to_enum
from services.service_rabbitmq import RabbitMQService
from services.api_telegram import TelegramAPIManager
from services.service_signal_persistence import SignalPersistenceService
from services.service_telegram import TelegramService


class MiddlewareService(LoggingMixin):
    """
    MiddlewareService is the central communication hub within the bot architecture. It facilitates seamless interaction
    between agents, Telegram bots, and RabbitMQ. Its primary roles include:

    1. **Client Registration**:
       - Registers agents (Generators, Sentinels) to enable communication with Telegram and RabbitMQ.
       - Associates Telegram bots with routines and chat IDs.

    2. **Signal Management**:
       - Forwards trading signals from agents to Telegram bots for approval or rejection.
       - Maintains a local cache of signals for recovery and ensures persistence through the SignalPersistenceManager.

    3. **Notification Handling**:
       - Distributes notifications from agents to their corresponding Telegram bots.
       - Ensures users are informed about critical trading events.

    4. **Telegram Integration**:
       - Sends messages to Telegram bots and manages inline interactions for trading decisions.
       - Handles user confirmations for signals and propagates those decisions to agents via RabbitMQ.

    5. **RabbitMQ Listener**:
       - Dynamically subscribes to exchanges for handling registrations, notifications, and signals.

    This class is crucial for orchestrating the communication flow within the bot infrastructure.
    """

    def __init__(self, config: ConfigReader):
        """
        Initializes the MiddlewareService instance.

        Parameters:
        - `agent` (str): Name of the agent or middleware instance for logging purposes.
        - `config` (ConfigReader): Configuration object providing environment and user-specific settings.

        Attributes:
        - `signals` (defaultdict): A cache for storing Signal objects, indexed by `message_id`.
        - `telegram_bots` (dict): Maps routine IDs to their respective TelegramService instances.
        - `telegram_bots_chat_ids` (dict): Maps routine IDs to lists of associated Telegram chat IDs.
        - `lock` (asyncio.Lock): A global lock for serializing asynchronous operations.
        - `signal_persistence_manager` (SignalPersistenceManager): Manages persistence of signals for recovery and state tracking.
        """

        super().__init__(config)
        self.agent = "Middleware"
        self.config = config
        self.signals = defaultdict(Signal)  # Cache for storing signal details keyed by message_id
        self.telegram_bots = {}  # Mapping routine_id -> TelegramService instance
        self.telegram_bots_chat_ids = {}  # Mapping routine_id -> list of chat_ids
        self.lock = asyncio.Lock()  # Global lock to serialize async operations
        self.signal_persistence_manager = None
        self.start_timestamp = None
        self.rabbitmq_s = None
        self.agents_configs = defaultdict(list)

    async def get_bot_instance(self, routine_id) -> Tuple[TelegramService, List[str]]:
        """
        Retrieves the TelegramService instance and its associated chat IDs for a given routine.

        :param routine_id: Unique identifier representing a particular bot routine.
        :return: A tuple containing the TelegramService instance (or None) and a list of chat IDs.
        """
        bot_instance = self.telegram_bots.get(routine_id, None)
        chat_ids = self.telegram_bots_chat_ids.get(routine_id, [])
        return bot_instance, chat_ids

    @exception_handler
    async def on_client_registration(self, routing_key: str, message: QueueMessage):
        """
        Handles client registration requests.

        This function is the callback for a new message on the RabbitMQ REGISTRATION exchange,
        which the middleware listens to using the static routing key 'registration.exchange'.
        It registers the client by associating the Telegram bot with the agent ID and creates
        RabbitMQ queues for the SIGNALS and NOTIFICATIONS exchanges to manage messages routed
        directly to the agents. Once the registration is completed, it sends an acknowledgment
        back to the registering routine through the RabbitMQ REGISTRATION_ACK exchange.

        Args:
            routing_key (str): The RabbitMQ routing key for the incoming message, which is static ('registration.exchange').
            message (QueueMessage): The registration message containing client details.

        Raises:
            Exception: If an error occurs while processing the registration request.
        """

        async with self.lock:
            self.info(f"Received client registration request for routine '{message.sender}'.")

            bot_name = message.get_bot_name()
            symbol = message.get_symbol()
            timeframe = message.get_timeframe()
            direction = message.get_direction()
            agent = message.sender
            bot_token = message.get("token")
            routine_id = message.get("routine_id")
            chat_ids = message.get("chat_ids", [])
            mode = string_to_enum(Mode, message.get('mode', Mode.UNDEFINED.name))

            trading_config = {
                "symbol": symbol,
                "timeframe": timeframe,
                "direction": direction,
                "bot_name": bot_name
            }

            # Retrieve or create a new Telegram bot instance
            bot_instance, existing_chat_ids = await self.get_bot_instance(routine_id)

            if not bot_instance:
                bot_instance = TelegramService(
                    self.config,
                    bot_token
                )
                self.telegram_bots[routine_id] = bot_instance
                self.telegram_bots_chat_ids[routine_id] = chat_ids

                self.info(
                    f"Starting a new Telegram bot with token '{bot_token}' for routine '{agent}'."
                )
                # Start the Telegram bot and add a handler for callback queries
                await bot_instance.start()
                await bot_instance.reset_bot_commands()

                bot_instance.add_callback_query_handler(handler=self.signal_confirmation_handler, filters=F.data.startswith("CONFIRM:"))

                self.agents_configs[bot_token].append(trading_config)

                if mode == Mode.GENERATOR:

                    async def emergency_command(m: Message):
                        """
                        Command handler for emergency close operation.
                        Presents a keyboard with available trading configurations to close positions.
                        """
                        if not self.agents_configs[bot_token]:
                            await m.answer("No trading configurations available.")
                            return

                        keyboard = []

                        for config in self.agents_configs[bot_token]:
                            if 'timeframe' not in config or config['timeframe'] is None:
                                continue

                            config_str = f"{config['symbol']}-{config['timeframe'].name}-{config['direction'].name}"
                            callback_data = f"CLOSE:{config_str}"
                            button = InlineKeyboardButton(text=config_str, callback_data=callback_data)

                            keyboard.append([button])

                        keyboard_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

                        await m.answer("Select a configuration to close all positions:", reply_markup=keyboard_markup)

                    async def emergency_callback_handler(callback_query: CallbackQuery):
                        """
                        Callback handler for emergency close buttons.
                        Processes the request to close positions for a specific configuration.
                        """
                        # Extract data from the callback
                        callback_data = callback_query.data

                        # Extract the configuration from callback_data
                        config_str = callback_data.split(":", 1)[1]

                        try:
                            symbol, timeframe_name, direction_name = config_str.split("-")

                            await callback_query.answer(f"Closing positions for {config_str}...")

                            topic = f"{symbol}.{timeframe_name}.{direction_name}"
                            exchange_name = RabbitExchange.EMERGENCY_CLOSE.name
                            exchange_type = RabbitExchange.EMERGENCY_CLOSE.exchange_type
                            trading_configuration = {
                                "symbol": symbol,
                                "timeframe": string_to_enum(Timeframe, timeframe_name),
                                "trading_direction": direction
                            }

                            await self.rabbitmq_s.publish_message(
                                exchange_name=exchange_name,
                                message=QueueMessage(
                                    sender="middleware",
                                    payload={},
                                    recipient=topic,
                                    trading_configuration=trading_configuration
                                ),
                                routing_key=topic,
                                exchange_type=exchange_type
                            )

                        except Exception as e:
                            await callback_query.answer(f"Error: {str(e)}", show_alert=True)

                    await bot_instance.register_command(command="emergency_close", handler=emergency_command, description="Close all positions for a configuration", chat_ids=chat_ids)
                    # Register callback handler with a filter for CLOSE: prefixed callbacks
                    bot_instance.add_callback_query_handler(emergency_callback_handler, F.data.startswith('CLOSE:'))

                if mode == Mode.SENTINEL:

                    async def list_command(callback_query: CallbackQuery):
                        try:
                            target_bot_token = bot_token  # Use the token from the incoming message
                            associated_routine_ids = []
                            for rid, registered_bot_service in self.telegram_bots.items():
                                # Access the token stored within the TelegramService instance
                                if registered_bot_service.token == target_bot_token:
                                    associated_routine_ids.append(rid)

                            exchange_name = RabbitExchange.LIST_OPEN_POSITION.name
                            exchange_type = RabbitExchange.LIST_OPEN_POSITION.exchange_type

                            for associated_routine_ids in associated_routine_ids:
                                await self.rabbitmq_s.publish_message(
                                    exchange_name=exchange_name,
                                    message=QueueMessage(
                                        sender="middleware",
                                        payload={},
                                        recipient=associated_routine_ids,
                                        trading_configuration=trading_config
                                    ),
                                    routing_key=associated_routine_ids,
                                    exchange_type=exchange_type
                                )

                        except Exception as e:
                            await callback_query.answer(f"Error: {str(e)}", show_alert=True)

                    await bot_instance.register_command(command="list_open_positions", handler=list_command, description="List all open positions", chat_ids=chat_ids)

            else:
                # Merge new chat_ids with existing ones
                updated_chat_ids = set(existing_chat_ids)
                new_chat_ids = [c for c in chat_ids if c not in updated_chat_ids]
                self.telegram_bots_chat_ids[routine_id].extend(new_chat_ids)

            # Send a registration confirmation message
            registration_message = self.message_with_details(
                f"🤖 Agent {agent} registered successfully.",
                agent, bot_name, symbol, timeframe, direction
            )
            if not self.config.is_silent_start():
                await self.send_telegram_message(routine_id, registration_message)

            # Register RabbitMQ listeners for signals and notifications
            self.info(f"Registering signal listener for routine '{agent}'...")
            await self.rabbitmq_s.register_listener(
                exchange_name=RabbitExchange.SIGNALS.name,
                callback=self.on_strategy_signal,
                routing_key=routine_id,
                exchange_type=RabbitExchange.SIGNALS.exchange_type
            )

            self.info(f"Registering notification listener for routine '{agent}'...")
            await self.rabbitmq_s.register_listener(
                exchange_name=RabbitExchange.NOTIFICATIONS.name,
                callback=self.on_notification,
                routing_key=routine_id,
                exchange_type=RabbitExchange.NOTIFICATIONS.exchange_type
            )

            # Send an acknowledgment back to the registering routine
            self.info(f"Sending registration acknowledgment to routine '{routine_id}'.")
            await self.rabbitmq_s.publish_message(
                exchange_name=RabbitExchange.REGISTRATION_ACK.name,
                message=QueueMessage(
                    sender="middleware",
                    payload=message.payload,
                    recipient=message.sender,
                    trading_configuration=message.trading_configuration
                ),
                routing_key=routine_id,
                exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type
            )

    @exception_handler
    async def on_notification(self, routing_key: str, message: QueueMessage):
        """
        Processes notification messages.

        This function is a callback for messages received from the RabbitMQ exchange 'NOTIFICATIONS'.
        When a client is registered using the `on_client_registration` function, a queue is created
        on the 'NOTIFICATIONS' exchange with a routing key corresponding to the agent ID.

        This function forwards the received notification to the Telegram bot associated with the
        agent ID corresponding to the routing key in the message.

        Args:
            routing_key (str): The routine ID (agent ID) to which the notification pertains.
            message (QueueMessage): The notification message containing text and context.

        Raises:
            Exception: If an error occurs while sending the notification.
        """

        async with self.lock:
            self.info(f"Received notification '{message}' for routine '{routing_key}'.")

            if self.config.is_silent_start() and self.is_bootstrapping():
                self.info(f"Silent mode active, will not send the notification \"{message.to_json()}\"")
                return

            routine_id = routing_key
            direction = message.get_direction()
            timeframe = message.get_timeframe()
            agent = message.sender
            bot_name = message.get_bot_name()

            notification_text = self.message_with_details(
                message.get("message"),
                agent,
                bot_name,
                message.get_symbol(),
                timeframe,
                direction
            )
            await self.send_telegram_message(routine_id, notification_text)

    @exception_handler
    async def send_telegram_message(self, routine_id: str, message: str, reply_markup=None):
        """
        Sends a text message (optionally with an inline keyboard) to all chat IDs associated with a routine.

        :param routine_id: The routine identifier for retrieving the correct bot and chat IDs.
        :param message: The text to be sent.
        :param reply_markup: Optional inline keyboard or reply markup.
        """
        bot_instance, chat_ids = await self.get_bot_instance(routine_id)
        message_log = message.replace("\n", " \\n ")
        for chat_id in chat_ids:
            self.debug(
                f"Sending a message to Telegram chat_id '{chat_id}' for routine '{routine_id}'. "
                f"Message content: {message_log}"
            )
            await bot_instance.send_message(chat_id, message, reply_markup)

    @exception_handler
    async def on_strategy_signal(self, routing_key: str, message: QueueMessage):
        """
        Processes a trading signal published by a strategy.

        This function is a callback for a message published on the RabbitMQ SIGNALS exchange,
        using a TOPIC format like {symbol.timeframe.direction}. It saves the signal in the
        database to ensure recovery in case of an executor reboot and sends a notification
        message to the Telegram bot linked to the agent that generated the signal. The notification
        includes inline buttons for confirmation.

        Args:
            routing_key (str): The routine ID of the target bot associated with the Telegram bot.
            message (QueueMessage): The message containing a 'Signal' instance.

        Raises:
            Exception: If any error occurs during signal processing.
        """
        async with self.lock:
            self.info(f"Received strategy signal: {message}")
            routine_id = routing_key

            signal: Signal = Signal.from_json(message.payload)
            # Extract fields from the message for clarity
            signal_id = signal.signal_id
            bot_name = message.get_bot_name()
            symbol = message.get_symbol()
            timeframe = message.get_timeframe()
            direction = message.get_direction()
            candle = signal.candle
            agent = message.sender

            # Cache the signal if not already present
            if signal_id not in self.signals:
                self.signals[signal_id] = signal

            # Convert Unix timestamps to human-readable strings
            t_open = unix_to_datetime(candle['time_open']).strftime('%H:%M')
            t_close = unix_to_datetime(candle['time_close']).strftime('%H:%M')

            # Debug log before saving the signal
            self.debug(
                f"Preparing to save a new signal with ID={signal_id}, "
                f"Symbol={symbol}, Timeframe={timeframe}, Direction={direction}, "
                f"CandleClose={t_close}"
            )

            # Save the signal in the persistence layer
            save_result = await self.signal_persistence_manager.save_signal(signal=signal)

            # Log based on the save operation result
            if not save_result:
                self.error(
                    f"Error while saving the new signal with the following details:\n"
                    f"  signal_id: {signal_id}\n"
                    f"  agent: {agent}\n"
                    f"  symbol: {symbol}\n"
                    f"  timeframe: {timeframe}\n"
                    f"  direction: {direction}\n"
                    f"  candle: {candle}\n"
                    f"  routine_id: {routine_id}\n"
                    f"Saving operation returned: {save_result}", exec_info=False
                )
            else:
                self.info(
                    f"Signal '{signal_id}' successfully saved with "
                    f"symbol='{symbol}', timeframe='{timeframe}', direction='{direction}'."
                )

            # Prepare the Telegram message for the user
            trading_opportunity_message = (
                f"🚀 <b>Alert!</b> A new trading opportunity has been identified "
                f"on frame {t_open} - {t_close}.\n\n"
                f"🔔 Would you like to confirm the placement of this order?\n\n"
                f"Select an option to place the order or block this signal (by default, "
                f"the signal will be <b>ignored</b> if no selection is made)."
            )

            reply_markup = self.get_signal_confirmation_dialog(signal_id)
            detailed_message = self.message_with_details(
                trading_opportunity_message,
                agent,
                bot_name,
                symbol,
                timeframe,
                direction
            )

            # Send the Telegram message with inline keyboard
            await self.send_telegram_message(routine_id, detailed_message, reply_markup=reply_markup)

    @exception_handler
    async def signal_confirmation_handler(self, callback_query: CallbackQuery):
        """
        Handles user confirmation or blocking of a trading signal.

        This function processes user input from Telegram inline buttons to confirm or block a trading signal.
        It updates the signal's status in the database to ensure persistence and recoverability after system
        reboots, ensuring that executors can restore the state of open signals for their respective topics.

        The function also updates the Telegram inline keyboard to reflect the user's choice and broadcasts the
        decision to relevant components via RabbitMQ using the 'SIGNALS_CONFIRMATION' exchange. The message is
        routed using the topic format {symbol.timeframe.direction}, ensuring that all executors subscribed to
        this topic (registered via `on_client_registration`) receive the updated choice.

        Args:
            callback_query (CallbackQuery): The user's interaction with the inline button.

        Raises:
            Exception: If an error occurs during signal confirmation.
        """

        async with self.lock:
            self.debug(f"Callback query received: {callback_query}")

            # The callback data is in CSV format: "signal_id,1" or "signal_id,0"
            signal_id, confirmed_flag = callback_query.data.replace("CONFIRM:", "").split(',')
            confirmed = (confirmed_flag == '1')

            user_username = callback_query.from_user.username or "Unknown User"
            user_id = callback_query.from_user.id or -1

            self.debug(
                f"Parsed callback data - signal_id={signal_id}, confirmed={confirmed}, "
                f"user_username={user_username}, user_id={user_id}"
            )

            # Try to retrieve the signal from cache
            signal = self.signals.get(signal_id)
            if not signal:
                self.debug(f"Signal {signal_id} not found in cache, attempting to retrieve from persistence.")
                signal = await self.signal_persistence_manager.get_signal(signal_id)
                if not signal:
                    self.error(f"Signal {signal_id} not found in persistence!", exec_info=False)
                    return

            # Check if the signal has expired (after the close of the next candle minus 5 seconds)
            current_time = now_utc()
            signal_entry_time = unix_to_datetime(signal.candle['time_close'])
            next_candle_end_time = signal_entry_time + timedelta(seconds=signal.timeframe.to_seconds() - 5)

            if current_time > next_candle_end_time:
                self.debug(f"Signal '{signal_id}' expired: {current_time} > {next_candle_end_time}.")
                confirmation_message = "⏰ It's too late, the signal has expired."
                message_str = self.message_with_details(
                    confirmation_message,
                    signal.agent,
                    signal.bot_name,
                    signal.symbol,
                    signal.timeframe,
                    signal.direction
                )
                await self.send_telegram_message(signal.routine_id, message_str)
                return

            # Prepare the updated inline keyboard based on user's choice
            csv_confirm = f"CONFIRM:{signal_id},1"
            csv_block = f"CONFIRM:{signal_id},0"
            if confirmed:
                keyboard = [[
                    InlineKeyboardButton(text="Confirmed ✔️", callback_data=csv_confirm),
                    InlineKeyboardButton(text="Block", callback_data=csv_block)
                ]]
            else:
                keyboard = [[
                    InlineKeyboardButton(text="Confirm", callback_data=csv_confirm),
                    InlineKeyboardButton(text="Block ✔️", callback_data=csv_block)
                ]]

            # Update the Telegram message inline keyboard
            await callback_query.message.edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )

            # Update signal with user decision
            signal.confirmed = confirmed
            signal.user = user_username
            signal.update_tms = dt_to_unix(now_utc())

            # Save updated signal status to persistence
            save_result = await self.signal_persistence_manager.update_signal_status(signal)
            if not save_result:
                self.error(f"Error while updating the status for signal '{signal_id}' to '{confirmed}'.", exec_info=False)

            # Publish the signal update to RabbitMQ for all executors
            topic = f"{signal.symbol}.{signal.timeframe.name}.{signal.direction.name}"
            payload = {
                "signal": to_serializable(signal)
            }
            exchange_name = RabbitExchange.SIGNALS_CONFIRMATIONS.name
            exchange_type = RabbitExchange.SIGNALS_CONFIRMATIONS.exchange_type
            trading_configuration = {
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "trading_direction": signal.direction
            }

            await self.rabbitmq_s.publish_message(
                exchange_name=exchange_name,
                message=QueueMessage(
                    sender="middleware",
                    payload=payload,
                    recipient=signal.routine_id,
                    trading_configuration=trading_configuration
                ),
                routing_key=topic,
                exchange_type=exchange_type
            )

            # Send confirmation message to the user
            choice_text = "✅ Confirm" if confirmed else "🚫 Block"
            time_open = unix_to_datetime(signal.candle['time_open']).strftime('%Y-%m-%d %H:%M:%S UTC')
            time_close = unix_to_datetime(signal.candle['time_close']).strftime('%Y-%m-%d %H:%M:%S UTC')

            confirmation_message = (
                f"ℹ️ Your choice to <b>{choice_text}</b> the signal for the candle from "
                f"{time_open} to {time_close} has been saved."
            )
            message_str = self.message_with_details(
                confirmation_message,
                signal.agent,
                signal.bot_name,
                signal.symbol,
                signal.timeframe,
                signal.direction
            )
            await self.send_telegram_message(signal.routine_id, message_str)

            self.debug(f"Confirmation message sent to routine '{signal.routine_id}'.")

    def get_signal_confirmation_dialog(self, signal_id: str) -> InlineKeyboardMarkup:
        """
        Builds the default inline keyboard for a new trading signal, allowing the user
        to either Confirm or Block the signal.

        :param signal_id: Unique identifier for the trading signal.
        :return: InlineKeyboardMarkup with 'Confirm' and 'Block' buttons.
        """
        self.debug("Creating the default signal confirmation dialog.")
        csv_confirm = f"CONFIRM:{signal_id},1"
        csv_block = f"CONFIRM:{signal_id},0"

        keyboard = [[
            InlineKeyboardButton(text="Confirm", callback_data=csv_confirm),
            InlineKeyboardButton(text="Block", callback_data=csv_block)
        ]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    def message_with_details(
            self,
            message: str,
            agent: str,
            bot_name: str,
            symbol: str,
            timeframe: Timeframe,
            direction: TradingDirection
    ) -> str:
        """
        Builds a detailed Telegram message containing extra info such as agent name, bot name,
        symbol, timeframe, and direction.

        :param message: Main text to display.
        :param agent: Name of the agent sending the message.
        :param bot_name: Name of the trading bot (if any).
        :param symbol: Trading pair (e.g., BTCUSDT).
        :param timeframe: The timeframe (e.g., 1H, 4H).
        :param direction: Trading direction (LONG or SHORT).
        :return: The original message with appended details.
        """
        details = []

        items = [
            ("⚙️", f"<b>Agent:</b> {agent}") if agent else None,
            ("💻", f"<b>Bot:</b> {bot_name}") if bot_name else None,
            ("💱", f"<b>Symbol:</b> {symbol}") if symbol else None,
            ("📊", f"<b>Timeframe:</b> {timeframe.name}") if timeframe else None,
            (("📈" if direction.name == "LONG" else "📉"), f"<b>Direction:</b> {direction.name}") if direction else None,
        ]
        items = [item for item in items if item]

        details = [
            f"{emoji} {'┌─' if i == 0 else '└─' if i == len(items) - 1 else '├─'} {text}"
            for i, (emoji, text) in enumerate(items)
        ]

        details_str = "\n".join(details)
        if details:
            return f"{message}\n\n<b>Details:</b>\n\n{details_str}"
        return message

    def is_bootstrapping(self) -> bool:
        """
        Returns True if bootstrapping is active (within 5 minutes).
        """
        return self.start_timestamp is not None and (time.time() - self.start_timestamp) <= (60 * 5)

    @exception_handler
    async def routine_start(self):
        """
        Starts the middleware service.

        This method performs the following tasks:
        1. **Registers REGISTRATION Listener**:
           - Sets up a RabbitMQ listener for incoming REGISTRATION messages.
           - Listens on the REGISTRATION exchange using the static routing key `registration.exchange`.
           - Ensures proper routing and handling of client registration requests.

        2. **Initializes Telegram API Manager**:
           - Prepares the Telegram API Manager for handling communication with Telegram bots.
           - Ensures the API manager is fully operational for processing Telegram messages and callbacks.

        3. **Signal Persistence Manager**:
           - Starts the SignalPersistenceManager to handle the storage and recovery of trading signals.

        This method is typically called once during the startup phase of the middleware service to establish necessary connections
        and prepare the service for operation.
        """
        self.rabbitmq_s = await RabbitMQService.get_instance()
        self.signal_persistence_manager = await SignalPersistenceService.get_instance(config=self.config)

        self.info(f"Starting middleware service '{self.agent}'.")
        exchange_name = RabbitExchange.REGISTRATION.name
        exchange_type = RabbitExchange.REGISTRATION.exchange_type
        routing_key = RabbitExchange.REGISTRATION.routing_key

        self.info("Registering listener for client REGISTRATION messages.")
        await self.rabbitmq_s.register_listener(
            exchange_name=exchange_name,
            callback=self.on_client_registration,
            routing_key=routing_key,
            exchange_type=exchange_type
        )

        self.info("Initializing TelegramAPIManager.")
        await TelegramAPIManager(self.config).initialize()
        self.info("Middleware service started successfully.")

        await self.signal_persistence_manager.start()

        self.start_timestamp = time.time()

    @exception_handler
    async def routine_stop(self):
        """
        Stops the middleware service.

        This method performs the following tasks:
        1. **Stops Telegram Bots**:
           - Iterates through all active Telegram bots associated with the middleware.
           - Gracefully shuts down each Telegram bot, ensuring no lingering connections.

        2. **Shuts Down Telegram API Manager**:
           - Cleans up resources used by the Telegram API Manager.
           - Ensures proper termination of all Telegram-related processes.

        3. **Stops Signal Persistence Manager**:
           - Finalizes and safely stops the SignalPersistenceManager.
           - Ensures that all signal data is saved and resources are released.

        This method is called during the shutdown phase of the middleware service to ensure a clean and orderly termination
        of all resources and services.
        """

        self.info(f"Stopping middleware service '{self.agent}'.")

        for routine_id, bot in self.telegram_bots.items():
            self.info(f"Stopping Telegram bot '{bot.agent}' for routine '{routine_id}'.")
            await bot.stop()

        self.info("Shutting down TelegramAPIManager.")
        await TelegramAPIManager(self.config).shutdown()
        self.info("Middleware service has been stopped.")

        await self.signal_persistence_manager.stop()
