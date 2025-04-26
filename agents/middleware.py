import asyncio
import time
from copy import deepcopy
from datetime import timedelta
from typing import Dict, List, Set, Any, Optional

from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

from dto.QueueMessage import QueueMessage
from dto.Signal import Signal, SignalStatus
from misc_utils.config import ConfigReader
from misc_utils.enums import RabbitExchange, Timeframe, TradingDirection, Mode
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import unix_to_datetime, dt_to_unix, now_utc, string_to_enum, new_id
from services.api_telegram import TelegramAPIManager
from services.service_rabbitmq import RabbitMQService
from services.service_signal_persistence import SignalPersistenceService
from services.service_telegram import TelegramService


class MiddlewareService(LoggingMixin):
    """
    MiddlewareService acts as the central hub for communication among agents, Telegram bots, and RabbitMQ.

    Responsibilities:
      - Client Registration: Map agents and routines to Telegram bots and configure chat IDs.
      - Signal Management: Cache and persist trading signals, and forward them for user confirmation.
      - Notification Handling: Route notifications to the appropriate Telegram chats.
      - Telegram Integration: Manage message sending and inline interactions.
      - RabbitMQ Listener: Subscribe dynamically to exchanges for registration, notifications, and signals.
    """

    def __init__(self, config: ConfigReader):
        """
        Initialize the MiddlewareService instance.

        Parameters:
          - config (ConfigReader): Provides environment and user-specific settings.

        Attributes:
          - signals: Cache for Signal objects indexed by message_id.
          - telegram_bots: Maps bot tokens to TelegramService instances.
          - agents_properties: Maps routine IDs to agent-specific properties.
          - agents_ui_config: Maps routine IDs to UI configuration (bot token and chat IDs).
          - lock: Global asynchronous lock for serializing operations.
          - signal_persistence_manager: Manages storage and recovery of trading signals.
        """
        super().__init__(config)
        self.agent = "Middleware"
        self.id = new_id()
        self.config = config

        # Map tokens to TelegramService instances
        self.telegram_bots: Dict[str, TelegramService] = {}

        # Map agents to their properties
        self.agents_properties: Dict[str, Dict[str, Any]] = {}
        # Map routine IDs to UI configurations (bot token and chat IDs)
        self.agents_ui_config: Dict[str, Dict[str, Any]] = {}

        self.agents_status: Dict[str, str] = {}

        self.lock = asyncio.Lock()  # Global lock to serialize async operations
        self.signal_persistence_manager = None
        self.start_timestamp = None
        self.rabbitmq_s = None

        self.token_to_bots: Dict[str, TelegramService] = {}

    async def _handle_emergency_close_command(self, m: Message):
        """
        Handle the emergency close command.

        Displays an inline keyboard with trading configurations for closing positions.
        """
        try:
            bot_token = m.bot.token  # Extract bot token from the message

            # Verify that the bot token is registered
            if bot_token not in self.telegram_bots:
                self.warning(f"Received command for unregistered bot token: {bot_token[:5]}...")

            keyboard = []

            # Filter routines associated with the current bot token
            associated_routines_ids = {
                key: ui_config
                for key, ui_config in self.agents_ui_config.items()
                if 'token' in ui_config and ui_config["token"] == bot_token
            }

            if not associated_routines_ids:
                await m.answer("No trading configurations available for this bot.")
                return

            for routine_id, ui_agent in associated_routines_ids.items():
                agent = self.agents_properties[routine_id]

                symbol = agent["symbol"]
                timeframe = agent["timeframe"]
                direction = agent["direction"]
                config_str = f"{symbol}-{timeframe.name}-{direction.name}"
                callback_data = f"CLOSE:{config_str}"
                button = InlineKeyboardButton(text=config_str, callback_data=callback_data)

                keyboard.append([button])

            # Ensure at least one button is added
            if not keyboard:
                await m.answer("No valid trading configurations found to display.")
                return

            keyboard_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
            await m.answer("Select a configuration to close all positions:", reply_markup=keyboard_markup)

        except Exception as e:
            self.error(f"Error in _handle_emergency_close_command: {e}", exc_info=e)
            await m.answer("An error occurred while processing your command.")

    async def _handle_emergency_close_callback(self, callback_query: CallbackQuery):
        # Extract configuration from callback data
        callback_data = callback_query.data
        config_str = callback_data.split(":", 1)[1]

        try:
            symbol, timeframe_name, direction_name = config_str.split("-")

            await callback_query.answer(f"Closing positions for {config_str}...")

            topic = f"{symbol}.{timeframe_name}.{direction_name}"
            exchange_name = RabbitExchange.jupiter_commands.name
            exchange_type = RabbitExchange.jupiter_commands.exchange_type

            meta_inf = MessageMetaInf(
                symbol=symbol,
                timeframe=string_to_enum(Timeframe, timeframe_name),
                direction=string_to_enum(TradingDirection, direction_name)
            )

            await self.rabbitmq_s.publish_message(
                exchange_name=exchange_name,
                message=QueueMessage(
                    sender="middleware",
                    payload={},
                    recipient=topic,
                    meta_inf=meta_inf
                ),
                routing_key=f"command.emergency_close.{topic}",
                exchange_type=exchange_type
            )

        except Exception as e:
            self.error(f"Error processing emergency close callback for {config_str}: {e}", exc_info=e)
            await callback_query.answer(f"Error: {str(e)}", show_alert=True)

    async def _handle_list_positions_command(self, m: Message):
        """
        Handle the /list_open_positions command.

        Identifies routines linked to the current bot token and sends a request for their open positions.
        """
        bot_token = m.bot.token  # Extract bot token from the message
        try:
            # Identify routines associated with the current bot token
            associated_routines_ids = {
                key: ui_config
                for key, ui_config in self.agents_ui_config.items()
                if 'token' in ui_config and ui_config["token"] == bot_token
            }

            if not associated_routines_ids:
                await m.answer("No configurations found for this bot to list positions.")
                return

            exchange_name = RabbitExchange.jupiter_commands.name
            exchange_type = RabbitExchange.jupiter_commands.exchange_type

            for routine_id, ui_agent in associated_routines_ids.items():
                agent = self.agents_properties[routine_id]
                meta_inf = MessageMetaInf(
                    routine_id=routine_id,
                    agent_name=agent["agent_name"],
                    symbol=agent["symbol"],
                    timeframe=agent["timeframe"],
                    direction=agent["direction"],
                    ui_token=bot_token
                )

                routing_key = f"command.list_open_positions.{routine_id}"

                await self.rabbitmq_s.publish_message(
                    exchange_name=exchange_name,
                    message=QueueMessage(
                        sender="middleware",
                        payload={},  # Additional info can be added if necessary
                        recipient=agent["agent_name"],
                        meta_inf=meta_inf
                    ),
                    routing_key=routing_key,
                    exchange_type=exchange_type
                )

        except Exception as e:
            self.error(f"Error in _handle_list_positions_command: {e}", exc_info=e)
            await m.answer(f"Error processing command: {str(e)}")

    async def _register_generator_commands(self, agent_id: str, bot_token: str, chat_ids: List[str]):
        """
        Register commands for generator agents on the Telegram bot.
        """
        bot_instance = self.telegram_bots[bot_token]

        await bot_instance.register_command(
            command="emergency_close",
            handler=self._handle_emergency_close_command,
            description="Close all positions for a configuration",
            chat_ids=chat_ids
        )
        bot_instance.add_callback_query_handler(self._handle_emergency_close_callback, F.data.startswith('CLOSE:'))
        bot_instance.add_callback_query_handler(handler=self.signal_confirmation_handler, filters=F.data.startswith("CONFIRM:"))

    async def _register_sentinel_commands(self, agent: str, bot_token: str, chat_ids: List[str]):
        """
        Register commands for sentinel agents on the Telegram bot.
        """
        bot_instance = self.telegram_bots[bot_token]

        await bot_instance.register_command(
            command="list_open_positions",
            handler=self._handle_list_positions_command,
            description="List all open positions",
            chat_ids=chat_ids
        )

    @exception_handler
    async def on_client_registration(self, routing_key: str, message: QueueMessage):
        """
        Process client registration requests from RabbitMQ.

        Registers a client by associating a Telegram bot with an agent, configures
        the UI and persistence settings, and sends a registration acknowledgment.

        Args:
          - routing_key: The routing key for registration messages.
          - message: The registration message containing client details.
        """
        async with self.lock:
            self.info(f"Received client registration request for routine '{message.sender}'.")

            bot_name = message.get_meta_inf().get_bot_name()
            instance_name = message.get_meta_inf().get_instance_name()
            symbol = message.get_meta_inf().get_symbol()
            timeframe = message.get_meta_inf().get_timeframe()
            direction = message.get_meta_inf().get_direction()
            agent_name = message.get_meta_inf().get_agent_name()
            bot_token = message.get_meta_inf().get_ui_token()
            routine_id = message.get_meta_inf().get_routine_id()
            chat_ids = message.get_meta_inf().get_ui_users()
            mode = string_to_enum(Mode, message.get('mode', Mode.UNDEFINED.name))

            # Check if a client with the same configuration is already registered on the same instance.
            duplicate_registration = next(
                (rid for rid, props in self.agents_properties.items()
                 if props.get('instance_name') == instance_name and
                 props.get('symbol') == symbol and
                 props.get('timeframe') == timeframe and
                 props.get('direction') == direction and
                 rid != routine_id),
                None
            )
            if duplicate_registration:
                self.warning(
                    f"A client is already registered for the configuration "
                    f"(instance: {instance_name}, symbol: {symbol}, timeframe: {timeframe}, direction: {direction}) "
                    f"under routine '{duplicate_registration}'. Ignoring registration from routine '{routine_id}'."
                )
                await self._send_acknowledgment(message, routine_id, False)
                return

            if routine_id in self.agents_properties:
                self.warning(f"Routine '{routine_id}' (Agent: {agent_name}) is re-registering. Updating properties.")

            self.agents_properties[routine_id] = {
                'agent_name': agent_name,
                'bot_name': bot_name,
                'instance_name': instance_name,
                'symbol': symbol,
                'timeframe': timeframe,
                'direction': direction,
                'mode': mode
            }
            self.debug(f"Stored properties for routine {routine_id}: {self.agents_properties[routine_id]}")

            # Update or store the UI configuration for the routine.
            self.agents_ui_config[routine_id] = {
                'token': bot_token,
                'chat_ids': chat_ids
            }
            self.debug(f"Stored UI config for routine {routine_id}: Token {bot_token[:5]}..., Chats {chat_ids}")

            if bot_token not in self.telegram_bots:
                bot_instance = TelegramService(self.config, bot_token)
                self.telegram_bots[bot_token] = bot_instance

                self.info(f"Starting new Telegram bot with token '{bot_token}' for routine '{agent_name}'.")

                # Start the Telegram bot and reset its commands.
                await bot_instance.start()
                await bot_instance.reset_bot_commands()

            if mode == Mode.GENERATOR:
                await self._register_generator_commands(agent_name, bot_token, chat_ids)
            if mode == Mode.SENTINEL:
                await self._register_sentinel_commands(agent_name, bot_token, chat_ids)

            # Send registration confirmation message.
            registration_message = self.message_with_details(
                f"🤖 Agent {agent_name} registered successfully.",
                agent_name,
                bot_name,
                symbol,
                timeframe,
                direction
            )
            if not self.config.is_silent_start():
                await self.send_telegram_message(routine_id, registration_message)

            self.info(f"Sending registration acknowledgment to routine '{routine_id}'.")

            await self._send_acknowledgment(message, routine_id, True)

    async def _send_acknowledgment(self, message: QueueMessage, routine_id: str, success: bool):
        """
        Sends a registration acknowledgment via RabbitMQ.

        Copies the original payload, updates the 'success' flag, and publishes the message
        using the provided routine_id as the routing key.

        Args:
            message (QueueMessage): The original registration message.
            routine_id (str): The routing key identifier.
            success (bool): Registration success status.
        """
        payload_copy = deepcopy(message.payload)
        payload_copy["success"] = success

        routing_key = f"system.registration_ack.{routine_id}"

        await self.rabbitmq_s.publish_message(
            exchange_name=RabbitExchange.jupiter_system.name,
            message=QueueMessage(
                sender="middleware",
                payload=payload_copy,
                recipient=message.sender,
                meta_inf=message.meta_inf
            ),
            routing_key=routing_key,
            exchange_type=RabbitExchange.jupiter_system.exchange_type
        )

    @exception_handler
    async def _handle_notification(self, routing_key: str, message: QueueMessage):
        """
        Primary callback for notifications received from the 'jupiter_notifications' exchange.

        Parses the routing key to delegate the message for user-specific or broadcast processing.
        """
        self.info(f"Received notification via RK '{routing_key}'. Message ID: {message.message_id}")

        parts = routing_key.split('.')

        # Validate routing key format
        if len(parts) < 3 or parts[0] != 'notification':
            self.warning(f"Invalid routing key format received: {routing_key}. Skipping message.")
            return

        scope = parts[1]

        # Process user-specific notifications
        if scope == 'user':
            if len(parts) != 3:
                self.warning(f"Invalid 'user' scope routing key format: {routing_key}. Expected 'notification.user.{{routine_id}}'. Skipping.")
                return
            routine_id = parts[2]

            if self.config.is_silent_start() and self.is_bootstrapping(routine_id):
                self.info(f"Silent mode active; notification suppressed: \"{message.to_json()}\"")
                return

            await self._process_user_notification(routine_id, message)

        # Process broadcast notifications
        elif scope == 'broadcast':
            if len(parts) < 4:
                self.warning(f"Invalid 'broadcast' RK: {routing_key}. Minimum 4 parts required. Skipping.")
                return

            # Mandatory and optional parts
            instance_name = parts[2]
            symbol = parts[3] if len(parts) > 3 else None
            timeframe_str = parts[4] if len(parts) > 4 else None
            direction_str = parts[5] if len(parts) > 5 else None

            await self._process_broadcast_notification(
                instance_name=instance_name,
                symbol=symbol,
                timeframe_str=timeframe_str,
                direction_str=direction_str,
                message=message
            )

        else:
            self.warning(f"Unknown scope '{scope}' in routing key: {routing_key}. Skipping message.")

    @exception_handler
    async def _process_user_notification(self, routine_id: str, message: QueueMessage):
        """
        Send a notification to the specified user's chat based on routine ID.
        """
        self.debug(f"Processing user-specific notification for routine_id: {routine_id}")

        if routine_id not in self.agents_ui_config:
            self.warning(f"Cannot process user notification. No UI config found for routine_id: {routine_id}")
            return

        ui_config = self.agents_ui_config[routine_id]
        token = ui_config.get("token")
        chat_ids = ui_config.get("chat_ids", [])

        if not token or not chat_ids:
            self.warning(f"Missing token or chat_ids for routine_id: {routine_id}")
            return

        if token not in self.telegram_bots:
            self.error(f"Telegram bot instance not found for token associated with routine_id: {routine_id}. Cannot send message.")
            return

        bot_instance = self.telegram_bots[token]
        # Format message content using additional metadata if necessary
        formatted_message = self._format_notification_content(message)

        self.info(f"Sending user notification from routine {routine_id} to {len(chat_ids)} chats via bot {token[:5]}...")
        for chat_id in chat_ids:
            try:
                await bot_instance.send_message(chat_id, formatted_message)
                self.debug(f"Enqueued message for chat_id: {chat_id} (Routine: {routine_id})")
            except Exception as e:
                self.error(f"Failed to send message for chat_id {chat_id} (Routine: {routine_id})", exc_info=e)

    @exception_handler
    async def _process_broadcast_notification(self, instance_name: str,
                                              symbol: Optional[str], timeframe_str: Optional[str],
                                              direction_str: Optional[str], message: QueueMessage):
        """
        Process and send a broadcast notification.

        Args:
          - instance_name: Required instance name filter.
          - symbol: Optional trading symbol filter.
          - timeframe_str: Optional timeframe filter.
          - direction_str: Optional trading direction filter.
          - message: The notification message.
        """
        self.debug(f"Processing broadcast: Inst={instance_name}, Sym={symbol}, TF={timeframe_str}, Dir={direction_str}")

        # 1. Identify routine IDs matching the provided filters
        relevant_routine_ids = self._find_routines_for_broadcast(
            instance_name=instance_name,
            symbol=symbol,
            timeframe_str=timeframe_str,
            direction_str=direction_str
        )

        ready_routine_ids = [
            routine_id for routine_id in relevant_routine_ids
            if  self.is_bootstrapping(routine_id)
        ]

        # 2. Aggregate unique targets by token
        targets: Dict[str, Set[str]] = self._aggregate_targets(ready_routine_ids)
        if not targets:
            self.warning(f"No targets for broadcast (Routines: {ready_routine_ids}). Skipping.")
            return

        # 3. Format the notification message
        formatted_message = self._format_notification_content(message)

        self.info(f"Sending broadcast notification to {len(targets)} bots / {sum(len(c) for c in targets.values())} total unique chats.")
        for token, unique_chat_ids in targets.items():
            if token not in self.telegram_bots:
                self.error(f"Telegram bot instance not found for token {token[:5]}... Cannot send broadcast.")
                continue

            bot_instance = self.telegram_bots[token]
            self.debug(f"Enqueuing broadcast message via bot {token[:5]} to {len(unique_chat_ids)} unique chats.")
            for chat_id in unique_chat_ids:
                try:
                    await bot_instance.send_message(chat_id, formatted_message)
                except Exception as e:
                    self.error(f"Failed to send broadcast message for chat_id {chat_id} via bot {token[:5]}...", exc_info=e)

    def _format_notification_content(self, message: QueueMessage) -> str:
        """
        Format the content of a notification message.

        Extracts the main content and appends additional details using metadata.
        """
        main_content = message.payload.get("message", "N/A")
        meta = message.get_meta_inf()
        return self.message_with_details(
            message=main_content,
            agent=meta.get_agent_name(),
            bot_name=meta.get_bot_name(),
            symbol=meta.get_symbol(),
            timeframe=meta.get_timeframe(),
            direction=meta.get_direction()
        )

    def _find_routines_for_broadcast(self,
                                     instance_name: str,
                                     symbol: Optional[str] = None,
                                     timeframe_str: Optional[str] = None,
                                     direction_str: Optional[str] = None) -> List[str]:
        """
        Find routine IDs that match the given criteria.

        instance_name is required; symbol, timeframe, and direction are optional filters.
        """
        matching_ids = []
        self.debug(f"Finding routines for broadcast: Inst='{instance_name}', Sym='{symbol}', TF='{timeframe_str}', Dir='{direction_str}'")

        target_timeframe: Optional[Timeframe] = None
        if timeframe_str:
            try:
                target_timeframe = string_to_enum(Timeframe, timeframe_str.upper())
            except KeyError:
                self.warning(f"Invalid timeframe '{timeframe_str}' in broadcast. Ignoring timeframe filter.")

        target_direction: Optional[TradingDirection] = None
        if direction_str:
            try:
                target_direction = string_to_enum(TradingDirection, direction_str.upper())
            except KeyError:
                self.warning(f"Invalid direction '{direction_str}' in broadcast. Ignoring direction filter.")

        for routine_id, properties in self.agents_properties.items():
            # Mandatory filter: instance name must match
            if properties.get('instance_name') != instance_name:
                continue

            if symbol is not None and properties.get('symbol') != symbol:
                continue
            if target_timeframe is not None and properties.get('timeframe') != target_timeframe:
                continue
            if target_direction is not None and properties.get('trading_direction') != target_direction:
                continue

            matching_ids.append(routine_id)

        self.debug(f"Found {len(matching_ids)} routines: {matching_ids}")
        return matching_ids

    def _aggregate_targets(self, routine_ids: List[str]) -> Dict[str, Set[str]]:
        """
        Aggregate unique Telegram bot tokens and their associated chat IDs from the given routine IDs.
        """
        targets: Dict[str, Set[str]] = {}
        for routine_id in routine_ids:
            if routine_id in self.agents_ui_config:
                ui_config = self.agents_ui_config[routine_id]
                token = ui_config.get("token")
                chat_ids = ui_config.get("chat_ids", [])
                if token and chat_ids:
                    if token not in targets:
                        targets[token] = set()
                    targets[token].update(chat_ids)
            else:
                self.warning(f"No UI config for routine_id {routine_id} during target aggregation.")
        return targets

    @exception_handler
    async def on_broadcast_notification(self, routing_key: str, message: QueueMessage):
        """
        Handle broadcast notifications.

        Sends a single notification per Telegram bot for the specified symbol and instance.
        """
        self.info(f"Received broadcast notification for key '{routing_key}'.")

        symbol, instance_name = routing_key.split(":")

        # Filter routines matching symbol and instance_name
        agents_for_symbol = {
            k: v
            for k, v in self.agents_properties.items()
            if v.get("symbol") == symbol and v.get("instance_name") == instance_name
        }

        if not agents_for_symbol:
            self.info(f"No registered agents for symbol '{symbol}'.")
            return

        # Aggregate chat IDs per bot token
        tokens_to_chat_ids: Dict[str, Set[str]] = {}
        for routine_id, agent in agents_for_symbol.items():
            ui_config = self.agents_ui_config[routine_id]
            token = ui_config["token"]
            user_ids = ui_config["chat_ids"]
            if token not in tokens_to_chat_ids:
                tokens_to_chat_ids[token] = set(user_ids)
            else:
                tokens_to_chat_ids[token].update(user_ids)

        message_text = message.get("message", message.to_json())

        agent = message.sender
        direction = message.get_meta_inf().get_direction()
        timeframe = message.get_meta_inf().get_timeframe()
        bot_name = message.get_meta_inf().get_bot_name()
        symbol = message.get_meta_inf().get_symbol()

        notification_text = self.message_with_details(
            message_text,
            agent,
            bot_name,
            symbol,
            timeframe,
            direction
        )

        # Send the notification to all aggregated chat IDs
        for token, chat_ids in tokens_to_chat_ids.items():
            bot_instance = self.telegram_bots.get(token)
            if not bot_instance:
                self.warning(f"Telegram bot not found for token {token}.")
                continue

            for chat_id in chat_ids:
                self.debug(f"Sending broadcast to bot {token} for chat_id {chat_id}.")
                await bot_instance.send_message(chat_id, notification_text)

    @exception_handler
    async def send_telegram_message(self, routine_id: str, message: str, reply_markup=None):
        """
        Send a Telegram message to all chat IDs associated with the given routine.

        Args:
          - routine_id: Identifier to retrieve the proper bot and chat IDs.
          - message: The message text to send.
          - reply_markup: Optional inline keyboard or additional markup.
        """
        telegram_config = self.agents_ui_config[routine_id]
        token = telegram_config["token"]
        chat_ids = telegram_config["chat_ids"]

        bot_instance = self.telegram_bots[token]
        message_log = message.replace("\n", " \\n ")
        for chat_id in chat_ids:
            self.debug(f"Sending message to chat_id '{chat_id}' for routine '{routine_id}'. Content: {message_log}")
            await bot_instance.send_message(chat_id, message, reply_markup)

    @exception_handler
    async def on_strategy_opportunity(self, routing_key: str, message: QueueMessage):
        """
        Process a trading signal from a strategy.

        Caches and persists the signal, then sends a notification with inline confirmation buttons
        to the corresponding Telegram bot.
        """
        async with self.lock:
            signal_id = message.payload["signal_id"]

            self.debug(f"Received signal opportunity for signal with ID {signal_id}")
            self.debug(f"Retrieving signal with ID {signal_id} in persistence.")

            signal: Signal = await self.signal_persistence_manager.get_signal(signal_id)
            if not signal:
                self.error(f"Signal with ID {signal_id} not found in persistence.")
                return
            self.debug(f"Signal with ID {signal_id} retrieved from persistence: {signal}")

            direction = message.get_meta_inf().get_direction()
            timeframe = message.get_meta_inf().get_timeframe()
            bot_name = message.get_meta_inf().get_bot_name()
            symbol = message.get_meta_inf().get_symbol()
            candle = signal.opportunity_candle
            agent = message.sender

            t_open = unix_to_datetime(candle['time_open']).strftime('%H:%M')
            t_close = unix_to_datetime(candle['time_close']).strftime('%H:%M')

            self.debug(f"Preparing to save signal ID={signal_id}, Symbol={symbol}, Timeframe={timeframe}, Direction={direction}, CandleClose={t_close}")

            save_result = await self.signal_persistence_manager.save_signal(signal=signal)

            if not save_result:
                self.error(
                    f"Error saving signal:\n"
                    f"  ID: {signal_id}\n"
                    f"  Agent: {agent}\n"
                    f"  Symbol: {symbol}\n"
                    f"  Timeframe: {timeframe}\n"
                    f"  Direction: {direction}\n"
                    f"  Candle: {candle}\n"
                    f"Result: {save_result}", exc_info=False
                )
            else:
                self.info(f"Signal '{signal_id}' saved successfully (Symbol: {symbol}, Timeframe: {timeframe}, Direction: {direction}).")

            trading_opportunity_message = (
                f"🚀 <b>Alert!</b> A new trading opportunity has been identified "
                f"from {t_open} to {t_close}.\n\n"
                f"🔔 Do you want to confirm placing this order?\n\n"
                f"If no selection is made, the signal will be <b>ignored</b>."
            )

            topic = f"{message.meta_inf.bot_name}.{message.meta_inf.instance_name}.{message.meta_inf.symbol}.{message.meta_inf.timeframe.name}.{message.meta_inf.direction.name}"
            reply_markup = self.get_signal_confirmation_dialog(signal_id, topic)
            detailed_message = self.message_with_details(
                trading_opportunity_message,
                agent,
                bot_name,
                symbol,
                timeframe,
                direction
            )

            await self.send_telegram_message(message.meta_inf.routine_id, detailed_message, reply_markup=reply_markup)

    @exception_handler
    async def signal_confirmation_handler(self, callback_query: CallbackQuery):
        """
        Handle user confirmation or rejection of a trading signal via inline buttons.

        Updates the signal's status in the persistence layer and publishes the updated signal via RabbitMQ.
        """
        async with self.lock:
            self.debug(f"Received callback query: {callback_query}")

            signal_id, confirmed_flag, topic = callback_query.data.replace("CONFIRM:", "").split(',')
            confirmed = (confirmed_flag == '1')

            bot_name, instance_name, symbol, timeframe_str, direction_str = topic.split('.')
            timeframe = string_to_enum(Timeframe, timeframe_str.upper())
            direction = string_to_enum(TradingDirection, direction_str.upper())

            # Get user who made a choice
            user = callback_query.from_user
            user_id = user.id or -1
            if user.username:
                user_username = user.username
            elif hasattr(user, 'full_name') and user.full_name:
                user_username = user.full_name
            else:
                first = user.first_name or ""
                last = " " + user.last_name if user.last_name else ""
                user_username = (first + last).strip() or str(user.id)

            self.debug(f"Parsed callback data - signal_id={signal_id}, confirmed={confirmed}, user={user_username}, id={user_id}")

            # Retrieve signal from cache or persistence if necessary
            signal: Optional[Signal] = await self.signal_persistence_manager.get_signal(signal_id)
            if not signal:
                self.error(f"Signal {signal_id} not found in persistence!", exc_info=False)
                return

            # Verify that the signal has not expired
            current_time = now_utc()
            signal_entry_time = unix_to_datetime(signal.opportunity_candle['time_close'])
            next_candle_end_time = signal_entry_time + timedelta(seconds=signal.timeframe.to_seconds() - 5)

            target_routine_id = None
            target_bot_token = None
            target_chat_ids = None

            for rid, props in self.agents_properties.items():
                if (props.get('instance_name') == instance_name and
                        props.get('symbol') == symbol and
                        props.get('timeframe') == timeframe and
                        props.get('direction') == direction):
                    target_routine_id = rid
                    self.debug(f"Found active routine_id '{target_routine_id}' matching stable identifiers.")
                    break

            if target_routine_id and target_routine_id in self.agents_ui_config:
                ui_config = self.agents_ui_config[target_routine_id]
                target_bot_token = ui_config.get("token")
                target_chat_ids = ui_config.get("chat_ids")
                if not target_bot_token or not target_chat_ids:
                    self.error(f"Missing UI config (token/chats) for active routine_id {target_routine_id}")
            elif target_routine_id:
                self.error(f"Found active routine_id {target_routine_id} but no UI config associated.")
            else:
                self.warning(f"No active agent found matching instance='{instance_name}', symbol='{symbol}', tf='{timeframe_str}', dir='{direction_str}'. Cannot send feedback.")

            if current_time > next_candle_end_time:
                self.debug(f"Signal '{signal_id}' expired: {current_time} > {next_candle_end_time}.")
                confirmation_message = "⏰ Too late, the signal has expired."
                message_str = self.message_with_details(
                    confirmation_message,
                    signal.agent,
                    signal.bot_name,
                    signal.symbol,
                    signal.timeframe,
                    signal.direction
                )
                await self.send_telegram_message(target_routine_id, message_str)
                return

            # Prepare updated inline keyboard based on the user's choice
            csv_confirm = f"CONFIRM:{signal_id},1,{topic}"
            csv_block = f"CONFIRM:{signal_id},0,{topic}"
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

            await callback_query.message.edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )

            # Update signal with user's decision
            signal.confirmed = confirmed
            signal.user = user_username
            signal.update_tms = dt_to_unix(now_utc())
            signal.status = SignalStatus.CONFIRMED if confirmed else SignalStatus.BLOCKED

            save_result = await self.signal_persistence_manager.update_signal_status(signal)
            if not save_result:
                self.error(f"Error updating status for signal '{signal_id}' to '{confirmed}'.", exc_info=False)
                return

            routing_key = f"event.signal.confirmation.{symbol}.{timeframe_str}.{direction_str}"
            payload = {
                "signal_id": signal.signal_id
            }
            exchange_name = RabbitExchange.jupiter_events.name
            exchange_type = RabbitExchange.jupiter_events.exchange_type

            meta_inf = MessageMetaInf(
                bot_name=bot_name,
                instance_name=instance_name,
                routine_id=symbol,
                agent_name="middleware",
                symbol=symbol,
                timeframe=timeframe,
                direction=direction
            )

            await self.rabbitmq_s.publish_message(
                exchange_name=exchange_name,
                message=QueueMessage(
                    sender="middleware",
                    payload=payload,
                    recipient=target_routine_id,
                    meta_inf=meta_inf
                ),
                routing_key=routing_key,
                exchange_type=exchange_type
            )

            choice_text = "✅ Confirm" if confirmed else "🚫 Block"
            time_open = unix_to_datetime(signal.opportunity_candle['time_open']).strftime('%Y-%m-%d %H:%M:%S UTC')
            time_close = unix_to_datetime(signal.opportunity_candle['time_close']).strftime('%Y-%m-%d %H:%M:%S UTC')

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
            await self.send_telegram_message(target_routine_id, message_str)

            self.debug(f"Confirmation message sent to routine '{target_routine_id}'.")

    def get_signal_confirmation_dialog(self, signal_id: str, topic: str) -> InlineKeyboardMarkup:
        """
        Create an inline keyboard for signal confirmation with 'Confirm' and 'Block' options.
        """
        self.debug("Creating default signal confirmation dialog.")
        csv_confirm = f"CONFIRM:{signal_id},1,{topic}"
        csv_block = f"CONFIRM:{signal_id},0,{topic}"

        keyboard = [[
            InlineKeyboardButton(text="Confirm", callback_data=csv_confirm),
            InlineKeyboardButton(text="Block", callback_data=csv_block)
        ]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    @exception_handler
    async def on_agent_status_update(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            try:
                routine_id = message.payload.get("routine_id")
                new_status = message.payload.get("status")

                if not routine_id:
                    self.warning(f"Received agent status update without 'routine_id' in payload. RK: {routing_key}, Payload: {message.payload}")
                    return

                if routine_id in self.agents_status and new_status in ["ready", "bootstrapping"]:
                    old_status = self.agents_status[routine_id]
                    self.agents_status[routine_id] = new_status
                    self.info(f"Updated status for routine {routine_id} from '{old_status}' to '{new_status}'.")
                else:
                    self.warning(f"Received status update for unknown routine '{routine_id}' or invalid status '{new_status}'. Payload: {message.payload}")
            except Exception as e:
                self.error("Error processing agent status update", exc_info=e)

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
        Append key details (agent, bot, symbol, timeframe, direction) to the message.

        Returns the message concatenated with formatted details.
        """
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

    def is_bootstrapping(self, routine_id: str) -> bool:
        """
        Return True if bootstrapping mode is active (within the first 5 minutes after startup).
        """
        return self.agents_status.get(routine_id, "unknown") == "bootstrapping"

    @exception_handler
    async def routine_start(self):
        """
        Start the middleware service.

        Sets up RabbitMQ listeners for client registration and notifications, initializes the Telegram API,
        and starts the signal persistence manager.
        """

        self.rabbitmq_s = await RabbitMQService.get_instance()
        self.signal_persistence_manager = await SignalPersistenceService.get_instance(config=self.config)

        self.info(f"Starting middleware service '{self.agent}'.")

        # Listener for Client Registration Requests
        exchange_name_system = RabbitExchange.jupiter_system.name
        exchange_type_system = RabbitExchange.jupiter_system.exchange_type
        routing_key_registration = "middleware.registration"
        queue_name_registration = f"middleware.{routing_key_registration}.{self.config.get_instance_name()}"

        self.info(f"Registering listener for Client Registration requests on exchange '{exchange_name_system}' (RK: '{routing_key_registration}', Queue: '{queue_name_registration}').")
        await self.rabbitmq_s.register_listener(
            exchange_name=exchange_name_system,
            exchange_type=exchange_type_system,
            routing_key=routing_key_registration,
            callback=self.on_client_registration,
            queue_name=queue_name_registration
        )

        # Listener for Signal Opportunity Events
        routing_key_signal_opportunity = "event.signal.opportunity.#"
        queue_name_signal_opportunity = f"middleware.event.signal.opportunity.{self.config.get_instance_name()}"

        self.info(f"Registering listener for Signal Opportunity events on exchange '{RabbitExchange.jupiter_events.name}' (RK: '{routing_key_signal_opportunity}', Queue: '{queue_name_signal_opportunity}').")
        await self.rabbitmq_s.register_listener(
            exchange_name=RabbitExchange.jupiter_events.name,
            exchange_type=RabbitExchange.jupiter_events.exchange_type,
            routing_key=routing_key_signal_opportunity,
            callback=self.on_strategy_opportunity,
            queue_name=queue_name_signal_opportunity
        )

        # Listener for Notifications (User-specific and Broadcast)
        routing_key_notifications = "notification.#"  # Listens to both user and broadcast
        queue_name_notifications = f"middleware.notifications.notification.{self.config.get_instance_name()}.{self.id}"

        self.info(f"Registering listener for All Notifications on exchange '{RabbitExchange.jupiter_notifications.name}' (RK: '{routing_key_notifications}', Queue: '{queue_name_notifications}').")
        await self.rabbitmq_s.register_listener(
            exchange_name=RabbitExchange.jupiter_notifications.name,
            exchange_type=RabbitExchange.jupiter_notifications.exchange_type,
            routing_key=routing_key_notifications,
            callback=self._handle_notification,
            queue_name=queue_name_notifications
        )

        # Listener for Agent Status Updates
        exchange_name_system = RabbitExchange.jupiter_system.name
        exchange_type_system = RabbitExchange.jupiter_system.exchange_type
        routing_key_agent_status = "middleware.agent.status"
        queue_name_agent_status = f"middleware.system.agent.status"
        self.info(f"Registering listener for Agent Status updates on exchange '{RabbitExchange.jupiter_system.name}' (RK: '{routing_key_agent_status}', Queue: '{queue_name_agent_status}').")
        await self.rabbitmq_s.register_listener(
            # *** CHANGE EXCHANGE ***
            exchange_name=RabbitExchange.jupiter_system.name,
            exchange_type=RabbitExchange.jupiter_system.exchange_type,
            routing_key=routing_key_agent_status,
            callback=self.on_agent_status_update,
            queue_name=queue_name_agent_status
        )

        # Initialize Telegram and Persistence
        self.info("Initializing Telegram API Manager.")
        await TelegramAPIManager(self.config).initialize()

        await self.signal_persistence_manager.start()
        self.info("Middleware service started successfully.")

        self.start_timestamp = time.time()

    @exception_handler
    async def routine_stop(self):
        """
        Stop the middleware service.

        Gracefully shuts down all Telegram bots, stops the Telegram API Manager,
        and terminates the signal persistence manager.
        """
        self.info(f"Stopping middleware service '{self.agent}'.")

        for routine_id, bot in self.telegram_bots.items():
            self.info(f"Stopping Telegram bot '{bot.agent}' for routine '{routine_id}'.")
            await bot.stop()

        self.info("Shutting down Telegram API Manager.")
        await TelegramAPIManager(self.config).shutdown()
        self.info("Middleware service has been stopped.")

        if self.signal_persistence_manager is not None:
            await self.signal_persistence_manager.stop()
