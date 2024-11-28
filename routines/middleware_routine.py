import asyncio

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.enums import RabbitExchange, Timeframe, TradingDirection
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum, unix_to_datetime, to_serializable
from services.rabbitmq_service import RabbitMQService
from services.telegram_api_manager import TelegramAPIManager
from services.telegram_service import TelegramService


class MiddlewareService:

    def __init__(self, routine_label: str, config: ConfigReader):
        self.routine_label = routine_label
        self.logger = BotLogger(name=self.routine_label, level=config.get_bot_logging_level().upper())
        self.signals = {}
        self.config = config
        self.telegram_bots = {}
        self.telegram_bots_chat_ids = {}
        self.lock = asyncio.Lock()

    async def get_bot_instance(self, routine_id) -> (TelegramService, str):
        t_bot = self.telegram_bots.get(routine_id, None)
        t_chat_ids = self.telegram_bots_chat_ids.get(routine_id, [])
        return t_bot, t_chat_ids

    @exception_handler
    async def on_client_registration(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received client registration request for routine '{message.sender}'")
            bot_name = message.get_bot_name()
            symbol = message.get_symbol()
            timeframe = string_to_enum(Timeframe, message.get_timeframe())
            direction = string_to_enum(TradingDirection, message.get_direction())
            routine_label = message.sender
            bot_token = message.get("token")
            routine_id = message.get("routine_id")
            chat_ids = message.get("chat_ids", [])  # Default to empty list if chat_ids is not provided

            # Recupera istanza del bot e chat_ids
            bot_instance, existing_chat_ids = await self.get_bot_instance(routine_id)

            # Se il bot non esiste, crealo e inizializzalo
            if not bot_instance:
                bot_instance = TelegramService(bot_token, f"{bot_name}_telegram_servie", logging_level=self.config.get_bot_logging_level())
                self.telegram_bots[routine_id] = bot_instance
                self.telegram_bots_chat_ids[routine_id] = chat_ids

                self.logger.info(f"Starting new Telegram bot {bot_token} for routine '{routine_label}'")
                await bot_instance.start()
                bot_instance.add_callback_query_handler(handler=self.signal_confirmation_handler)
            else:
                # Aggiungi nuovi chat_id solo se non già esistenti
                updated_chat_ids = set(existing_chat_ids)  # Usa set per evitare duplicati
                new_chat_ids = [chat_id for chat_id in chat_ids if chat_id not in updated_chat_ids]
                self.telegram_bots_chat_ids[routine_id].extend(new_chat_ids)

            registration_notification_message = self.message_with_details(f"🤖 Routine {routine_label} registered successfully.", routine_label, bot_name, symbol, timeframe, direction)
            # Invia messaggi di conferma ai nuovi chat_id
            await self.send_telegram_message(routine_id, registration_notification_message)

            # Registra i listener per Signals e Notifications

            self.logger.info(f"Registered listener for signals on routine '{routine_label}'")
            await RabbitMQService.register_listener(
                exchange_name=RabbitExchange.SIGNALS.name,
                callback=self.on_strategy_signal,
                routing_key=routine_id,
                exchange_type=RabbitExchange.SIGNALS.exchange_type
            )

            self.logger.info(f"Registered listener for notification on routine '{routine_label}'")
            await RabbitMQService.register_listener(
                exchange_name=RabbitExchange.NOTIFICATIONS.name,
                callback=self.on_notification,
                routing_key=routine_id,
                exchange_type=RabbitExchange.NOTIFICATIONS.exchange_type
            )

            self.logger.info(f"Sending registration ack to routine '{routine_id}'")
            await RabbitMQService.publish_message(
                exchange_name=RabbitExchange.REGISTRATION_ACK.name,
                message=QueueMessage(sender="middleware", payload=message.payload, recipient=message.sender, trading_configuration=message.trading_configuration),
                routing_key=routine_id,
                exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

    @exception_handler
    async def on_notification(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received notification \"{message}\" for routine '{routing_key}'")
            routine_id = routing_key
            direction = string_to_enum(TradingDirection, message.get_direction())
            timeframe = string_to_enum(Timeframe, message.get_timeframe())
            routine_label = message.sender
            bot_name = message.get_bot_name()
            message_str = self.message_with_details(message.get("message"), routine_label, bot_name, message.get_symbol(), timeframe, direction)
            await self.send_telegram_message(routine_id, message_str)

    @exception_handler
    async def send_telegram_message(self, routine_id, message, reply_markup=None):
        t_bot, t_chat_ids = await self.get_bot_instance(routine_id)
        for chat_id in t_chat_ids:
            self.logger.debug(f"Sending Telegram message {message} to chat {chat_id}")
            await t_bot.send_message(chat_id, message, reply_markup)

    @exception_handler
    async def on_strategy_signal(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received strategy signal: {message}")
            routine_id = routing_key
            signal_obj = {
                "bot_name": message.get_bot_name(),
                "signal_id": message.message_id,
                "symbol": message.get_symbol(),
                "timeframe": string_to_enum(Timeframe, message.get_timeframe()),
                "direction": string_to_enum(TradingDirection, message.get_direction()),
                "candle": message.get("candle"),
                "routine_id": routine_id,
                "routine_label": message.sender
            }

            if not signal_obj['signal_id'] in self.signals:
                self.signals[message.message_id] = signal_obj

            t_open = unix_to_datetime(signal_obj['candle']['time_open']).strftime('%H:%M')
            t_close = unix_to_datetime(signal_obj['candle']['time_close']).strftime('%H:%M')

            trading_opportunity_message = (f"🚀 <b>Alert!</b> A new trading opportunity has been identified on frame {t_open} - {t_close}.\n\n"
                                           f"🔔 Would you like to confirm the placement of this order?\n\n"
                                           "Select an option to place the order or ignore this signal (by default, the signal will be <b>ignored</b> if no selection is made).")

            reply_markup = self.get_signal_confirmation_dialog(signal_obj.get('signal_id'))
            message = self.message_with_details(trading_opportunity_message, signal_obj["routine_label"], signal_obj['bot_name'], signal_obj['symbol'], signal_obj['timeframe'], signal_obj['direction'])

            # use routing_key as telegram bot token
            await self.send_telegram_message(routine_id, message, reply_markup=reply_markup)

    @exception_handler
    async def signal_confirmation_handler(self, callback_query: CallbackQuery):
        async with self.lock:
            self.logger.debug(f"Callback query answered: {callback_query}")

            # Retrieve data from callback, now in CSV format
            signal_id, confirmed_flag = callback_query.data.split(',')
            self.logger.debug(f"Data retrieved from callback: signal_id: {signal_id}, confirmed_flag: {confirmed_flag}")
            confirmed = (confirmed_flag == '1')
            user_username = callback_query.from_user.username if callback_query.from_user.username else "Unknown User"
            user_id = callback_query.from_user.id if callback_query.from_user.id else -1

            self.logger.debug(f"Parsed data - signal_id: {signal_id}, confirmed: {confirmed}, user_username: {user_username}, user_id: {user_id}")

            signal = self.signals[signal_id]

            symbol = signal.get("symbol")
            routine_label = signal.get("routine_label")
            bot_name = signal.get("bot_name")
            timeframe = signal.get("timeframe")  # Already as enum
            direction = signal.get("direction")  # Already as enum
            routine_id = signal.get("routine_id")

            csv_confirm = f"{signal_id},1"
            csv_block = f"{signal_id},0"
            self.logger.debug(f"CSV formatted data - confirm: {csv_confirm}, block: {csv_block}")

            # Set the keyboard buttons with updated callback data
            if confirmed:
                keyboard = [
                    [
                        InlineKeyboardButton(text="Confirmed ✔️", callback_data=csv_confirm),
                        InlineKeyboardButton(text="Ignored", callback_data=csv_block)
                    ]
                ]
            else:
                keyboard = [
                    [
                        InlineKeyboardButton(text="Confirm", callback_data=csv_confirm),
                        InlineKeyboardButton(text="Ignored ✔️", callback_data=csv_block)
                    ]
                ]
            self.logger.debug(f"Keyboard set with updated callback data: {keyboard}")

            # Update the inline keyboard
            await callback_query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

            # Update the database

            topic = f"{symbol}.{timeframe.name}.{direction.name}"
            payload = {
                "confirmation": confirmed,
                "signal": to_serializable(signal),
                "username": user_username
            }
            exchange_name, exchange_type = RabbitExchange.SIGNALS_CONFIRMATIONS.name, RabbitExchange.SIGNALS_CONFIRMATIONS.exchange_type
            trading_configuration = {"symbol": symbol, "timeframe": timeframe, "trading_direction": direction}
            await RabbitMQService.publish_message(
                exchange_name=exchange_name,
                message=QueueMessage(sender="middleware", payload=payload, recipient=routine_id, trading_configuration=trading_configuration),
                routing_key=topic,
                exchange_type=exchange_type)

            candle = signal['candle']

            choice_text = "✅ Confirm" if confirmed else "🚫 Ignore"

            time_open = unix_to_datetime(candle['time_open'])
            time_close = unix_to_datetime(candle['time_close'])

            open_dt_formatted = time_open.strftime('%Y-%m-%d %H:%M:%S UTC')
            close_dt_formatted = time_close.strftime('%Y-%m-%d %H:%M:%S UTC')

            t_message = f"ℹ️ Your choice to <b>{choice_text}</b> the signal for the candle from {open_dt_formatted} to {close_dt_formatted} has been successfully saved."
            routine_id = signal.get("routine_id")
            message_str = self.message_with_details(t_message, routine_label, bot_name, symbol, timeframe, direction)
            await self.send_telegram_message(routine_id, message_str)

            self.logger.debug(f"Confirmation message sent: {message_str}")

    def get_signal_confirmation_dialog(self, signal_id) -> InlineKeyboardMarkup:
        self.logger.debug("Starting signal confirmation dialog creation")
        csv_confirm = f"{signal_id},1"
        csv_ignore = f"{signal_id},0"

        keyboard = [
            [
                InlineKeyboardButton(text="Confirm", callback_data=csv_confirm),
                InlineKeyboardButton(text="Ignore", callback_data=csv_ignore)
            ]
        ]
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        self.logger.debug(f"Keyboard created: {keyboard}")

        return reply_markup

    def message_with_details(self, message: str, routine_label: str, bot_name: str, symbol: str, timeframe: Timeframe, direction: TradingDirection):
        direction_emoji = "📈" if direction.name == "LONG" else "📉️"
        detailed_message = (
            f"{message}\n\n"
            "<b>Details:</b>\n\n"
            f"⚙️ <b>Agent:</b> {routine_label}\n"
            f"💻 <b>Bot:</b> {bot_name}\n"
            f"💱 <b>Symbol:</b> {symbol}\n"
            f"📊 <b>Timeframe:</b> {timeframe.name}\n"
            f"{direction_emoji} <b>Direction:</b> {direction.name}"
        )
        return detailed_message

    @exception_handler
    async def routine_start(self):
        self.logger.info(f"Starting middleware service {self.routine_label}")
        exchange_name, exchange_type, routing_key = RabbitExchange.REGISTRATION.name, RabbitExchange.REGISTRATION.exchange_type, RabbitExchange.REGISTRATION.routing_key
        await RabbitMQService.register_listener(
            exchange_name=exchange_name,
            callback=self.on_client_registration,
            routing_key=routing_key,
            exchange_type=exchange_type)

        await TelegramAPIManager().initialize()
        self.logger.info("Middleware service started successfully")

    @exception_handler
    async def routine_stop(self):
        self.logger.info(f"Middleware service {self.routine_label} stopped")

        for routine_id, bot in self.telegram_bots:
            self.logger.info(f"Stopping bot {bot.routine_label}")
            await bot.stop()

        await TelegramAPIManager().shutdown()
