import argparse
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.enums import RabbitExchange, Timeframe, TradingDirection
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum, unix_to_datetime, to_serializable
from services.rabbitmq_service import RabbitMQService
from services.telegram_service import TelegramService


class MiddlewareService:

    def __init__(self, config: ConfigReader, queue_service: RabbitMQService):
        self.worker_id = f"{config.get_bot_name()}_middleware"

        self.logger = BotLogger(worker_id=self.worker_id, level=config.get_bot_logging_level().upper())
        self.queue_service = queue_service
        self.signals = {}
        self.telegram_bots = {}
        self.telegram_bots_chat_ids = {}
        self.lock = asyncio.Lock()

    async def get_bot_instance(self, sentinel_id) -> (TelegramService, str):
        t_bot = self.telegram_bots.get(sentinel_id, None)
        t_chat_ids = self.telegram_bots_chat_ids.get(sentinel_id, [])
        return t_bot, t_chat_ids

    @exception_handler
    async def on_client_registration(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received client registration request: {message}")
            bot_name = message.sender
            bot_token = message.get("token")
            sentinel_id = message.get("sentinel_id")
            chat_ids = message.get("chat_ids", [])  # Default to empty list if chat_ids is not provided

            # Recupera istanza del bot e chat_ids
            bot_instance, existing_chat_ids = await self.get_bot_instance(sentinel_id)

            # Se il bot non esiste, crealo e inizializzalo
            if not bot_instance:
                bot_instance = TelegramService(bot_token, "telegram_service")
                self.telegram_bots[sentinel_id] = bot_instance
                self.telegram_bots_chat_ids[sentinel_id] = chat_ids
                await bot_instance.start()
                bot_instance.add_callback_query_handler(handler=self.signal_confirmation_handler)
            else:
                # Aggiungi nuovi chat_id solo se non già esistenti
                updated_chat_ids = set(existing_chat_ids)  # Usa set per evitare duplicati
                new_chat_ids = [chat_id for chat_id in chat_ids if chat_id not in updated_chat_ids]
                self.telegram_bots_chat_ids[sentinel_id].extend(new_chat_ids)

            # Invia messaggi di conferma ai nuovi chat_id
            for chat_id in self.telegram_bots_chat_ids[sentinel_id]:
                await bot_instance.send_message(chat_id, f"🤖 Routine {bot_name} registered successfully.")

            # Registra i listener per Signals e Notifications
            await self.queue_service.register_listener(
                exchange_name=RabbitExchange.SIGNALS.name,
                callback=self.on_strategy_signal,
                routing_key=sentinel_id,
                exchange_type=RabbitExchange.SIGNALS.exchange_type
            )

            await self.queue_service.register_listener(
                exchange_name=RabbitExchange.NOTIFICATIONS.name,
                callback=self.on_notification,
                routing_key=sentinel_id,
                exchange_type=RabbitExchange.NOTIFICATIONS.exchange_type
            )

            await self.queue_service.publish_message(
                exchange_name=RabbitExchange.REGISTRATION_ACK.name,
                message=QueueMessage(sender="middleware", payload=message.payload),
                routing_key=sentinel_id,
                exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

    @exception_handler
    async def on_notification(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received notification \"{message}\" for routine id {routing_key}")
            sentinel_id = routing_key
            t_bot, t_chat_ids = await self.get_bot_instance(sentinel_id)
            direction = string_to_enum(TradingDirection, message.get("direction"))
            timeframe = string_to_enum(Timeframe, message.get("timeframe"))
            message_with_details = self.message_with_details(message.get("message"), message.sender, message.get("symbol"), timeframe, direction)
            for chat_id in t_chat_ids:
                await t_bot.send_message(chat_id, message_with_details)

    @exception_handler
    async def on_strategy_signal(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received strategy signal: {message}")
            sentinel_id = routing_key
            signal_obj = {
                "bot_name": message.sender,
                "signal_id": message.message_id,
                "symbol": message.get("symbol"),
                "timeframe": string_to_enum(Timeframe, message.get("timeframe")),
                "direction": string_to_enum(TradingDirection, message.get("direction")),
                "candle": message.get("candle"),
                "sentinel_id": sentinel_id
            }

            if not signal_obj['signal_id'] in self.signals:
                self.signals[message.message_id] = signal_obj

            t_open = unix_to_datetime(signal_obj['candle']['time_open']).strftime('%H:%M')
            t_close = unix_to_datetime(signal_obj['candle']['time_close']).strftime('%H:%M')

            trading_opportunity_message = (f"🚀 <b>Alert!</b> A new trading opportunity has been identified on frame {t_open} - {t_close}.\n\n"
                                           f"🔔 Would you like to confirm the placement of this order?\n\n"
                                           "Select an option to place the order or ignore this signal (by default, the signal will be <b>ignored</b> if no selection is made).")

            reply_markup = self.get_signal_confirmation_dialog(signal_obj.get('signal_id'))
            message = self.message_with_details(trading_opportunity_message, signal_obj['bot_name'], signal_obj['symbol'], signal_obj['timeframe'], signal_obj['direction'])

            # use routing_key as telegram bot token

            t_bot, t_chat_ids = await self.get_bot_instance(sentinel_id)
            for chat_id in t_chat_ids:
                await t_bot.send_message(chat_id, message, reply_markup=reply_markup)

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
            timeframe = signal.get("timeframe")
            direction = signal.get("direction")

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
            await self.queue_service.publish_message(
                exchange_name=exchange_name,
                message=QueueMessage(sender="middleware", payload=payload),
                routing_key=topic,
                exchange_type=exchange_type)

            candle = signal['candle']

            choice_text = "✅ Confirm" if confirmed else "🚫 Ignore"

            time_open = unix_to_datetime(candle['time_open'])
            time_close = unix_to_datetime(candle['time_close'])

            open_dt_formatted = time_open.strftime('%Y-%m-%d %H:%M:%S UTC')
            close_dt_formatted = time_close.strftime('%Y-%m-%d %H:%M:%S UTC')

            t_message = f"ℹ️ Your choice to <b>{choice_text}</b> the signal for the candle from {open_dt_formatted} to {close_dt_formatted} has been successfully saved."

            sentinel_id = signal.get("sentinel_id")
            t_bot, t_chats_id = await self.get_bot_instance(sentinel_id)
            message_with_details = self.message_with_details(t_message, signal.get("bot_name"), signal.get("symbol"), signal.get("timeframe"), signal.get("direction"))

            for chat_id in t_chats_id:
                await t_bot.send_message(chat_id, message_with_details)

            self.logger.debug(f"Confirmation message sent: {message_with_details}")

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

    def message_with_details(self, message: str, bot_name: str, symbol: str, timeframe: Timeframe, direction: TradingDirection):
        direction_emoji = "📈" if direction.name == "LONG" else "📉️"
        detailed_message = (
            f"{message}\n\n"
            "<b>Details:</b>\n\n"
            f"💻 <b>Bot name:</b> {bot_name}\n"
            f"💱 <b>Symbol:</b> {symbol}\n"
            f"📊 <b>Timeframe:</b> {timeframe.name}\n"
            f"{direction_emoji} <b>Direction:</b> {direction.name}"
        )
        return detailed_message

    @exception_handler
    async def start(self):
        self.logger.info(f"Middleware service {self.worker_id} started")

        exchange_name, exchange_type, routing_key = RabbitExchange.REGISTRATION.name, RabbitExchange.REGISTRATION.exchange_type, RabbitExchange.REGISTRATION.routing_key
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.on_client_registration,
            routing_key=routing_key,
            exchange_type=exchange_type)

        self.logger.info("Middleware service started successfully")

    @exception_handler
    async def stop(self):
        self.logger.info(f"Middleware service {self.worker_id} stopped")
        await self.queue_service.stop()

        for topic, bots in self.telegram_bots:
            for bot in bots:
                self.logger.info(f"Stopping bot {bot.worker_id}")
                await bot.stop()
