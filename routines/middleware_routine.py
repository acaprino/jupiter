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

    def __init__(self, config: ConfigReader):
        self.worker_id = f"{config.get_bot_name()}_middleware"

        self.logger = BotLogger(worker_id=self.worker_id, level=config.get_bot_logging_level().upper())
        self.queue_service = RabbitMQService(
            worker_id=self.worker_id,
            user=config.get_rabbitmq_username(),
            password=config.get_rabbitmq_password(),
            rabbitmq_host=config.get_rabbitmq_host(),
            port=config.get_rabbitmq_port())

        self.signals = {}
        self.telegram_bots = {}
        self.telegram_bots_chat_ids = {}
        self.lock = asyncio.Lock()

    async def get_bot_instance(self, bot_token) -> (TelegramService, str):
        if not bot_token in self.telegram_bots:
            t_bot = TelegramService(bot_token, f"telegram_{bot_token.replace(':', '_')}")
            self.telegram_bots[bot_token] = t_bot
            await t_bot.start()
            t_bot.add_callback_query_handler(handler=self.signal_confirmation_handler)

        t_bot = self.telegram_bots[bot_token]
        t_chat_ids = self.telegram_bots_chat_ids[bot_token]
        return t_bot, t_chat_ids

    @exception_handler
    async def signal_confirmation_handler(self, callback_query: CallbackQuery):
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

        t_bot, t_chats_id = self.get_bot_instance(signal.get("bot_token"))
        message_with_details = self.message_with_details(t_message, signal.get("bot_name"), signal.get("symbol"), signal.get("timeframe"), signal.get("direction"))

        for chat_id in t_chats_id:
            await t_bot.send_message(chat_id, message_with_details)

        self.logger.debug(f"Confirmation message sent: {message_with_details}")

    @exception_handler
    async def on_strategy_signal(self, routing_key: str, message: QueueMessage):
        async with self.lock:
            self.logger.info(f"Received strategy signal: {message}")

            signal_obj = {
                "bot_name": message.sender,
                "signal_id": message.message_id,
                "symbol": message.get("symbol"),
                "timeframe": string_to_enum(Timeframe, message.get("timeframe")),
                "direction": string_to_enum(TradingDirection, message.get("direction")),
                "candle": message.get("candle"),
                "bot_token": routing_key
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

            t_bot, t_chat_ids = await self.get_bot_instance(routing_key)
            for chat_id in t_chat_ids:
                await t_bot.send_message(chat_id, message, reply_markup=reply_markup)

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
        await self.queue_service.start()

        exchange_name, exchange_type, routing_key = RabbitExchange.REGISTRATION.name, RabbitExchange.REGISTRATION.exchange_type, RabbitExchange.REGISTRATION.routing_key
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.on_client_registration,
            routing_key=routing_key,
            exchange_type=exchange_type)

        self.logger.info("Middleware service started successfully")

    @exception_handler
    async def on_client_registration(self, routing_key: str, message: QueueMessage):
        self.logger.info(f"Received client registration request: {message}")
        bot_name = message.sender
        bot_token = message.get("token")
        chat_ids = message.get("chat_ids")

        bot_instance = await self.get_bot_instance(bot_token)

        if not bot_instance:
            t_bot = TelegramService(bot_token, f"telegram_service")
            self.telegram_bots[bot_token] = t_bot
            self.telegram_bots_chat_ids[bot_token] = chat_ids
            await t_bot.start()
            t_bot.add_callback_query_handler(handler=self.signal_confirmation_handler)

        t_bot, t_chat_ids = await self.get_bot_instance(bot_token)
        for chat_id in t_chat_ids:
            await t_bot.send_message(chat_id, f"🤖 Bot {bot_name} registered successfully!")

        exchange_name, exchange_type = RabbitExchange.SIGNALS.name, RabbitExchange.SIGNALS.exchange_type
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.on_strategy_signal,
            routing_key=bot_token,
            exchange_type=exchange_type)

        exchange_name, exchange_type = RabbitExchange.NOTIFICATIONS.name, RabbitExchange.NOTIFICATIONS.exchange_type
        await self.queue_service.register_listener(
            exchange_name=exchange_name,
            callback=self.on_notification,
            routing_key=bot_token,
            exchange_type=exchange_type)

    @exception_handler
    async def on_notification(self, routing_key: str, message: QueueMessage):
        self.logger.info(f"Received notification: {message}")

        t_bot, t_chat_ids = await self.get_bot_instance(routing_key)
        for chat_id in t_chat_ids:
            message_with_details = self.message_with_details(message.get("message"), message.sender, message.get("symbol"), message.get("timeframe"), message.get("direction"))
            await t_bot.send_message(chat_id, message_with_details)

    @exception_handler
    async def stop(self):
        self.logger.info(f"Middleware service {self.worker_id} stopped")
        await self.queue_service.stop()

        for topic, bots in self.telegram_bots:
            for bot in bots:
                self.logger.info(f"Stopping bot {bot.worker_id}")
                await bot.stop()


if __name__ == "__main__":
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')

    # Read command-line parameters
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Bot launcher script.')
    parser.add_argument('config_file', nargs='?', default='config.json', help='Path to the configuration file.')

    # Parse the command-line arguments
    args = parser.parse_args()

    config_file_param = args.config_file

    print(f"Config file: {config_file_param}")

    global_config = ConfigReader.load_config(config_file_param=config_file_param)

    executor = ThreadPoolExecutor(max_workers=5)
    loop = asyncio.new_event_loop()
    loop.set_default_executor(executor)
    asyncio.set_event_loop(loop)

    # Initialize the middleware
    middleware = MiddlewareService(global_config)

    loop.create_task(middleware.start())

    # Run all tasks asynchronously
    try:
        loop.run_forever()
    finally:
        loop.close()
