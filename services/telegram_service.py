import asyncio
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class TelegramService:
    _instances = {}
    _lock = asyncio.Lock()

    def __new__(cls, token, worker_id, *args, **kwargs):
        if token not in cls._instances:
            cls._instances[token] = super(TelegramService, cls).__new__(cls)
        return cls._instances[token]

    def __init__(self, token, worker_id):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.token = token
        self.worker_id = worker_id
        self.bot = Bot(token=self.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self._initialized = True
        self._is_running = False
        self.logger = BotLogger.get_logger(self.worker_id)

    @exception_handler
    async def start(self):
        if self._is_running:
            self.logger.info(f"Bot {self.worker_id} is already running.")
            return

        self._is_running = True
        asyncio.create_task(self.dp.start_polling(self.bot))

    @exception_handler
    async def stop(self):
        if not self._is_running:
            self.logger.info(f"Bot {self.worker_id} is not running.")
            return

        await self.dp.stop_polling()
        await self.bot.session.close()
        self._is_running = False

    @exception_handler
    async def send_message(self, chat_id, text, reply_markup=None):
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup
            )
            self.logger.info(f"Message sent to {chat_id}: {text}")
        except Exception as e:
            self.logger.error(f"Failed to send message to {chat_id}: {e}")

    def add_command_handler(self, command: str, handler):
        if not self._is_running:
            self.logger.info("Bot is not running. Cannot add command handler.")
            return

        self.router.message.register(handler, Command(commands=[command]))
        self.logger.info(f"Added command handler for '{command}'.")

    def add_callback_query_handler(self, handler):
        if not self._is_running:
            self.logger.info("Bot is not running. Cannot add callback query handler.")
            return

        self.router.callback_query.register(handler)
        self.logger.info("Added callback query handler.")
