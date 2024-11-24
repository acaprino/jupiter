import asyncio
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class TelegramService:
    _instances = {}
    _lock = asyncio.Lock()

    def __new__(cls, token, routine_label, *args, **kwargs):
        if token not in cls._instances:
            cls._instances[token] = super(TelegramService, cls).__new__(cls)
        return cls._instances[token]

    def __init__(self, token, routine_label):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.token = token
        self.routine_label = routine_label
        self.bot = Bot(token=self.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self._initialized = True
        self._is_running = False
        self.logger = BotLogger.get_logger(self.routine_label)
        self.max_requests_per_minute = 60
        self.time_window = 60
        self.semaphore = asyncio.Semaphore(self.max_requests_per_minute)

    @exception_handler
    async def start(self):
        if self._is_running:
            self.logger.info(f"Bot {self.routine_label} is already running.")
            return

        self._is_running = True
        # asyncio.create_task(self.dp.start_polling(self.bot))
        asyncio.create_task(self._rate_limited_polling())
        self.logger.info(f"Bot {self.routine_label} started.")

    async def _rate_limited_polling(self):
        while self._is_running:
            async with self.semaphore:
                try:
                    await self.dp.start_polling(self.bot, timeout=30)
                except TelegramRetryAfter as e:
                    self.logger.warning(f"Rate limit exceeded. Retrying after {e.retry_after} seconds...")
                    await asyncio.sleep(e.retry_after)
                except TelegramServerError as e:
                    self.logger.error(f"Server error: {e}. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    await asyncio.sleep(5)
            await asyncio.sleep(self.time_window / self.max_requests_per_minute)

    @exception_handler
    async def stop(self):
        if not self._is_running:
            self.logger.info(f"Bot {self.routine_label} is not running.")
            return

        await self.dp.stop_polling()
        await self.bot.session.close()
        self._is_running = False
        self.logger.info(f"Bot {self.routine_label} stopped.")

    @exception_handler
    async def send_message(self, chat_id, text, reply_markup=None):
        async with self.semaphore:
            try:
                await self.bot.send_message(chat_id, text, reply_markup=reply_markup)
                self.logger.info(f"Message sent to {chat_id}: {text}")
            except TelegramRetryAfter as e:
                self.logger.warning(f"Rate limited. Retry after {e.retry_after} seconds.")
                await asyncio.sleep(e.retry_after)
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
