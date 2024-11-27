import asyncio
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
import random
import threading
from aiohttp import ClientConnectionError

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class TelegramService:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, token, routine_label, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramService, cls).__new__(cls)
            return cls._instances[token]

    def __init__(self, token, routine_label, max_requests_per_minute=60, time_window=60):
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
        self.max_requests_per_minute = max_requests_per_minute
        self.time_window = time_window
        self.semaphore = asyncio.Semaphore(self.max_requests_per_minute)

    @exception_handler
    async def start(self):
        if self._is_running:
            self.logger.info(f"Bot {self.routine_label} is already running.")
            return

        self._is_running = True
        self.logger.info(f"Bot {self.routine_label} started.")
        await self._rate_limited_polling()

    async def _rate_limited_polling(self):
        """
        Starts the polling process. Should not be inside a loop.
        """
        await asyncio.sleep(random.uniform(0.5, 2.0))
        try:
            await self.dp.start_polling(self.bot, timeout=30, limit=10)
        except TelegramRetryAfter as e:
            self.logger.warning(f"Rate limit exceeded. Retrying after {e.retry_after} seconds...")
            await asyncio.sleep(e.retry_after)
            await self._rate_limited_polling()
        except TelegramServerError as e:
            self.logger.error(f"Server error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            await self._rate_limited_polling()
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")

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
        """
        Sends a message to a specified chat, adhering to rate limits.
        """
        async with self.semaphore:
            try:
                await self.bot.send_message(chat_id, text, reply_markup=reply_markup)
                self.logger.info(f"Message sent to {chat_id}: {text}")
            except TelegramRetryAfter as e:
                self.logger.warning(f"Rate limited. Retry after {e.retry_after} seconds.")
                await asyncio.sleep(e.retry_after)
            except TelegramServerError as e:
                self.logger.error(f"Server error while sending message to {chat_id}: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)
            except ClientConnectionError as e:
                self.logger.error(f"Connection error while sending message to {chat_id}: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Failed to send message to {chat_id}: {e}")

    def add_command_handler(self, command: str, handler):
        """
        Adds a command handler to the bot if the bot is running.
        """
        self.router.message.register(handler, Command(commands=[command]))
        self.logger.info(f"Added command handler for '{command}'.")

        if not self._is_running:
            self.logger.info("Bot is not running. Handler will be activated when bot starts.")

    def add_callback_query_handler(self, handler):
        """
        Adds a callback query handler to the bot if the bot is running.
        """
        self.router.callback_query.register(handler)
        self.logger.info("Added callback query handler.")

        if not self._is_running:
            self.logger.info("Bot is not running. Handler will be activated when bot starts.")
