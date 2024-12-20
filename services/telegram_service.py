import asyncio
import random
import threading

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiohttp import ClientConnectionError

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from services.telegram_api_manager import TelegramAPIManager


class TelegramService:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, token, agent, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramService, cls).__new__(cls)
            return cls._instances[token]

    def __init__(self, token, agent, logging_level="INFO"):
        if hasattr(self, '_initialized') and self._initialized:
            return

        self.token = token
        self.agent = agent
        self.bot = Bot(
            token=self.token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self._initialized = True
        self._is_running = False
        self.logger = BotLogger(self.agent, logging_level)

        # Initialize the global API manager
        self.api_manager = TelegramAPIManager()

    @exception_handler
    async def start(self):
        if self._is_running:
            self.logger.info(f"Bot {self.agent} is already running.")
            return

        self._is_running = True
        self.logger.info(f"Bot {self.agent} started.")

        # Initialize the global API manager if not already initialized
        await self.api_manager.initialize()

        # Start polling
        asyncio.create_task(self._polling())

    @exception_handler
    async def stop(self):
        if not self._is_running:
            self.logger.info(f"Bot {self.agent} is not running.")
            return

        self._is_running = False

        await self.dp.stop_polling()
        await self.bot.session.close()

        # Optionally, shutdown the global API manager if needed
        # await self.api_manager.shutdown()

        self.logger.info(f"Bot {self.agent} stopped.")

    @exception_handler
    async def _polling(self):
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Prevent simultaneous polling
        while self._is_running:
            try:
                await self.dp.start_polling(self.bot)
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.logger.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except TelegramServerError as e:
                self.logger.error(f"Server error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ClientConnectionError as e:
                self.logger.error(f"Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.critical(f"Unexpected error during polling: {e}")
                break  # Exit the loop on unexpected exceptions
        self.logger.info("Polling stopped.")

    @exception_handler
    async def send_message(self, chat_id, text, reply_markup=None):
        """
        Enqueues the send_message API call to the global API manager.
        """
        self.logger.info(f"Sending message to chat {chat_id}: {text}")
        await self.api_manager.enqueue(
            self.bot.send_message,
            self.agent,
            chat_id,
            text,
            reply_markup=reply_markup
        )

    def add_command_handler(self, command: str, handler):
        self.router.message.register(handler, Command(commands=[command]))
        self.logger.info(f"Added command handler for '{command}'.")

    def add_callback_query_handler(self, handler):
        self.router.callback_query.register(handler)
        self.logger.info("Added callback query handler.")
