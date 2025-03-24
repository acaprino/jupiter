import asyncio
import random
import threading

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiohttp import ClientConnectionError

from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from services.api_telegram import TelegramAPIManager

class TelegramService(LoggingMixin):
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, config: ConfigReader, token, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramService, cls).__new__(cls)
            return cls._instances[token]

    def __init__(self, config: ConfigReader, token):
        if hasattr(self, '_initialized') and self._initialized:
            return
        super().__init__(config)
        self.agent = "TelegramService"
        self.config = config

        self.token = token
        self.bot = Bot(
            token=self.token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self._initialized = True
        self._is_running = False

        # Initialize the global API manager
        self.api_manager = TelegramAPIManager(self.config)

    @exception_handler
    async def start(self):
        if self._is_running:
            self.info(f"Bot {self.agent} is already running.")
            return

        self._is_running = True
        self.info(f"Bot {self.agent} started.")

        # Initialize the global API manager if not already initialized
        await self.api_manager.initialize()

        # Start polling
        asyncio.create_task(self._polling())

    @exception_handler
    async def stop(self):
        if not self._is_running:
            self.info(f"Bot {self.agent} is not running.")
            return

        self._is_running = False

        await self.dp.stop_polling()
        await self.bot.session.close()

        # Optionally, shutdown the global API manager if needed
        # await self.api_manager.shutdown()

        self.info(f"Bot {self.agent} stopped.")

    @exception_handler
    async def _polling(self):
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Prevent simultaneous polling
        while self._is_running:
            try:
                await self.dp.start_polling(self.bot)
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except TelegramServerError as e:
                self.error(f"Server error. Retrying in 5 seconds...", exec_info=e)
                await asyncio.sleep(5)
            except ClientConnectionError as e:
                self.error(f"Connection error. Retrying in 5 seconds...", exec_info=e)
                await asyncio.sleep(5)
            except Exception as e:
                self.critical(f"Unexpected error during polling", exec_info=e)
                break  # Exit the loop on unexpected exceptions
        self.info("Polling stopped.")

    @exception_handler
    async def send_message(self, chat_id, text, reply_markup=None):
        """
        Enqueues the send_message API call to the global API manager.
        """
        text_log = text.replace("\n", " \\n ")
        self.info(f"Sending message to chat {chat_id}: {text_log}")
        await self.api_manager.enqueue(
            self.bot.send_message,
            self.agent,
            chat_id,
            text,
            reply_markup=reply_markup
        )

    def add_command_handler(self, command: str, handler):
        self.router.message.register(handler, Command(commands=[command]))
        self.info(f"Added command handler for '{command}'.")

    def add_callback_query_handler(self, handler):
        self.router.callback_query.register(handler)
        self.info("Added callback query handler.")
