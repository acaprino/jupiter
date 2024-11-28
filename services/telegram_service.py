import asyncio
import random
import threading
import logging
from aiohttp import ClientConnectionError
from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command

from misc_utils.bot_logger import BotLogger


class TelegramService:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, token, routine_label, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramService, cls).__new__(cls)
            return cls._instances[token]

    def __init__(self, token, routine_label, log_level="INFO"):
        if hasattr(self, '_initialized') and self._initialized:
            return

        self.token = token
        self.routine_label = routine_label
        self.bot = Bot(
            token=self.token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self._initialized = True
        self._is_running = False
        self.logger = BotLogger.get_logger(name=self.routine_label, level=log_level)

    async def start(self):
        if self._is_running:
            self.logger.info(f"Bot {self.routine_label} is already running.")
            return

        self._is_running = True
        self.logger.info(f"Bot {self.routine_label} started.")
        asyncio.create_task(self._polling())

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
                self.logger.exception("Unexpected error during polling:")
                break  # Exit the loop on unexpected exceptions
        self.logger.info("Polling stopped.")

    async def stop(self):
        if not self._is_running:
            self.logger.info(f"Bot {self.routine_label} is not running.")
            return

        self._is_running = False
        await self.dp.stop_polling()
        await self.bot.session.close()
        self.logger.info(f"Bot {self.routine_label} stopped.")

    async def send_message(self, chat_id, text, reply_markup=None):
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                await self.bot.send_message(chat_id, text, reply_markup=reply_markup)
                self.logger.info(f"Message sent to {chat_id}: {text}")
                return  # Exit after successful send
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.logger.warning(f"Rate limit exceeded when sending message to {chat_id}. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except TelegramServerError as e:
                self.logger.error(f"Server error when sending message to {chat_id}: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ClientConnectionError as e:
                self.logger.error(f"Connection error when sending message to {chat_id}: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.exception(f"Failed to send message to {chat_id}:")
                break  # Exit on unexpected exception
            retries += 1
        self.logger.error(f"Exceeded maximum retries to send message to {chat_id}.")

    def add_command_handler(self, command: str, handler):
        self.router.message.register(handler, Command(commands=[command]))
        self.logger.info(f"Added command handler for '{command}'.")

    def add_callback_query_handler(self, handler):
        self.router.callback_query.register(handler)
        self.logger.info("Added callback query handler.")
