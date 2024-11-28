import asyncio
import traceback

from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiohttp import ClientConnectionError

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class TelegramAPIManager:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelegramAPIManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.queue = None
        self.worker_task = None
        self._initialized = False

    @exception_handler
    async def initialize(self):
        if self._initialized:
            return
        self.queue = asyncio.Queue()
        self.worker_task = asyncio.create_task(self._process_queue())
        self._initialized = True

    @exception_handler
    async def enqueue(self, method, routine_label, *args, **kwargs):
        await self.queue.put((method, routine_label, args, kwargs))

    @exception_handler
    async def _process_queue(self):
        while True:
            method, routine_label, args, kwargs = await self.queue.get()
            await self._execute_api_call(method, routine_label, *args, **kwargs)
            self.queue.task_done()

    @exception_handler
    async def _execute_api_call(self, method, routine_label, *args, **kwargs):
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                await method(*args, **kwargs)
                return
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                BotLogger.get_logger(routine_label).warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except TelegramServerError as e:
                BotLogger.get_logger(routine_label).error(f"Server error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ClientConnectionError as e:
                BotLogger.get_logger(routine_label).error(f"Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                BotLogger.get_logger(routine_label).critical(f"Unexpected error during API call: {e}")
                break
            retries += 1
        if retries >= max_retries:
            BotLogger.get_logger(routine_label).error("Exceeded maximum retries for API call.")

    @exception_handler
    async def shutdown(self):
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            self._initialized = False
            self.queue = None
