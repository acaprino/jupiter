import asyncio

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
    async def enqueue(self, method, agent, *args, **kwargs):
        await self.queue.put((method, agent, args, kwargs))

    async def _process_queue(self):
        while True:
            method, agent, args, kwargs = await self.queue.get()
            try:
                await self._execute_api_call(method, agent, *args, **kwargs)
            except Exception as e:
                BotLogger.get_logger(agent).critical(f"Error processing API call in _process_queue: {e}")
            finally:
                self.queue.task_done()

    @exception_handler
    async def _execute_api_call(self, method, agent, *args, **kwargs):
        max_retries = 5
        retries = 0
        logger = BotLogger.get_logger(agent)
        while retries < max_retries:
            try:
                await method(*args, **kwargs)
                return
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                logger.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except (TelegramServerError, ClientConnectionError) as e:
                logger.error(f"Temporary error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.critical("Unexpected error during API call:")
                raise  # Re-raise the exception to be caught in _process_queue
            retries += 1
        logger.error("Exceeded maximum retries for API call.")

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
