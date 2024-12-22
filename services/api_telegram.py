import asyncio
import threading

from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiohttp import ClientConnectionError
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class TelegramAPIManager:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TelegramAPIManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if TelegramAPIManager._initialized:
            return
        self.queue = None
        self.worker_task = None
        self.logger = BotLogger.get_logger(name="TelegramAPIManager")
        TelegramAPIManager._initialized = True
        self.logger.info("TelegramAPIManager instance created.")

    @exception_handler
    async def initialize(self):
        self.logger.info("Called initialize function")
        if hasattr(self, '_initialized') and self.queue is not None:
            self.logger.info("Already initialized. Skipping initialization.")
            return

        self.logger.info("Initializing...")
        self.queue = asyncio.Queue()
        self.worker_task = asyncio.create_task(self._process_queue())
        self.logger.info("Initialization complete.")

    @exception_handler
    async def enqueue(self, method, agent, *args, **kwargs):
        await self.queue.put((method, agent, args, kwargs))

    async def _process_queue(self):
        self.logger.info("_process_queue started")
        while True:
            method, agent, args, kwargs = await self.queue.get()
            try:
                self.logger.info(f"Processing {method}, {agent}, {args}, {kwargs}")
                await self._execute_api_call(method, agent, *args, **kwargs)
            except asyncio.CancelledError:
                self.logger.debug("_process_queue cancelled")
                raise
            except Exception as e:
                BotLogger.get_logger(agent).critical(f"Error processing API call in _process_queue: {e}")
            finally:
                self.logger.info(f"Task processed {method}, {agent}, {args}, {kwargs}")
                self.queue.task_done()

    @exception_handler
    async def _execute_api_call(self, method, agent, *args, **kwargs):
        max_retries = 5
        retries = 0
        self.logger.warning(f"Executing API call for agent {agent}")
        while retries < max_retries:
            try:
                await method(*args, **kwargs)
                return
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.logger.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except (TelegramServerError, ClientConnectionError) as e:
                self.logger.error(f"Temporary error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.critical("Unexpected error during API call:")
                raise  # Re-raise the exception to be caught in _process_queue
            retries += 1
        self.logger.error("Exceeded maximum retries for API call.")

    @exception_handler
    async def shutdown(self):
        self.logger.info("Shutting down TelegramAPIManager.")
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
                self.logger.debug("Worker task successfully cancelled.")
            except asyncio.CancelledError:
                self.logger.debug("Worker task cancellation confirmed.")
            except Exception as e:
                self.logger.critical(f"Error during worker task cancellation: {e}")
        else:
            self.logger.debug("No worker task to cancel.")
