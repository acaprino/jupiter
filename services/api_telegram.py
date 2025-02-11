import asyncio
import threading

from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiohttp import ClientConnectionError
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin


class TelegramAPIManager(LoggingMixin):
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls, config: ConfigReader):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TelegramAPIManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, config: ConfigReader):
        if TelegramAPIManager._initialized:
            return
        super().__init__(config)
        self.config = config
        self.agent = "TelegramAPI"
        self.queue = None
        self.worker_task = None
        TelegramAPIManager._initialized = True

    @exception_handler
    async def initialize(self):
        self.info("Called initialize function")
        if hasattr(self, '_initialized') and self.queue is not None:
            self.info("Already initialized. Skipping initialization.")
            return

        self.info("Initializing...")
        self.queue = asyncio.Queue()
        self.worker_task = asyncio.create_task(self._process_queue())
        self.info("Initialization complete.")

    @exception_handler
    async def enqueue(self, method, agent, *args, **kwargs):
        await self.queue.put((method, agent, args, kwargs))

    async def _process_queue(self):
        self.info("_process_queue started")
        while True:
            method, agent, args, kwargs = await self.queue.get()
            try:
                self.info(f"Processing {method}, {agent}, {args}, {kwargs}")
                await self._execute_api_call(method, agent, *args, **kwargs)
            except asyncio.CancelledError:
                self.debug("_process_queue cancelled")
                raise
            except Exception as e:
                self.critical(f"Error processing API call in _process_queue: {e}")
            finally:
                self.info(f"Task processed {method}, {agent}, {args}, {kwargs}")
                self.queue.task_done()

    @exception_handler
    async def _execute_api_call(self, method, agent, *args, **kwargs):
        max_retries = 5
        retries = 0
        self.info(f"Executing API call for agent {agent}")
        while retries < max_retries:
            try:
                await method(*args, **kwargs)
                return
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except (TelegramServerError, ClientConnectionError) as e:
                self.error(f"Temporary error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.critical(f"Unexpected error during API call: {e}")
                raise  # Re-raise the exception to be caught in _process_queue
            retries += 1
        self.error("Exceeded maximum retries for API call.")

    @exception_handler
    async def shutdown(self):
        self.info("Shutting down TelegramAPIManager.")
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
                self.debug("Worker task successfully cancelled.")
            except asyncio.CancelledError:
                self.debug("Worker task cancellation confirmed.")
            except Exception as e:
                self.critical(f"Error during worker task cancellation: {e}")
        else:
            self.debug("No worker task to cancel.")
