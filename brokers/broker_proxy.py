import asyncio
import contextvars
import threading
from typing import TypeVar, Generic, Optional, Type, Dict

from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler

T = TypeVar('T')

config_string_ctx = contextvars.ContextVar("config_string", default=None)


class Broker(Generic[T]):
    _instance: Optional['Broker'] = None
    _lock = threading.Lock()

    def __new__(cls) -> 'Broker':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._broker_instance = None
                    cls._instance.async_lock = asyncio.Lock()
                    cls._instance.logger = None  # Create direct logger field to avoid __getattr__ lock recursion
        return cls._instance

    @exception_handler
    async def initialize(self, broker_class: Type[T], config: ConfigReader, connection: Dict) -> 'Broker':
        if self._broker_instance is not None:
            raise Exception("Broker is already initialized")

        logger = BotLogger.get_logger(name=config.get_bot_name(), level=config.get_bot_logging_level())
        async with self.async_lock:
            try:
                self._broker_instance = broker_class(config, connection)
                return self
            except Exception as e:
                logger.error(f"Error while instantiating broker implementation {broker_class}: {e}")
                raise e

    def with_config(self, config_string: str) -> 'Broker':
        config_string_ctx.set(config_string)
        return self

    @property
    def is_initialized(self) -> bool:
        return self._broker_instance is not None

    def __getattr__(self, name):
        if not self.is_initialized:
            raise Exception("Broker not initialized. Call initialize() first")

        attr = getattr(self._broker_instance, name)

        if callable(attr):
            async def proxy_wrapper(*args, **kwargs):
                async with self.async_lock:
                    # kwargs["config"] = config_string_ctx.get()
                    self._broker_instance.context_config = config_string_ctx.get()
                    try:
                        ret = await attr(*args, **kwargs)
                    finally:
                        config_string_ctx.set(None)
                    return ret
            return proxy_wrapper
        return attr
