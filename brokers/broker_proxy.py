import asyncio
import contextvars
import threading
from typing import TypeVar, Generic, Optional, Type, Dict

from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler

T = TypeVar('T')

# Context variable to store a configuration string in a thread and asyncio safe manner.
config_string_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("config_string", default=None)


class Broker(Generic[T]):
    """
    A singleton broker class providing thread-safe and asyncio-safe access to an underlying broker instance.

    This class implements a lazy-instantiated singleton pattern using double-check locking with a threading.Lock
    to ensure that only one instance is created. It further ensures asynchronous safety by employing an asyncio.Lock
    to serialize method calls on the underlying broker instance.

    Key Features:
      - **Singleton Pattern**: Guarantees a single broker instance throughout the application.
      - **Thread Safety**: Uses a threading.Lock during instantiation to avoid race conditions in multi-threaded contexts.
      - **Asyncio Safety**: Wraps broker method calls with an asyncio.Lock to prevent concurrent execution in async tasks.
      - **Context Propagation**: Leverages contextvars.ContextVar to safely pass a context-specific configuration string
        (or similar data) across threads and async tasks without interference.

    Usage:
      1. Asynchronously call `initialize()` with the broker implementation class, configuration, and connection
         parameters to instantiate the underlying broker instance.
      2. Optionally, use `with_context()` to set a context (e.g., a configuration string) that will be injected into
         subsequent method calls.
      3. Access broker methods directly on the singleton instance. Method calls are proxied to the underlying broker,
         ensuring proper use of the async lock and context propagation.

    Note:
      - Ensure that `initialize()` is called before invoking any broker methods, as uninitialized access will raise an exception.
    """

    _instance: Optional['Broker'] = None
    _lock = threading.Lock()

    def __init__(self):
        """
        Initialize the Broker instance.

        The __init__ method sets the initial state for the Broker, ensuring that the underlying broker instance is None.
        This method is typically called only once due to the singleton pattern.
        """
        self._broker_instance = None

    def __new__(cls) -> 'Broker':
        """
        Create and return the singleton instance of the Broker.

        This method employs a double-checked locking mechanism with a threading.Lock to ensure that only one instance
        of Broker is created even in a multi-threaded environment.

        Returns:
            Broker: The singleton instance of the Broker.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._broker_instance = None
                    cls._instance.async_lock = asyncio.Lock()
                    cls._instance.logger = None  # Direct logger field to avoid __getattr__ lock recursion
        return cls._instance

    @exception_handler
    async def initialize(self, broker_class: Type[T], config: ConfigReader, connection: Dict) -> 'Broker':
        """
        Asynchronously initialize the underlying broker instance.

        This method instantiates the broker implementation using the provided broker_class, config, and connection
        parameters. It also configures a logger based on the configuration. If the broker has already been
        initialized, an exception is raised.

        Args:
            broker_class (Type[T]): The class of the broker implementation to instantiate.
            config (ConfigReader): A configuration reader containing necessary settings.
            connection (Dict): A dictionary of connection parameters.

        Returns:
            Broker: The current Broker instance with the underlying broker initialized.

        Raises:
            Exception: If the broker is already initialized or if instantiation fails.
        """
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

    def with_context(self, config_string: str) -> 'Broker':
        """
        Set a context-specific configuration string for subsequent broker method calls.

        This method stores the given configuration string in a context variable, ensuring that it is accessible
        in the current asynchronous context without interfering with other threads or tasks.

        Args:
            config_string (str): The configuration string to be set in the context.

        Returns:
            Broker: The current Broker instance.
        """
        config_string_ctx.set(config_string)
        return self

    @property
    def is_initialized(self) -> bool:
        """
        Check whether the underlying broker instance has been initialized.

        Returns:
            bool: True if the broker instance is initialized, False otherwise.
        """
        return self._broker_instance is not None

    def __getattr__(self, name):
        """
        Proxy attribute access to the underlying broker instance.

        If the accessed attribute is callable, this method returns an asynchronous proxy function that:
          - Acquires the asynchronous lock to ensure exclusive access.
          - Injects the current context (from contextvars) into the underlying broker.
          - Executes the actual method and resets the context afterward.

        Args:
            name (str): The name of the attribute to access.

        Returns:
            Any: The proxied attribute or method of the underlying broker instance.

        Raises:
            Exception: If the broker instance is not initialized.
        """
        if not self.is_initialized:
            raise Exception("Broker not initialized. Call initialize() first")

        attr = getattr(self._broker_instance, name)

        if callable(attr):
            async def proxy_wrapper(*args, **kwargs):
                async with self.async_lock:
                    # Set the context for the broker instance from the current context variable.
                    self._broker_instance.context = config_string_ctx.get()
                    try:
                        ret = await attr(*args, **kwargs)
                    finally:
                        # Reset the context after the method call.
                        config_string_ctx.set(None)
                    return ret
            return proxy_wrapper
        return attr
