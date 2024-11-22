# decorators.py

from functools import wraps
from typing import Callable, Awaitable, TypeVar, Optional

R = TypeVar('R')


def exception_handler(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[Optional[R]]]:
    """
    Decorator to handle exceptions in asynchronous class methods.
    If an exception occurs, it logs the error using `self.logger` and returns None.

    Parameters:
    - func: The asynchronous function to decorate.

    Returns:
    - The decorated function with exception handling.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Optional[R]:
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            # Check if the instance has a 'logger' attribute with a 'log_error' method
            logger = getattr(self, 'logger', None)
            if logger and callable(getattr(logger, 'error', None)):
                logger.error(f"Exception in {func.__name__}: {e}")
            else:
                # Fallback if an appropriate logger does not exist
                print(f"Exception in {func.__name__}: {e}")
            return None  # Returns None instead of raising the exception

    return wrapper