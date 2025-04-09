import traceback
from functools import wraps
from typing import Callable, Awaitable, TypeVar, Optional

R = TypeVar('R')


def exception_handler(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[Optional[R]]]:
    """
    Decorator to handle exceptions in asynchronous static or instance methods.
    If an exception occurs, it logs the error using `self.logger` if available,
    otherwise it falls back to a simple print statement.

    Parameters:
    - func: The asynchronous function to decorate.

    Returns:
    - The decorated function with exception handling.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs) -> Optional[R]:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # If `self` is provided, use its logger; otherwise, fallback to print
            instance = args[0] if args else None
            logger = getattr(instance, 'logger', None)

            # Prepare a detailed log message
            func_name = func.__name__
            error_msg = f"Exception caught in '{func_name}'"
            if instance and hasattr(instance, 'agent'):
                error_msg += f" (Agent: {instance.agent})"
            if instance and hasattr(instance, 'context'):
                error_msg += f" (Context: {instance.context})"
            error_msg += f": {type(e).__name__} - {e}"

            if logger and callable(getattr(logger, 'error', None)):
                logger.error(f"Exception in {func.__name__}: {e}", exec_info=e)
            else:
                # Fallback if an appropriate logger does not exist
                print(f"Fallback Log: {error_msg}")
                traceback.print_exc()
            raise

    return wrapper
