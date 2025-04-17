import functools
import inspect
import logging
import os
import traceback
from typing import Callable, TypeVar, Coroutine, Any, Optional, ParamSpec

# Type variable for return type of the decorated function
R = TypeVar('R')
# ParamSpec to capture the parameter types of the original function
P = ParamSpec('P')

# Fallback global logger in case a BotLogger is unavailable or not desired
global_fallback_logger = logging.getLogger("fallback_exception_handler")
if not global_fallback_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    global_fallback_logger.addHandler(handler)
    global_fallback_logger.setLevel(logging.ERROR)


def exception_handler(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]:
    """
    Decorator for handling exceptions in async functions: logs the exception and re-raises it.

    It attempts to use `instance.logger` if the decorated function is a method
    and the instance has a compatible logger (e.g., BotLogger). Otherwise,
    it falls back to a standard logger.

    Parameters:
    - func: The async function (coroutine) to wrap.

    Returns:
    - The wrapped async function with built-in exception handling.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # Determine if this was called as a method: first arg is the instance
        instance = args[0] if args and isinstance(args[0], object) else None
        chosen_logger: Optional[logging.Logger] = None
        extra_info: dict = {}

        func_name = func.__name__
        logger_label = "UnknownLogger"
        agent_name = "UnknownAgent"
        context_info = "N/A"

        # Try to extract a logger and context from the instance (if any)
        if instance:
            potential_logger = getattr(instance, 'logger', None)
            # Check if logger has an 'error' method and a 'name' attribute
            if potential_logger and hasattr(potential_logger, 'error') and hasattr(potential_logger, 'name'):
                chosen_logger = potential_logger
            agent_name = getattr(instance, 'agent', instance.__class__.__name__)
            context_info = getattr(instance, 'context', 'N/A')
            logger_label = getattr(instance, 'logger_name', 'UnknownLogger')

        # Use fallback logger if none found on instance
        if not chosen_logger:
            chosen_logger = global_fallback_logger
            agent_name = agent_name if agent_name != "UnknownAgent" else func_name
            context_info = context_info if context_info != "N/A" else "GlobalContext"

        try:
            # Execute the original function
            return await func(*args, **kwargs)

        except Exception as original_exc:
            # Gather caller info for precise logging
            try:
                frame = inspect.currentframe().f_back
                filename = os.path.basename(frame.f_code.co_filename) if frame else 'unknown_file'
                line_no = frame.f_lineno if frame else 'unknown_line'

                log_message = (
                    f"Exception in '{func_name}' [{filename}:{line_no}]: "
                    f"{type(original_exc).__name__} - {original_exc}"
                )

                # If using a specialized logger, include structured extra info
                if chosen_logger is not global_fallback_logger:
                    extra_info = {
                        'filename_lineno': f"{filename}:{line_no}",
                        'function': func_name,
                        'agent': agent_name,
                        'logger': logger_label,
                        'context': context_info
                    }
                    chosen_logger.error(log_message, exc_info=original_exc, extra=extra_info)
                else:
                    # Standard fallback logging
                    global_fallback_logger.error(
                        f"{log_message} (Agent: {agent_name}, Context: {context_info})",
                        exc_info=original_exc
                    )

            except Exception as logging_error:
                # Critical failure during logging itself
                print("!!! CRITICAL ERROR IN EXCEPTION HANDLER LOGGING !!!")
                print(f"Original Exception in {func_name}: {type(original_exc).__name__} - {original_exc}")
                traceback.print_exc()
                print(f"Error while logging: {type(logging_error).__name__} - {logging_error}")
                traceback.print_exc()

            # Re-raise the original exception
            raise

    return wrapper
