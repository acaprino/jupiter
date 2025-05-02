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

# Fallback global logger (same as before)
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

    Safely attempts to use instance attributes (`logger`, `agent`, `context`) for logging context
    only *after* an exception occurs. Falls back to a global logger if instance attributes
    are unavailable or if it's not a method call.

    Parameters:
    - func: The async function (coroutine) to wrap.

    Returns:
    - The wrapped async function with built-in exception handling.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # Determine if this is potentially a method call early
        instance = args[0] if args and isinstance(args[0], object) else None
        func_name = func.__name__ # Get function name outside try/except

        try:
            # Execute the original function
            return await func(*args, **kwargs)

        except Exception as original_exc:
            # --- Context Gathering Moved Inside Except Block ---
            chosen_logger: Optional[logging.Logger] = None
            agent_name = "UnknownAgent"
            context_info = "N/A"
            logger_label = "UnknownLogger"
            log_extra = {} # Dictionary for extra logging info

            try:
                # Try to get instance attributes *only if an exception occurred*
                if instance:
                    # Safely attempt to get the logger
                    potential_logger = getattr(instance, 'logger', None)
                    if potential_logger and hasattr(potential_logger, 'error') and hasattr(potential_logger, 'name'):
                        chosen_logger = potential_logger

                    # Safely attempt to get other context info
                    # Use default values if attributes don't exist
                    agent_name = getattr(instance, 'agent', instance.__class__.__name__)
                    context_info = getattr(instance, 'context', 'N/A')
                    logger_label = getattr(instance, 'logger_name', 'UnknownLogger')

                # Determine the final logger to use (instance or fallback)
                if not chosen_logger:
                    chosen_logger = global_fallback_logger
                    # Adjust names if using fallback
                    agent_name = agent_name if agent_name != "UnknownAgent" else func_name
                    context_info = context_info if context_info != "N/A" else "GlobalContext"
                    # Use the fallback logger's name if logger_label wasn't found on instance
                    logger_label = logger_label if logger_label != "UnknownLogger" else chosen_logger.name

                # --- Logging Logic ---
                frame = inspect.currentframe().f_back # Frame where exception occurred
                filename = os.path.basename(frame.f_code.co_filename) if frame else 'unknown_file'
                line_no = frame.f_lineno if frame else 'unknown_line'

                log_message = (
                    f"Exception in '{func_name}' [{filename}:{line_no}]: "
                    f"{type(original_exc).__name__} - {original_exc}"
                )

                # Prepare 'extra' dict based on whether we are using BotLogger or fallback
                if chosen_logger is not global_fallback_logger and instance:
                    # Format for BotLogger (assuming 's_' prefix)
                    log_extra = {
                        's_filename_lineno': f"{filename}:{line_no}",
                        's_funcName': func_name,
                        's_agent': agent_name,
                        's_logger': logger_label,
                        's_config': context_info
                    }
                    # Use the chosen_logger (which is the instance's logger here)
                    chosen_logger.error(log_message, exc_info=original_exc, extra=log_extra)
                else:
                    # Format for fallback logger (simpler message)
                    fallback_msg = f"{log_message} (Agent: {agent_name}, Context: {context_info})"
                    # Use chosen_logger (which is the fallback logger here)
                    chosen_logger.error(fallback_msg, exc_info=original_exc)

            except Exception as logging_error:
                # Critical failure during logging itself
                print(f"!!! CRITICAL ERROR IN EXCEPTION HANDLER LOGGING (during handling of {type(original_exc).__name__} in {func_name}) !!!")
                print(f"Original Exception: {original_exc}")
                traceback.print_exc() # Print traceback of original exception
                print(f"\nError during logging: {type(logging_error).__name__} - {logging_error}")
                traceback.print_exc() # Print traceback of logging exception

            # Re-raise the original exception regardless of logging success/failure
            raise

    return wrapper