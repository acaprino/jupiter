import logging
import inspect
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler
from typing import Dict, Optional, Union, Any
import threading


class BotLogger:
    """
    A Logger class implementing the Factory Pattern, safe for asyncio contexts.
    Manages multiple logger instances based on unique names.
    """

    _loggers: Dict[str, 'BotLogger'] = {}
    _lock = threading.Lock()

    def __init__(self, name: str, level: Optional[str] = 'INFO'):
        """
        Initializes a Logger instance.

        Parameters:
        - name: Unique name for the logger.
        - level: Logging level as a string (e.g., 'DEBUG', 'INFO').
        """
        self.name = name
        self.log_file_path = f"logs/{self.name}.log"
        self.level = level.upper()
        self.logger = logging.getLogger(self.name)
        self._file_lock = threading.Lock()  # Lock for file handler access
        self._configure_logger()

    def _configure_logger(self):
        """Configures the logger with a rotating file handler and formatter."""
        log_level_num = getattr(logging, self.level, logging.INFO)
        self.logger.setLevel(log_level_num)

        # Create log directory if it doesn't exist
        log_directory = os.path.dirname(self.log_file_path)
        if log_directory and not os.path.exists(log_directory):
            os.makedirs(log_directory, exist_ok=True)

        # Create handler only if none exists
        if not self.logger.handlers:
            handler = ConcurrentRotatingFileHandler(
                self.log_file_path,
                maxBytes=50 * 1024 * 1024,  # 10 MB
                backupCount=50,
                encoding='utf-8'
            )
            # Set the handler level so it respects the logger's level
            handler.setLevel(log_level_num)

            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(s_logger)s - %(s_agent)s - %(s_config)s - %(s_filename_lineno)s - %(s_funcName)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.propagate = False

    @classmethod
    def get_logger(cls, name: str, level: Optional[str] = "INFO") -> 'BotLogger':
        """
        Factory method to get a Logger instance.

        Parameters:
        - name: Unique name for the logger.
        - level: Logging level as a string (default is 'INFO').

        Returns:
        - Logger instance associated with the given name.
        """
        with cls._lock:
            key = name.lower()
            if key not in cls._loggers:
                cls._loggers[key] = cls(name, level)
            else:
                # Update level if needed
                logger_instance = cls._loggers[key]
                new_level = level.upper()
                new_level_num = getattr(logging, new_level, logging.INFO)
                if logger_instance.logger.level != new_level_num:
                    logger_instance.logger.setLevel(new_level_num)
                    # Update level on all handlers
                    for handler in logger_instance.logger.handlers:
                        handler.setLevel(new_level_num)
                    logger_instance.level = new_level
            return cls._loggers[key]

    def _log(self, level: str, logger_name: str, agent: str, msg: str, config: str,
             exc_info: Union[bool, Any] = None, extra: Optional[Dict[str, Any]] = None):
        """
        Internal helper to log messages with contextual information.

        Parameters:
        - level: Logging level as a string (e.g., 'debug', 'info').
        - logger_name: Name of the logging component.
        - agent: Name of the agent involved.
        - msg: The log message.
        - config: Configuration identifier.
        - exc_info: If True, includes exception information in the log. If an
                     exception object, uses that. Defaults to None.
        - extra: Optional dictionary to override the default extra attributes
                 of the log record. If None (default), the standard extra
                 attributes ('s_filename_lineno', 's_funcName', 's_agent',
                 's_logger', 's_config') are generated based on the call stack
                 and other parameters.
        """
        final_extra: Dict[str, Any]

        if extra is not None:
            # Use the provided 'extra' dictionary directly
            final_extra = extra
        else:
            # Build the default 'extra' dictionary only if one wasn't provided
            frame = None  # Initialize frame to None
            try:
                # Stack level: [0]=_log, [1]=debug/info/etc, [2]=caller of debug/info/etc
                frame = inspect.stack()[2]
                filename = os.path.basename(frame.filename)
                func_name = frame.function
                line_no = frame.lineno
            except IndexError:
                # Handle cases where the stack is shallower than expected (e.g., called directly)
                filename = "unknown_file"
                func_name = "unknown_func"
                line_no = 0
            finally:
                # Ensure the frame object is released to avoid potential reference cycles
                if frame:
                    del frame

            final_extra = {
                's_filename_lineno': f"{filename}:{line_no}",
                's_funcName': func_name,
                's_agent': agent,
                's_logger': logger_name,
                's_config': config
            }

        # Use the lock to ensure thread safety if needed (e.g., for file handlers)
        with self._file_lock:
            try:
                # Get the appropriate logging method (debug, info, warning, etc.)
                log_method = getattr(self.logger, level.lower())
                # Call the logging method with the message, exception info, and extra context
                log_method(msg, exc_info=exc_info, extra=final_extra)
            except AttributeError:
                # Fallback if an invalid level string is provided
                self.logger.error(
                    f"Invalid log level '{level}' used for message: {msg}. Falling back to ERROR.",
                    extra=final_extra  # Still provide context if possible
                )
            except Exception as e:
                # Catch-all for unexpected errors during the logging process itself
                # Log this error using the standard logger error method, avoiding recursion
                self.logger.error(f"Error during logging process: {e}", exc_info=True)

    def debug(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent", extra: Optional[Dict[str, Any]] = None):
        self._log('debug', logger_name, agent, msg, config, extra=extra)

    def info(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent", extra: Optional[Dict[str, Any]] = None):
        self._log('info', logger_name, agent, msg, config, extra=extra)

    def warning(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent", extra: Optional[Dict[str, Any]] = None):
        self._log('warning', logger_name, agent, msg, config, extra=extra)

    def error(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent", exc_info: Union[bool, Any] = None, extra: Optional[Dict[str, Any]] = None):
        self._log('error', logger_name, agent, msg, config, exc_info=exc_info, extra=extra)

    def critical(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent", exc_info: Union[bool, Any] = None, extra: Optional[Dict[str, Any]] = None):
        self._log('critical', logger_name, agent, msg, config, exc_info=exc_info, extra=extra)
