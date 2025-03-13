import logging
import inspect
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler
from typing import Dict, Optional
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
                '%(asctime)s - %(levelname)s - %(s_logger)s - %(s_agent)s - %(s_filename_lineno)s - %(s_funcName)s - %(message)s'
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

    def _log(self, level: str, logger_name: str, agent: str, msg: str, exc_info: bool = False):
        """
        Internal helper to log messages with contextual information.

        Parameters:
        - level: Logging level as a string (e.g., 'debug', 'info').
        - msg: The log message.
        - exc_info: If True, includes exception information in the log.
        """
        frame = inspect.stack()[3]
        filename = os.path.basename(frame.filename)
        func_name = frame.function
        line_no = frame.lineno

        with self._file_lock:
            log_method = getattr(self.logger, level.lower(), self.info)
            log_method(msg, exc_info=exc_info, extra={
                's_filename_lineno': f"{filename}:{line_no}",
                's_funcName': func_name,
                's_agent': agent,
                's_logger': logger_name
            })

    def debug(self, msg: str, logger_name: str = "UnknownLogger", agent: str = "UnknownAgent"):
        self._log('debug', logger_name, agent, msg)

    def info(self, msg: str, logger_name: str = "UnknownLogger", agent: str = "UnknownAgent"):
        self._log('info', logger_name, agent, msg)

    def warning(self, msg: str, logger_name: str = "UnknownLogger", agent: str = "UnknownAgent"):
        self._log('warning', logger_name, agent, msg, exc_info=True)

    def error(self, msg: str, logger_name: str = "UnknownLogger", agent: str = "UnknownAgent"):
        self._log('error', logger_name, agent, msg, exc_info=True)

    def critical(self, msg: str, logger_name: str = "UnknownLogger", agent: str = "UnknownAgent"):
        self._log('critical', logger_name, agent, msg, exc_info=True)
