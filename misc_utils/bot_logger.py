import asyncio
import logging
import inspect
import os
from logging.handlers import RotatingFileHandler
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
        if not self.logger.handlers:
            log_level_num = getattr(logging, self.level, logging.INFO)
            self.logger.setLevel(log_level_num)

            log_directory = os.path.dirname(self.log_file_path)
            if log_directory and not os.path.exists(log_directory):
                os.makedirs(log_directory, exist_ok=True)

            handler = RotatingFileHandler(
                self.log_file_path,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=50,
                encoding='utf-8'
            )

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(s_agent)s - %(s_filename_lineno)s - %(s_funcName)s - %(message)s')
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
            if name.lower() not in cls._loggers:
                cls._loggers[name.lower()] = cls(name, level)
            return cls._loggers[name.lower()]

    def _log(self, level: str, agent: str, msg: str, exc_info: bool = False):
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
                's_agent': agent
            })

    def debug(self, msg: str, agent: str = "UnknownAgent"):
        self._log('debug', agent, msg)

    def info(self, msg: str, agent: str = "UnknownAgent"):
        self._log('info', agent, msg)

    def warning(self, msg: str, agent: str = "UnknownAgent"):
        self._log('warning', agent, msg)

    def error(self, msg: str, agent: str = "UnknownAgent"):
        self._log('error', agent, msg, exc_info=True)

    def critical(self, msg: str, agent: str = "UnknownAgent"):
        self._log('critical', agent, msg, exc_info=True)
