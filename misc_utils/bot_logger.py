import logging
import inspect
import os
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional


class BotLogger:
    """
    A Logger class implementing the Factory Pattern.
    Manages multiple logger instances based on unique names.
    """

    # Class-level dictionary to store logger instances
    _loggers: Dict[str, 'BotLogger'] = {}

    def __init__(self, name: str, level: Optional[str] = 'INFO'):
        """
        Initializes a Logger instance.

        Parameters:
        - name: Unique name for the logger.
        - log_file_path: File path for the log file.
        - level: Logging level as a string (e.g., 'DEBUG', 'INFO').
        """
        self.name = name
        self.log_file_path = f"logs/{self.name}.log"
        self.level = level.upper()
        self.logger = logging.getLogger(self.name)
        self._configure_logger()

    def _log(self, level: str, msg: str, exc_info: bool = False):
        """
        Internal helper to log messages with contextual information.

        Parameters:
        - level: Logging level as a string (e.g., 'debug', 'info').
        - msg: The log message.
        - exc_info: If True, includes exception information in the log.
        """
        # Retrieve the caller's frame information
        frame = inspect.stack()[2]
        filename = os.path.basename(frame.filename)
        func_name = frame.function
        line_no = frame.lineno

        # Get the logging method based on the level
        log_method = getattr(self.logger, level.lower(), self.logger.info)

        # Log the message with extra properties
        log_method(msg, exc_info=exc_info, extra={
            's_filename_lineno': f"{filename}:{line_no}",
            's_funcName': func_name
        })

    def _configure_logger(self):
        """Configures the logger with a rotating file handler and formatter."""
        if not self.logger.handlers:
            # Set logging level
            log_level_num = getattr(logging, self.level, logging.INFO)
            self.logger.setLevel(log_level_num)

            # Ensure the log directory exists
            log_directory = os.path.dirname(self.log_file_path)
            if log_directory and not os.path.exists(log_directory):
                os.makedirs(log_directory, exist_ok=True)  # Create directory if it doesn't exist

            # Create RotatingFileHandler
            handler = RotatingFileHandler(
                self.log_file_path,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=50,
                encoding='utf-8'
            )

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(s_filename_lineno)s - %(s_funcName)s - %(message)s')
            handler.setFormatter(formatter)

            # Add handler to the logger
            self.logger.addHandler(handler)
            self.logger.propagate = False  # Prevent log messages from being propagated to the root logger

    @classmethod
    def get_logger(cls, name: str, level: Optional[str] = "INFO") -> 'BotLogger':
        """
        Factory method to get a Logger instance.

        Parameters:
        - name: Unique name for the logger.
        - log_file_path: File path for the log file.
        - level: Logging level as a string (default is 'INFO').

        Returns:
        - Logger instance associated with the given name.
        """
        logger_key = name.lower()

        if logger_key not in cls._loggers:
            cls._loggers[logger_key] = cls(name, level)

        return cls._loggers[logger_key]

    def debug(self, msg: str):
        """Logs a message at DEBUG level."""
        self._log('debug', msg)

    def info(self, msg: str):
        """Logs a message at INFO level."""
        self._log('info', msg)

    def warning(self, msg: str):
        """Logs a message at WARNING level."""
        self._log('warning', msg)

    def error(self, msg: str):
        """Logs a message at ERROR level with exception information if available."""
        self._log('error', msg, exc_info=True)

    def critical(self, msg: str):
        """Logs a message at CRITICAL level."""
        self._log('critical', msg, exc_info=True)
