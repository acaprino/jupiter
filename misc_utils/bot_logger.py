import logging
import inspect
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler
from typing import Dict, Optional, Union, Any
import threading

WRAPPER_FILES = {"logger_mixing.py", "bot_logger.py"}


def _find_caller(stack_offset: int = 2):
    """
    Walks up the call stack and returns (filename, func_name, line_no)
    of the first frame not in WRAPPER_FILES.

    stack_offset: how many f_back hops to skip before checking.
    """
    frame = inspect.currentframe()
    # skip this helper + the _log() frame(s)
    for _ in range(stack_offset):
        frame = frame.f_back if frame else None

    # climb until we exit wrapper files
    while frame and os.path.basename(frame.f_code.co_filename) in WRAPPER_FILES:
        frame = frame.f_back

    if not frame:
        # fallback: shallow stack
        return "unknown_file", "unknown_func", 0

    filename = os.path.basename(frame.f_code.co_filename)
    func_name = frame.f_code.co_name
    line_no = frame.f_lineno
    # break reference cycles
    del frame
    return filename, func_name, line_no


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

    def _log(
            self,
            level: str,
            logger_name: str,
            agent: str,
            msg: str,
            config: str,
            exc_info: Union[bool, Any] = None,
            extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Internal helper to log messages with contextual information.

        If `extra` is provided, it is used verbatim.
        Otherwise, we locate the real caller via _find_caller() and build:
          - s_filename_lineno
          - s_funcName
          - s_agent
          - s_logger
          - s_config
        """
        # 1) honor user‐provided extras
        if extra is not None:
            final_extra = extra
        else:
            # 2) discover real call site
            filename, func_name, line_no = _find_caller(stack_offset=2)
            final_extra = {
                's_filename_lineno': f"{filename}:{line_no}",
                's_funcName': func_name,
                's_agent': agent,
                's_logger': logger_name,
                's_config': config
            }

        # 3) perform the actual log write
        with self._file_lock:
            log_method = getattr(self.logger, level.lower(), None)
            if not callable(log_method):
                log_method = self.logger.info
            log_method(
                msg,
                exc_info=exc_info,
                extra=final_extra
            )

    def debug(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent",
              extra: Optional[Dict[str, Any]] = None):
        self._log('debug', logger_name, agent, msg, config, extra=extra)

    def info(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent",
             extra: Optional[Dict[str, Any]] = None):
        self._log('info', logger_name, agent, msg, config, extra=extra)

    def warning(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent",
                extra: Optional[Dict[str, Any]] = None):
        self._log('warning', logger_name, agent, msg, config, extra=extra)

    def error(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent",
              exc_info: Union[bool, Any] = None, extra: Optional[Dict[str, Any]] = None):
        self._log('error', logger_name, agent, msg, config, exc_info=exc_info, extra=extra)

    def critical(self, msg: str, config: str = "na", logger_name: str = "UnknownLogger", agent: str = "UnknownAgent",
                 exc_info: Union[bool, Any] = None, extra: Optional[Dict[str, Any]] = None):
        self._log('critical', logger_name, agent, msg, config, exc_info=exc_info, extra=extra)
