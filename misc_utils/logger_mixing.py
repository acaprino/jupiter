from typing import Optional

from misc_utils import utils_functions
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration


class LoggingMixin:

    def __init__(self, config: ConfigReader, trading_config: Optional[TradingConfiguration] = None):
        self.logger = BotLogger.get_logger(name=config.get_bot_name(), level=config.get_bot_logging_level())
        self.logger_name = config.get_config_file()
        self.context_config = 'na'
        if trading_config:
            self.context_config = utils_functions.log_config_str(trading_config)

    def debug(self, msg: str, config: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        context_config = getattr(self, "context_config", self.__class__.__name__)
        self.logger.debug(msg=msg, logger_name=self.logger_name, agent=agent, config=config if config is not None else self.context_config)

    def info(self, msg: str, config: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.info(msg=msg, logger_name=self.logger_name, agent=agent, config=config if config is not None else self.context_config)

    def warning(self, msg: str, config: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.warning(msg=msg, logger_name=self.logger_name, agent=agent, config=config if config is not None else self.context_config)

    def error(self, msg: str, config: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.error(msg=msg, logger_name=self.logger_name, agent=agent, config=config if config is not None else self.context_config)

    def critical(self, msg: str, config: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.critical(msg=msg, logger_name=self.logger_name, agent=agent, config=config if config is not None else self.context_config)
