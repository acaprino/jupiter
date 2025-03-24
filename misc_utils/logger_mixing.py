import os
from typing import Optional

from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration


class LoggingMixin:

    def __init__(self, config: ConfigReader):
        self.logger = BotLogger.get_logger(name=config.get_bot_name(), level=config.get_bot_logging_level())
        self.logger_name = os.path.basename(config.get_config_file())
        self.context = '*'

    def debug(self, msg: str, context_param: Optional[str] = None, **kwargs):
        self.logger.debug(msg=msg, logger_name=self.logger_name, agent=self.get_agent(), config=self.get_context(context_param))

    def info(self, msg: str, context_param: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.info(msg=msg, logger_name=self.logger_name, agent=self.get_agent(), config=self.get_context(context_param))

    def warning(self, msg: str, context_param: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.warning(msg=msg, logger_name=self.logger_name, agent=self.get_agent(), config=self.get_context(context_param))

    def error(self, msg: str, context_param: Optional[str] = None, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.error(msg=msg, logger_name=self.logger_name, agent=self.get_agent(), config=self.get_context(context_param))

    def critical(self, msg: str, context_param: Optional[str] = None, **kwargs):
        self.logger.critical(msg=msg, logger_name=self.logger_name, agent=self.get_agent(), config=self.get_context(context_param))

    def get_context(self, context_param: Optional[str]):
        context = getattr(self, "context", self.__class__.__name__)
        if context_param is not None: return context_param
        if context is not None: return context
        return "*"

    def get_agent(self):
        return getattr(self, "agent", self.__class__.__name__)
