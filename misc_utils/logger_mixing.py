from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader


class LoggingMixin:

    def __init__(self, config: ConfigReader):
        self.logger = BotLogger.get_logger(name=config.get_bot_name(), level=config.get_bot_logging_level())

    def debug(self, msg: str, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.debug(msg, agent)

    def info(self, msg: str, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.info(msg, agent)

    def warning(self, msg: str, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.warning(msg, agent)

    def error(self, msg: str, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.error(msg, agent)

    def critical(self, msg: str, **kwargs):
        agent = getattr(self, "agent", self.__class__.__name__)
        self.logger.critical(msg, agent)
