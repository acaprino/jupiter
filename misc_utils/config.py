import json
import threading
from typing import Dict, Any, List, Optional

from misc_utils.enums import TradingDirection, Timeframe, Mode
from misc_utils.utils_functions import string_to_enum


class TelegramConfiguration:
    """
    Represents a Telegram configuration for a trading configuration.
    """

    def __init__(self, token: str, chat_ids: List[str]):
        self.token = token
        self.chat_ids = chat_ids

    # Accessors
    def get_token(self) -> str:
        return self.token

    def get_chat_ids(self) -> List[str]:
        return self.chat_ids

    # Mutators
    def set_token(self, token: str):
        self.token = token

    def set_chat_ids(self, chat_ids: List[str]):
        self.chat_ids = chat_ids


class TradingConfiguration:
    """
    Represents an individual trading configuration.
    """

    def __init__(self, bot_name: str, symbol: str, timeframe: Timeframe, trading_direction: TradingDirection, risk_percent: float, telegram_config: TelegramConfiguration):
        self.symbol = symbol
        self.timeframe = timeframe
        self.trading_direction = trading_direction
        self.risk_percent = risk_percent
        self.telegram_config = telegram_config
        self.bot_name = bot_name

    def __repr__(self):
        return (f"TradingConfiguration(symbol={self.symbol}, timeframe={self.timeframe.name}, "
                f"trading_direction={self.trading_direction.name}, risk_percent={self.risk_percent}, "
                f"telegram_config={self.telegram_config})")

    # Accessors
    def get_symbol(self) -> str:
        return self.symbol

    def get_timeframe(self) -> Timeframe:
        return self.timeframe

    def get_trading_direction(self) -> TradingDirection:
        return self.trading_direction

    def get_risk_percent(self) -> float:
        return self.risk_percent

    def get_telegram_config(self) -> TelegramConfiguration:
        return self.telegram_config

    # Mutators
    def set_symbol(self, symbol: str):
        self.symbol = symbol

    def set_timeframe(self, timeframe: Timeframe):
        self.timeframe = timeframe

    def set_trading_direction(self, trading_direction: TradingDirection):
        self.trading_direction = trading_direction

    def set_risk_percent(self, risk_percent: float):
        self.risk_percent = risk_percent

    def set_telegram_config(self, telegram_config: TelegramConfiguration):
        self.telegram_config = telegram_config


class ConfigReader:
    """
    Reads and validates configuration settings, providing accessors for each property.
    """
    _configs: Dict[str, 'ConfigReader'] = {}
    _lock = threading.Lock()

    def __init__(self, config_file_param: str):
        self.config_file = config_file_param
        self.config = None
        self.enabled = None
        self.broker_config = None
        self.trading_configs: List[TradingConfiguration] = []
        self.bot_config = None
        self.telegram_config = None
        self.mongo_config = None
        self.rabbitmq_config = None
        self.params = {}
        self._initialize_config()

    @classmethod
    def load_config(cls, config_file_param: str) -> 'ConfigReader':
        """
        Loads the configuration file and caches it based on the bot's name.
        """
        with open(config_file_param, 'r') as f:
            temp_config = json.load(f)
        bot_name = temp_config.get('bot', {}).get('name')
        bot_key = bot_name.lower() if bot_name else None

        with cls._lock:
            if bot_key not in cls._configs:
                cls._configs[bot_key] = cls(config_file_param)
            return cls._configs[bot_key]

    def _initialize_config(self):
        """
        Loads and validates the configuration file structure.
        """
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        if not self.config:
            raise ValueError("Configuration not loaded.")

        # Initialize each section if present
        self.enabled = self.config.get("enabled", False)
        self.broker_config = self.config.get("broker", {})

        # Initialize bot configuration
        bot_config = self.config.get("bot", {})
        bot_config['mode'] = string_to_enum(Mode, bot_config.get('mode', '').upper())
        self.bot_config = bot_config

        # Validate and initialize trading configurations
        trading_config = self.config.get("trading", {})
        self.trading_configs = [
            self._validate_configuration_item(item, bot_config['name'])
            for item in trading_config.get("configurations", [])
        ] if "configurations" in trading_config else []

        # Validate MongoDB and RabbitMQ sections
        self.mongo_config = self.config.get("mongo", None)
        self.rabbitmq_config = self.config.get("rabbitmq", None)

        # Validate MongoDB or RabbitMQ presence based on mode
        mode = self.get_bot_mode()
        if mode == Mode.MIDDLEWARE:
            # If mode is MIDDLEWARE, either MongoDB or RabbitMQ must be present
            if not self.mongo_config and not self.rabbitmq_config:
                raise ValueError("In 'MIDDLEWARE' mode, either MongoDB or RabbitMQ configuration must be provided.")
        else:
            # For other modes, both MongoDB and RabbitMQ are optional
            if not self.mongo_config and not self.rabbitmq_config:
                print("Warning: Both MongoDB and RabbitMQ configurations are missing.")

        # Additional validations for RabbitMQ
        if self.rabbitmq_config:
            required_rabbitmq_keys = ["host", "port", "username", "password", "exchange"]
            for key in required_rabbitmq_keys:
                if key not in self.rabbitmq_config:
                    raise ValueError(f"Missing key '{key}' in RabbitMQ configuration.")

        # Additional validations for MongoDB
        if self.mongo_config:
            required_mongo_keys = ["host", "port", "db_name"]
            for key in required_mongo_keys:
                if key not in self.mongo_config:
                    raise ValueError(f"Missing key '{key}' in MongoDB configuration.")

    def _validate_configuration_item(self, item: Dict[str, Any], bot_name: str) -> TradingConfiguration:
        """
        Validates and converts a configuration dictionary into a TradingConfiguration object.
        """
        required_keys = ["symbol", "timeframe", "trading_direction", "risk_percent", "telegram"]
        for key in required_keys:
            if key not in item:
                raise ValueError(f"Missing key '{key}' in a trading configuration item.")

        # Create TelegramConfiguration object
        telegram_config = item["telegram"]
        if not isinstance(telegram_config, dict):
            raise TypeError(f"'telegram' in trading configuration must be a dictionary.")

        telegram_configuration = TelegramConfiguration(
            token=telegram_config["token"],
            chat_ids=telegram_config["chat_ids"]
        )

        return TradingConfiguration(
            bot_name=bot_name,
            symbol=item["symbol"],
            timeframe=string_to_enum(Timeframe, item["timeframe"]),
            trading_direction=string_to_enum(TradingDirection, item["trading_direction"]),
            risk_percent=float(item.get("risk_percent")),
            telegram_config=telegram_configuration
        )

    # Params registration and retrieval

    def register_param(self, key: str, value: Any):
        self.params[key] = value

    def get_param(self, key: str) -> Any:
        return self.params.get(key)

    # Getters for individual sections and properties

    def get_enabled(self) -> bool:
        return self.enabled

    # Broker Config
    def get_broker_timeout(self) -> int:
        return self.broker_config.get("timeout")

    def get_broker_account(self) -> int:
        return self.broker_config.get("account")

    def get_broker_password(self) -> str:
        return self.broker_config.get("password")

    def get_broker_server(self) -> str:
        return self.broker_config.get("server")

    def get_broker_mt5_path(self) -> str:
        return self.broker_config.get("mt5_path")

    # Trading Config
    def get_trading_configurations(self) -> List[TradingConfiguration]:
        return self.trading_configs

    def get_trading_configuration_by_symbol(self, symbol: str) -> Optional[TradingConfiguration]:
        for config in self.trading_configs:
            if config.get_symbol() == symbol:
                return config
        return None

    def get_trading_configuration_by_timeframe(self, timeframe: Timeframe) -> List[TradingConfiguration]:
        return [config for config in self.trading_configs if config.get_timeframe() == timeframe]

    # Bot Config
    def get_bot_version(self) -> float:
        return self.bot_config.get("version")

    def get_bot_name(self) -> str:
        return self.bot_config.get("name")

    def get_bot_magic_number(self) -> int:
        return self.bot_config.get("magic_number")

    def get_bot_logging_level(self) -> str:
        return self.bot_config.get("logging_level")

    def get_bot_mode(self) -> Mode:
        return self.bot_config.get("mode")

    # Mongo Config
    def get_mongo_host(self) -> str:
        return self.mongo_config.get("host")

    def get_mongo_port(self) -> int:
        return int(self.mongo_config.get("port"))

    def get_mongo_db_name(self) -> str:
        return self.mongo_config.get("db_name")

    # RabbitMQ Config
    def get_rabbitmq_host(self) -> str:
        return self.rabbitmq_config.get("host", "localhost")

    def get_rabbitmq_port(self) -> int:
        return int(self.rabbitmq_config.get("port", 5672))

    def get_rabbitmq_username(self) -> str:
        return self.rabbitmq_config.get("username", "guest")

    def get_rabbitmq_password(self) -> str:
        return self.rabbitmq_config.get("password", "guest")

    def get_rabbitmq_exchange(self) -> str:
        return self.rabbitmq_config.get("exchange", "")
