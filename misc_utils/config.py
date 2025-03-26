import json
import threading
from typing import Dict, Any, List, Optional
from itertools import product  # Import product for Cartesian product

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

    def __str__(self):
        return f"TelegramConfiguration(token='{self.token}', chat_ids={self.chat_ids})"

    def __repr__(self):
        return self.__str__()


class TradingConfiguration:
    """
    Represents an individual trading configuration.
    """

    def __init__(self, bot_name: str, agent: Optional[str], symbol: str, timeframe: Timeframe, trading_direction: TradingDirection, invest_percent: float, telegram_config: TelegramConfiguration,
                 magic_number: int):
        self.bot_name = bot_name
        self.agent = agent
        self.symbol = symbol
        self.timeframe = timeframe
        self.trading_direction = trading_direction
        self.invest_percent = invest_percent
        self.telegram_config = telegram_config
        self.magic_number = magic_number

    def __str__(self):
        return (f"TradingConfiguration(bot_name={self.bot_name}, agent={self.agent}, symbol={self.symbol}, "
                f"timeframe={self.timeframe.name}, trading_direction={self.trading_direction.name}, "
                f"invest_percent={self.invest_percent}, magic_number={self.magic_number}, "  # Aggiunto magic_number
                f"telegram_config={self.telegram_config})")

    def __repr__(self):
        return self.__str__()

    # Accessors
    def get_bot_name(self) -> str:
        return self.bot_name

    def get_agent(self) -> str:
        return self.agent

    def get_symbol(self) -> str:
        return self.symbol

    def get_timeframe(self) -> Timeframe:
        return self.timeframe

    def get_trading_direction(self) -> TradingDirection:
        return self.trading_direction

    def get_invest_percent(self) -> float:
        return self.invest_percent

    def get_telegram_config(self) -> TelegramConfiguration:
        return self.telegram_config

    def get_magic_number(self) -> int:  # Nuovo metodo accessore
        return self.magic_number

    # Mutators
    def set_bot_name(self, bot_name: str):
        self.bot_name = bot_name

    def set_agent(self, agent: str):
        self.agent = agent

    def set_symbol(self, symbol: str):
        self.symbol = symbol

    def set_timeframe(self, timeframe: Timeframe):
        self.timeframe = timeframe

    def set_trading_direction(self, trading_direction: TradingDirection):
        self.trading_direction = trading_direction

    def set_invest_percentt(self, invest_percent: float):
        self.invest_percent = invest_percent

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
        bot_name = temp_config.get('name')
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

        # Initialize bot configuration from top-level keys
        bot_config = {
            'version': self.config.get('version'),
            'name': self.config.get('name'),
            'logging_level': self.config.get('logging_level'),
            'mode': string_to_enum(Mode, self.config.get('mode', '').upper())
        }
        self.bot_config = bot_config

        # Validate and initialize trading configurations
        trading_config = self.config.get("trading", [])
        self.trading_configs = []
        if trading_config:
            for item in trading_config:
                configs = self._generate_trading_configurations(item, bot_config['name'])
                self.trading_configs.extend(configs)

        # Validate RabbitMQ section
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
        self.mongo_config = self.config.get("mongo", None)
        if self.mongo_config:
            required_mongo_keys = ["host", "port", "db_name"]
            for key in required_mongo_keys:
                if key not in self.mongo_config:
                    raise ValueError(f"Missing key '{key}' in MongoDB configuration.")

    def _generate_trading_configurations(self, item: Dict[str, Any], bot_name: str) -> List[TradingConfiguration]:
        # Verifica presenza sezione strategies
        strategies = item.get("strategies")
        if not strategies or not isinstance(strategies, list):
            raise ValueError("Missing or invalid 'strategies' array in trading configuration")

        # Parametri condivisi per tutte le strategie
        invest_percent = item.get("invest_percent")
        telegram_config = item.get("telegram")

        # Validazione parametri condivisi
        if invest_percent is None:
            raise ValueError("Missing 'invest_percent' in trading configuration")
        if not telegram_config:
            raise ValueError("Missing 'telegram' configuration in trading section")

        # Crea oggetto TelegramConfiguration
        tg_config = TelegramConfiguration(
            token=telegram_config["token"],
            chat_ids=telegram_config["chat_ids"]
        )

        configurations = []
        used_magic_numbers = set()

        for strategy in strategies:
            # Validazione campi obbligatori per strategia
            required_strategy_keys = ["symbol", "timeframe", "trading_direction", "magic_number"]
            for key in required_strategy_keys:
                if key not in strategy:
                    raise ValueError(f"Missing key '{key}' in strategy configuration")

            # Controllo univocità magic number
            magic_number = strategy["magic_number"]
            if magic_number in used_magic_numbers:
                raise ValueError(f"Duplicate magic_number {magic_number} found in strategies")
            used_magic_numbers.add(magic_number)

            # Conversione a enum
            timeframe_enum = string_to_enum(Timeframe, strategy["timeframe"])
            direction_enum = string_to_enum(TradingDirection, strategy["trading_direction"])

            # Creazione configurazione
            config = TradingConfiguration(
                bot_name=bot_name,
                agent=item.get("agent"),
                symbol=strategy["symbol"],
                timeframe=timeframe_enum,
                trading_direction=direction_enum,
                invest_percent=invest_percent,
                telegram_config=tg_config,
                magic_number=magic_number
            )
            configurations.append(config)

        return configurations

    # Params registration and retrieval

    def register_param(self, key: str, value: Any):
        self.params[key] = value

    def get_param(self, key: str, default: any = None) -> Any:
        return self.params.get(key)

    def is_silent_start(self) -> bool:
        return self.get_param('start_silent', False)

    # Getters for individual sections and properties

    def get_config_file(self) -> str:
        return self.config_file

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

    # Bot Config
    def get_bot_version(self) -> float:
        return self.bot_config.get("version")

    def get_bot_name(self) -> str:
        return self.bot_config.get("name")

    def get_bot_logging_level(self) -> str:
        return self.bot_config.get("logging_level")

    def get_bot_mode(self) -> Mode:
        return self.bot_config.get("mode")

    # Mongo Config
    def get_mongo_host(self) -> Optional[str]:
        return self.mongo_config.get("host") if self.mongo_config else None

    def get_mongo_port(self) -> Optional[int]:
        return int(self.mongo_config.get("port")) if self.mongo_config else None

    def get_mongo_username(self) -> Optional[str]:
        return self.mongo_config.get("username") if self.mongo_config else None

    def get_mongo_password(self) -> Optional[str]:
        return self.mongo_config.get("password") if self.mongo_config else None

    def get_mongo_db_name(self) -> Optional[str]:
        return self.mongo_config.get("db_name") if self.mongo_config else None

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
