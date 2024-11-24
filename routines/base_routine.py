import asyncio
import uuid
from abc import ABC, abstractmethod

from brokers.mt5_broker import MT5Broker
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable, extract_properties
from services.rabbitmq_service import RabbitMQService


class BaseRoutine(ABC):
    def __init__(self, routine_label, config: ConfigReader, trading_config: TradingConfiguration):
        self.routine_label = routine_label
        self.config = config
        self.trading_config = trading_config
        self.id = str(uuid.uuid4())
        self.logger = BotLogger.get_logger(name=f"{self.routine_label}", level=config.get_bot_logging_level())
        self.execution_lock = asyncio.Lock()
        self.client_registered_event = asyncio.Event()
        self.logger.info(f"Initializing routine {self.routine_label} with id {self.id}")
        self.queue_service = RabbitMQService(routine_label=routine_label,
                                             user=config.get_rabbitmq_username(),
                                             password=config.get_rabbitmq_password(),
                                             rabbitmq_host=config.get_rabbitmq_host(),
                                             port=config.get_rabbitmq_port())
        self.broker = MT5Broker(routine_label=routine_label,
                                account=config.get_broker_account(),
                                password=config.get_broker_password(),
                                server=config.get_broker_server(),
                                path=config.get_broker_mt5_path())

    @exception_handler
    async def routine_start(self):
        await self.queue_service.start()
        # Common registration process
        await self.queue_service.register_listener(
            exchange_name=RabbitExchange.REGISTRATION_ACK.name,
            callback=self.on_client_registration_ack,
            routing_key=self.id,
            exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

        if not await self.broker.startup():
            raise Exception("Broker startup failed.")

        registration_payload = to_serializable(self.trading_config.get_telegram_config())
        registration_payload["sentinel_id"] = self.id
        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction"])
        client_registration_message = QueueMessage(
            sender=self.routine_label,
            payload=registration_payload,
            recipient="middleware",
            trading_configuration=tc)
        await self.queue_service.publish_message(
            exchange_name=RabbitExchange.REGISTRATION.name,
            exchange_type=RabbitExchange.REGISTRATION.exchange_type,
            routing_key=RabbitExchange.REGISTRATION.routing_key,
            message=client_registration_message)

        await self.client_registered_event.wait()
        self.logger.info(f"{self.__class__.__name__} {self.routine_label} started.")

        # Call the custom setup method for subclasses
        await self.start()

    @exception_handler
    async def routine_stop(self):
        self.logger.info(f"Stopping routine {self.routine_label} with id {self.id}")
        await self.queue_service.stop()
        await self.broker.shutdown()
        await self.stop()

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        self.client_registered_event.set()

    @exception_handler
    async def wait_client_registration(self):
        await self.client_registered_event.wait()

    @abstractmethod
    async def start(self):
        """Subclasses implement their specific start logic here."""
        pass

    @abstractmethod
    async def stop(self):
        """Subclasses implement their specific stop logic here."""
        pass
