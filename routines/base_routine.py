import asyncio
import uuid
from abc import ABC, abstractmethod

from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler


class BaseRoutine(ABC):
    def __init__(self, worker_id, log_level: str, client_config: dict, queue_service):
        self.worker_id = worker_id
        self.client_config = client_config
        self.queue_service = queue_service
        self.id = str(uuid.uuid4())
        self.logger = BotLogger.get_logger(name=f"{self.worker_id}", level=log_level)
        self.execution_lock = asyncio.Lock()
        self.client_registered_event = asyncio.Event()
        self.logger.info(f"Initializing routine {self.worker_id} with id {self.id}")

    @exception_handler
    async def common_start(self):
        # Common registration process
        await self.queue_service.register_listener(
            exchange_name=RabbitExchange.REGISTRATION_ACK.name,
            callback=self.on_client_registration_ack,
            routing_key=self.id,
            exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

        registration_payload = self.client_config
        registration_payload['sentinel_id'] = self.id
        client_registration_message = QueueMessage(
            sender=self.worker_id,
            payload=registration_payload,
            recipient="middleware")
        await self.queue_service.publish_message(
            exchange_name=RabbitExchange.REGISTRATION.name,
            exchange_type=RabbitExchange.REGISTRATION.exchange_type,
            routing_key=RabbitExchange.REGISTRATION.routing_key,
            message=client_registration_message)

        await self.client_registered_event.wait()
        self.logger.info(f"{self.__class__.__name__} {self.worker_id} started.")

        # Call the custom setup method for subclasses
        await self.start()

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
