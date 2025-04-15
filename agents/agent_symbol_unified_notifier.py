import uuid
from typing import List, Optional

from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.message_metainf import MessageMetaInf
from services.service_rabbitmq import RabbitMQService


class SymbolUnifiedNotifier(LoggingMixin):
    """
    A notifier agent for broadcasting messages to all interested clients.
    Registration is managed by higher-level components. This agent is solely responsible for sending broadcast notifications.
    """

    def __init__(self, agent: str, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        """
        Initialize the notifier with a specific agent name, configuration, and a list of trading configurations.

        :param agent: The unique name for the agent (e.g., "Market State Notifier Agent").
        :param config: The configuration object.
        :param trading_configs: A list of trading configuration instances.
        """
        super().__init__(config)
        self.id = str(uuid.uuid4())  # Unique identifier for this notifier instance
        self.agent = agent  # Assigned agent name
        self.config = config
        self.trading_configs = trading_configs
        # Prepare a set of symbols based on trading configurations for internal use
        self.symbols = {tc.symbol for tc in self.trading_configs}
        self.rabbitmq_s = None

    async def routine_start(self):
        """
        Start the notifier routine by initializing the RabbitMQ service.
        """
        self.info("Starting agent routine.")
        self.rabbitmq_s = await RabbitMQService.get_instance()
        await self.start()

    async def routine_stop(self):
        """
        Stop the notifier routine.
        """
        self.info("Stopping agent routine.")
        await self.stop()

    @exception_handler
    async def request_broadcast_notification(self, message_content: str, symbol: str):
        """
        Create and send a broadcast notification for a given symbol.

        :param message_content: The text to broadcast.
        :param symbol: The target symbol for the broadcast.
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        self.info(f"Broadcast request for symbol '{symbol}': {message_content}")

        payload = {
            "message": message_content
        }

        # Use the unique agent identifier for the sender
        agent_id = getattr(self, 'agent', self.agent)
        instance_name = self.config.get_instance_name()

        # Form the routing key and send the message to the broadcast exchange
        await self.send_queue_message(
            exchange=RabbitExchange.jupiter_notifications,
            payload=payload,
            symbol=symbol,
            sender_id=agent_id,
            routing_key=f"notification.broadcast.{instance_name}.{symbol}"
        )

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 symbol: Optional[str] = None,
                                 recipient: Optional[str] = "middleware",
                                 sender_id: Optional[str] = None,
                                 routing_key: Optional[str] = None):
        """
        Helper method to publish a message to a specified RabbitMQ exchange with standard metadata.

        :param exchange: The target RabbitMQ exchange.
        :param payload: The message payload.
        :param symbol: Optional symbol to include in metadata.
        :param recipient: The recipient identifier (default "middleware").
        :param sender_id: Optional sender identifier.
        :param routing_key: The routing key for message delivery.
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        sender = sender_id or self.agent  # Default sender is the agent name

        # Build minimal metadata context using configuration values
        instance_name = self.config.get_instance_name()
        bot_name = self.config.get_bot_name()

        meta_inf = MessageMetaInf(
            bot_name=bot_name,
            instance_name=instance_name,
            agent_name=self.agent,
            symbol=symbol
        )

        q_message = QueueMessage(
            sender=sender,
            payload=payload,
            recipient=recipient,
            meta_inf=meta_inf
        )

        self.info(f"Publishing message to exchange '{exchange.name}' with routing key '{routing_key}': {q_message}")
        await self.rabbitmq_s.publish_message(
            exchange_name=exchange.name,
            message=q_message,
            routing_key=routing_key,
            exchange_type=exchange.exchange_type
        )

    @exception_handler
    async def send_message_to_all_clients_for_symbol(self, message: str, symbol: str):
        """
        Broadcast a general notification to all clients interested in the given symbol.

        :param message: The notification text.
        :param symbol: The target symbol.
        """
        await self.request_broadcast_notification(message, symbol)

    async def start(self):
        """
        Start logic for the notifier. Specific startup logic should be implemented by subclasses.
        """
        pass  # To be implemented by subclasses

    async def stop(self):
        """
        Stop logic for the notifier. Specific shutdown logic should be implemented by subclasses.
        """
        pass  # To be implemented by subclasses
