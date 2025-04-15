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
    Notifier agent for sending broadcast requests.
    Registration is handled by higher-level classes (e.g., RegistrationAwareAgent).
    This agent focuses solely on broadcasting messages.
    """

    def __init__(self, agent: str, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        """
        Initialize the notifier agent.

        :param agent: Specific agent name (e.g., "Market state notifier agent")
        :param config: Instance of the configuration reader.
        :param trading_configs: List of trading configurations.
        """
        super().__init__(config)
        self.id = str(uuid.uuid4())  # The ID should come from RegistrationAwareAgent
        self.agent = agent  # Specific agent name
        self.config = config
        self.trading_configs = trading_configs
        # Group symbols for internal logic (not for registration)
        self.symbols = {tc.symbol for tc in self.trading_configs}
        self.rabbitmq_s = None

    async def routine_start(self):
        """
        Start the agent routine.
        """
        self.info("Starting agent routine.")
        self.rabbitmq_s = await RabbitMQService.get_instance()
        await self.start()

    async def routine_stop(self):
        """
        Stop the agent routine.
        """
        self.info("Stopping agent routine.")
        await self.stop()

    @exception_handler
    async def request_broadcast_notification(self, message_content: str, symbol: str):
        """
        Sends a broadcast notification request to the middleware to forward the notification to all interested clients.

        :param message_content: The content of the message to be broadcast.
        :param symbol: The target symbol.
        :param notification_type: The type of notification (default is "general").
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        self.info(f"Requesting broadcast for notification on symbol '{symbol}': {message_content}")

        payload = {
            "message": message_content
        }

        # Use the unique agent ID if available; otherwise, use the generic agent name
        agent_id = getattr(self, 'agent', self.agent)

        # Send the message to the TOPIC exchange for broadcast notifications
        await self.send_queue_message(
            exchange=RabbitExchange.jupiter_notifications,
            payload=payload,
            symbol=symbol,
            sender_id=agent_id
        )

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 symbol: Optional[str] = None,
                                 recipient: Optional[str] = "middleware",
                                 sender_id: Optional[str] = None):
        """
        Helper method to send messages to the RabbitMQ queue.

        :param exchange: The RabbitMQ exchange.
        :param payload: The message payload.
        :param symbol: The routing key to be used for the message.
        :param recipient: The recipient of the message (default is "middleware").
        :param sender_id: The sender ID, if provided.
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        sender = sender_id or self.agent  # Use the provided sender ID or the default agent name

        # Create a minimal trading configuration context for the message.
        # This can be enhanced if the agent has access to its own specific configuration.
        instance_name = self.config.get_instance_name()
        bot_name = self.config.get_bot_name()

        meta_inf = MessageMetaInf(
            bot_name=bot_name,
            instance_name=instance_name,
            agent_name=self.agent,
            symbol=symbol
        )

        routing_key = f"notification.broadcast.{instance_name}.{symbol}"

        q_message = QueueMessage(
            sender=sender,
            payload=payload,
            recipient=recipient,
            meta_inf=meta_inf
        )

        self.info(f"Sending message to exchange '{exchange.name}' (Routing Key: {routing_key}): {q_message}")
        await self.rabbitmq_s.publish_message(
            exchange_name=exchange.name,
            message=q_message,
            routing_key=routing_key,
            exchange_type=exchange.exchange_type
        )

    @exception_handler
    async def send_message_to_all_clients_for_symbol(self, message: str, symbol: str):
        """
        Wrapper method to send a general broadcast notification for a given symbol.

        :param message: The message to be broadcast.
        :param symbol: The target symbol.
        """
        await self.request_broadcast_notification(message, symbol)

    async def start(self):
        """
        Subclasses should implement their own start logic (without registration here).
        """
        pass  # To be implemented by subclasses

    async def stop(self):
        """
        Subclasses should implement their own stop logic (without deregistration here).
        """
        pass  # To be implemented by subclasses
