import asyncio
import uuid
from abc import abstractmethod

from brokers.broker_proxy import Broker
from dto.QueueMessage import QueueMessage
from misc_utils import utils_functions
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import to_serializable, extract_properties
from notifiers.notifier_market_state import NotifierMarketState
from services.service_rabbitmq import RabbitMQService


class RegistrationAwareAgent(LoggingMixin):

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initializes an agent instance with the provided configuration and trading settings.
        The agent is capable of registering itself with the Middleware and waits for a
        registration confirmation (acknowledgment).

        Each agent of this type has:
        - A unique ID.
        - A topic of the format `{symbol.timeframe.direction}` required to receive messages
          from the Middleware directed specifically to the agent or broadcasted to the topic
          via RabbitMQ.

        The agent does not start its routine until a registration callback is received
        through RabbitMQ.

        Args:
            config (ConfigReader): Configuration object for the bot settings.
            trading_config (TradingConfiguration): Configuration specific to trading,
                including symbol, timeframe, and trading direction.
        """
        super().__init__(config)
        # Initialize the id and the topic
        self.id = str(uuid.uuid4())
        self.topic = f"{trading_config.get_symbol()}.{trading_config.get_timeframe().name}.{trading_config.get_trading_direction().name}"
        prefix = str(trading_config.get_agent()) if trading_config.get_agent() is not None else config.get_bot_mode().name
        self.agent = f"{prefix}_{self.topic}"
        # Initialize the configuration
        self.config = config
        self.trading_config = trading_config
        # Initialize synchronization primitives
        self.execution_lock = asyncio.Lock()
        self.client_registered_event = asyncio.Event()
        self.context = utils_functions.log_config_str(trading_config)

    @exception_handler
    async def routine_start(self):
        """
        Starts the agent's routine, performing the following steps:

        1. Registers the agent with the middleware through the RabbitMQ direct exchange
           `REGISTRATION` using the static routing key `registration.exchange`.
        2. Subscribes to registration acknowledgment messages via RabbitMQ through the
           RabbitMQ direct exchange `REGISTRATION_ACK` using the agent's ID as the routing key.
        3. Waits for a successful registration acknowledgment callback message from the middleware
           through the RabbitMQ exchange `REGISTRATION_ACK` using the agent's ID as the routing key.
        4. Registers the agent with the market state observer for its associated trading symbol.
        5. Invokes the subclass-specific `start` method to execute custom startup logic.

        Raises:
            Exception: If any error occurs during the startup process.
        """

        self.info(f"Starting routine {self.agent} with id {self.id}")
        # Common registration process
        self.info(f"Registering listener for client registration ack with id {self.id}")
        await RabbitMQService.register_listener(
            exchange_name=RabbitExchange.REGISTRATION_ACK.name,
            callback=self.on_client_registration_ack,
            routing_key=self.id,
            exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

        self.info(f"Sending client registration message with id {self.id}")
        registration_payload = to_serializable(self.trading_config.get_telegram_config())
        registration_payload["routine_id"] = self.id
        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction", "bot_name"])
        client_registration_message = QueueMessage(
            sender=self.agent,
            payload=registration_payload,
            recipient="middleware",
            trading_configuration=tc)

        await RabbitMQService.publish_message(
            exchange_name=RabbitExchange.REGISTRATION.name,
            exchange_type=RabbitExchange.REGISTRATION.exchange_type,
            routing_key=RabbitExchange.REGISTRATION.routing_key,
            message=client_registration_message)

        self.info(f"Waiting for client registration on with client id {self.id}.")
        await self.client_registered_event.wait()
        self.info(f"{self.__class__.__name__} {self.agent} started.")

        await NotifierMarketState(self.config).register_observer(
            self.trading_config.symbol,
            self.on_market_status_change,
            self.id
        )

        # Call the custom setup method for subclasses
        await self.start()

    @abstractmethod
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        pass

    @exception_handler
    async def routine_stop(self):
        """
        Stops the agent's routine gracefully and invokes the subclass-specific `stop` method to execute custom stop logic.

        Raises:
            Exception: If any error occurs during the shutdown process.
        """

        self.info(f"Stopping routine {self.agent} with id {self.id}")
        await self.stop()

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        """
        Callback for handling registration acknowledgment from the middleware.

        This method processes an acknowledgment message received on the RabbitMQ
        `REGISTRATION_ACK` exchange, using the agent's ID as the routing key.

        Args:
            routing_key (str): The routing key associated with the acknowledgment, which matches the agent's ID.
            message (QueueMessage): The acknowledgment message containing details of the registration.

        Side Effects:
            Sets the `client_registered_event` to signal that registration has been successfully completed.
        """

        self.info(f"Client with id {self.id} successfully registered, calling registration callback.")
        self.client_registered_event.set()

    @exception_handler
    async def wait_client_registration(self):
        """
        Waits for the client registration process to complete.

        This method blocks execution until the `client_registered_event` is set. The event is
        triggered upon receiving a registration acknowledgment callback through the RabbitMQ
        `REGISTRATION_ACK` exchange, using the agent's ID as the routing key.
        """

        await self.client_registered_event.wait()

    @abstractmethod
    async def start(self):
        """Subclasses implement their specific start logic here."""
        pass

    @abstractmethod
    async def stop(self):
        """Subclasses implement their specific stop logic here."""
        pass

    def broker(self) -> Broker:
        return Broker().with_context(self.context)