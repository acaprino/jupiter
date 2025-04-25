import asyncio

from abc import ABC, abstractmethod
from typing import Optional
from brokers.broker_proxy import Broker
from dto.QueueMessage import QueueMessage
from misc_utils import utils_functions
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import to_serializable, extract_properties, new_id
from notifiers.notifier_market_state import NotifierMarketState
from services.service_rabbitmq import RabbitMQService


class RegistrationAwareAgent(LoggingMixin, ABC):
    _REGISTRATION_TIMEOUT = 60 * 5  # Timeout for registration in seconds
    _MAX_REGISTRATION_RETRIES = 3  # Maximum number of registration attempts

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initialize the registration-aware agent with configuration and trading parameters.

        Constructs the topic and agent name using sanitized values.
        Sets up the execution lock and registration event.
        """
        super().__init__(config)
        self.id = new_id()
        self._sanitized_symbol = self._sanitize_routing_key(trading_config.get_symbol())
        self._sanitized_timeframe = self._sanitize_routing_key(trading_config.get_timeframe().name)
        self._sanitized_direction = self._sanitize_routing_key(trading_config.get_trading_direction().name)

        self.topic = f"{self._sanitized_symbol}.{self._sanitized_timeframe}.{self._sanitized_direction}"
        prefix = (trading_config.get_agent() or config.get_bot_mode().name)[:3]
        self.agent = f"{prefix}_{self.topic}"

        self.config = config
        self.trading_config = trading_config
        self.execution_lock = asyncio.Lock()
        self.client_registered_event = asyncio.Event()
        self.context = utils_functions.log_config_str(trading_config)
        self._registration_consumer_tag: Optional[str] = None
        self._market_observer_id: Optional[str] = None
        self.rabbitmq_s = None

    @staticmethod
    def _sanitize_routing_key(value: str) -> str:
        """
        Replace invalid routing key characters (e.g., spaces) with underscores.
        """
        invalid_chars = [" "]
        for char in invalid_chars:
            value = value.replace(char, "_")
        return value.strip()

    @exception_handler
    async def routine_start(self):
        """
        Begin agent routine by registering a RabbitMQ listener,
        sending the registration request, and waiting for confirmation.
        Also registers the market state observer.
        """
        self.info(f"Starting routine {self.agent} (ID: {self.id})")
        self.rabbitmq_s = await RabbitMQService.get_instance()

        try:
            # Listener for signal registration ACK
            mode_prefix = self.config.get_bot_mode().name[:3]
            routing_key_registration_ack_ack = "system.registration_ack"
            full_routing_key_registration_ack_ack = f"{routing_key_registration_ack_ack}.{self.id}"
            queue_name_registration_ack_ack = f"{routing_key_registration_ack_ack}.{self.config.get_instance_name()}.{mode_prefix}.{self.topic}.{self.id}"
            self.info(f"Registering [Registration ACK] listener on topic '{self.topic}' with routing key '{full_routing_key_registration_ack_ack}' and queue '{queue_name_registration_ack_ack}'.")
            self._registration_consumer_tag = await self.rabbitmq_s.register_listener(
                exchange_name=RabbitExchange.jupiter_system.name,
                exchange_type=RabbitExchange.jupiter_system.exchange_type,
                routing_key=full_routing_key_registration_ack_ack,
                callback=self.on_client_registration_ack,
                queue_name=queue_name_registration_ack_ack
            )
            self.info(f"Registered RabbitMQ listener with tag {self._registration_consumer_tag}")
        except Exception as e:
            self.error(f"Failed to register RabbitMQ listener: {str(e)}", exc_info=e)
            raise

        for attempt in range(self._MAX_REGISTRATION_RETRIES):
            try:
                await self._send_registration_request()
                self.info(f"Sent registration request (attempt {attempt + 1})")
                await self._wait_registration_confirmation()
                break
            except asyncio.TimeoutError:
                if attempt == self._MAX_REGISTRATION_RETRIES - 1:
                    raise RuntimeError(f"Registration failed after {self._MAX_REGISTRATION_RETRIES} attempts")
                self.warning(f"Registration timeout, retrying ({attempt + 1}/{self._MAX_REGISTRATION_RETRIES})")

        await self.start()

        try:
            m_state_notif = await NotifierMarketState.get_instance(self.config)
            self._market_observer_id = await m_state_notif.register_observer(
                self.trading_config.symbol,
                self.on_market_status_change,
                self.id
            )
            self.info(f"Registered market observer with ID {self._market_observer_id}")
        except Exception as e:
            await self._cleanup_resources()
            raise RuntimeError(f"Market state registration failed: {str(e)}")

    async def _send_registration_request(self):
        """
        Build and send a client registration message to the middleware.
        """
        registration_payload = to_serializable(self.trading_config.get_telegram_config())
        registration_payload["routine_id"] = self.id
        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction", "bot_name"])
        registration_payload.update({
            "routine_id": self.id,
            "mode": self.config.get_bot_mode().name
        })
        self.debug(f"Registration payload: {registration_payload}")

        message_meta_inf = MessageMetaInf(
            agent_name=self.agent,
            routine_id=self.id,
            mode=self.config.get_bot_mode(),
            bot_name=self.config.get_bot_name(),
            instance_name=self.config.get_instance_name(),
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            direction=self.trading_config.get_trading_direction(),
            ui_token=self.trading_config.get_telegram_config().get_token(),
            ui_users=self.trading_config.get_telegram_config().get_chat_ids()
        )

        client_registration_message = QueueMessage(
            sender=self.agent,
            payload=registration_payload,
            recipient="middleware",
            meta_inf=message_meta_inf
        )

        await self.rabbitmq_s.publish_message(
            exchange_name=RabbitExchange.jupiter_system.name,
            exchange_type=RabbitExchange.jupiter_system.exchange_type,
            routing_key="middleware.registration",
            message=client_registration_message
        )

    async def _wait_registration_confirmation(self):
        """
        Await registration confirmation for a specified timeout.
        """
        try:
            await asyncio.wait_for(self.client_registered_event.wait(), timeout=self._REGISTRATION_TIMEOUT)
        except asyncio.TimeoutError as t:
            self.error("Registration acknowledgment timeout", exc_info=t)
            raise
        finally:
            self.client_registered_event.clear()

    @exception_handler
    async def routine_stop(self):
        """
        Stop the agent routine and clean up resources.
        """
        self.info(f"Stopping routine {self.agent} (ID: {self.id})")
        await self.stop()
        await self._cleanup_resources()

    async def _cleanup_resources(self):
        """
        Unregister RabbitMQ listener and market observer.
        """
        try:
            if self._registration_consumer_tag:
                rabbitmq_s = await self.rabbitmq_s.get_instance()
                await rabbitmq_s.unregister_listener(self._registration_consumer_tag)
                self._registration_consumer_tag = None
                self.info("Unregistered RabbitMQ listener")
        except Exception as e:
            self.error(f"Error unregistering RabbitMQ listener: {str(e)}", exc_info=e)
        try:
            if self._market_observer_id:
                m_state_notif = await NotifierMarketState.get_instance(self.config)
                await m_state_notif.unregister_observer(
                    self.trading_config.symbol,
                    self._market_observer_id
                )
                self._market_observer_id = None
                self.info("Unregistered market observer")
        except Exception as e:
            self.error(f"Error unregistering market observer: {str(e)}", exc_info=e)

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        """
        Process the registration acknowledgment; set the registration event if the ID matches AND registration was successful.
        """
        payload = message.payload
        received_routine_id = payload.get("routine_id")
        registration_success = payload.get("success", False)  # Default to False if 'success' key is missing

        if received_routine_id != self.id:
            self.warning(f"Received ACK for different agent ID: {received_routine_id}")
            return

        if registration_success:
            self.info(f"Registration confirmed for {self.agent}")
            self.client_registered_event.set()
        else:
            # Registration failed according to middleware
            self.error(f"Registration failed for {self.agent}. Middleware response indicated failure (e.g., duplicate).")
            # DO NOT set the event. This will cause _wait_registration_confirmation to timeout.

    @abstractmethod
    async def on_market_status_change(self, symbol: str, is_open: bool,
                                      closing_time: float, opening_time: float,
                                      initializing: bool):
        """
        Abstract method to handle market status changes. Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    async def start(self):
        """
        Abstract method for additional startup logic. Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    async def stop(self):
        """
        Abstract method for shutdown logic. Must be implemented by subclasses.
        """
        pass

    def broker(self) -> Broker:
        """
        Retrieve the Broker instance with the agent's context applied.
        """
        return Broker().with_context(self.context)
