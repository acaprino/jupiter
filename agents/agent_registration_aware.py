import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import Optional

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


class RegistrationAwareAgent(LoggingMixin, ABC):
    _REGISTRATION_TIMEOUT = 30  # seconds
    _MAX_REGISTRATION_RETRIES = 3

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        super().__init__(config)
        self.id = str(uuid.uuid4())
        self._sanitized_symbol = self._sanitize_routing_key(trading_config.get_symbol())
        self._sanitized_timeframe = self._sanitize_routing_key(trading_config.get_timeframe().name)
        self._sanitized_direction = self._sanitize_routing_key(trading_config.get_trading_direction().name)

        self.topic = f"{self._sanitized_symbol}.{self._sanitized_timeframe}.{self._sanitized_direction}"
        prefix = trading_config.get_agent() or config.get_bot_mode().name
        self.agent = f"{prefix}_{self.topic}"

        self.config = config
        self.trading_config = trading_config
        self.execution_lock = asyncio.Lock()  # For subclass use
        self.client_registered_event = asyncio.Event()
        self.context = utils_functions.log_config_str(trading_config)
        self._registration_consumer_tag: Optional[str] = None
        self._market_observer_id: Optional[str] = None
        self.rabbitmq_s = None

    @staticmethod
    def _sanitize_routing_key(value: str) -> str:
        """Replace invalid RabbitMQ routing key characters with underscores, except for valid routing key characters."""
        invalid_chars = [" "]
        for char in invalid_chars:
            value = value.replace(char, "_")
        return value.strip()

    @exception_handler
    async def routine_start(self):
        self.info(f"Starting routine {self.agent} (ID: {self.id})")

        self.rabbitmq_s = await RabbitMQService.get_instance()

        try:
            self._registration_consumer_tag = await self.rabbitmq_s.register_listener(
                exchange_name=RabbitExchange.REGISTRATION_ACK.name,
                callback=self.on_client_registration_ack,
                routing_key=self.id,
                exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type
            )
            self.info(f"Successfully registered RabbitMQ listener with tag {self._registration_consumer_tag}")
        except Exception as e:
            self.error(f"Failed to register RabbitMQ listener: {str(e)}")
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

        try:
            m_state_notif = await NotifierMarketState.get_instance(self.config)
            self._market_observer_id = await m_state_notif.register_observer(
                self.trading_config.symbol,
                self.on_market_status_change,
                self.id
            )
            self.info(f"Successfully registered market observer with ID {self._market_observer_id}")
        except Exception as e:
            await self._cleanup_resources()
            raise RuntimeError(f"Market state registration failed: {str(e)}")

        await self.start()

    async def _send_registration_request(self):
        registration_payload = to_serializable(self.trading_config.get_telegram_config())
        registration_payload.update({
            "routine_id": self.id,
            "status": "register"
        })

        tc = extract_properties(self.trading_config,
                                ["symbol", "timeframe", "trading_direction", "bot_name"])

        client_registration_message = QueueMessage(
            sender=self.agent,
            payload=registration_payload,
            recipient="middleware",
            trading_configuration=tc
        )

        await self.rabbitmq_s.publish_message(
            exchange_name=RabbitExchange.REGISTRATION.name,
            exchange_type=RabbitExchange.REGISTRATION.exchange_type,
            routing_key=RabbitExchange.REGISTRATION.routing_key,
            message=client_registration_message
        )

    async def _wait_registration_confirmation(self):
        try:
            await asyncio.wait_for(self.client_registered_event.wait(),
                                   timeout=self._REGISTRATION_TIMEOUT)
        except asyncio.TimeoutError:
            self.error("Registration acknowledgment timeout")
            raise
        finally:
            self.client_registered_event.clear()

    @exception_handler
    async def routine_stop(self):
        self.info(f"Stopping routine {self.agent} (ID: {self.id})")
        await self.stop()
        await self._cleanup_resources()

    async def _cleanup_resources(self):
        try:
            if self._registration_consumer_tag:
                rabbitmq_s = await self.rabbitmq_s.get_instance()
                await rabbitmq_s.unregister_listener(self._registration_consumer_tag)
                self._registration_consumer_tag = None
                self.info("Successfully unregistered RabbitMQ listener")
        except Exception as e:
            self.error(f"Error unregistering RabbitMQ listener: {str(e)}", exec_info=e)

        try:
            if self._market_observer_id:
                m_state_notif = await NotifierMarketState.get_instance(self.config)
                await m_state_notif.unregister_observer(
                    self.trading_config.symbol,
                    self._market_observer_id
                )
                self._market_observer_id = None
                self.info("Successfully unregistered market observer")
        except Exception as e:
            self.error(f"Error unregistering market observer: {str(e)}", exec_info=e)

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        if message.payload.get("status") != "success":
            self.error(f"Registration failed: {message.payload.get('error', 'Unknown error')}")
            return

        if message.payload.get("routine_id") != self.id:
            self.warning(f"Received ACK for different agent ID: {message.payload.get('routine_id')}")
            return

        self.info(f"Registration confirmed for {self.agent}")
        self.client_registered_event.set()

    @abstractmethod
    async def on_market_status_change(self, symbol: str, is_open: bool,
                                      closing_time: float, opening_time: float,
                                      initializing: bool):
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    def broker(self) -> Broker:
        """Returns thread/asyncio-safe broker instance with agent context."""
        return Broker.instance().with_context(self.context)
