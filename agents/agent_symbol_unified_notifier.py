"""
SymbolUnifiedNotifier serves as a base class for handling notifications related to trading symbols
and their associated Telegram clients (bots).

Key Features:
1. **Efficient Client Registration**:
   - Registers Telegram clients for specific trading symbols based on their configurations.
   - Consolidates multiple configurations linked to the same Telegram client to avoid redundant notifications.

2. **Optimized Notification Delivery**:
   - Sends notifications only once to each Telegram client, regardless of the number of trading configurations
     or topics (e.g., multiple symbols or timeframes) associated with that client.
   - Prevents duplication of notifications for the same event when multiple topics are linked to a single Telegram bot.

3. **Symbol-to-Client Mapping**:
   - Groups trading configurations by their associated symbols and consolidates Telegram clients for each symbol.
   - Maintains a mapping of Telegram clients to the symbols they are associated with, ensuring precise and efficient notification targeting.

Attributes:
    - `id`: Unique identifier for the notifier instance.
    - `agent`: Name of the agent using the notifier.
    - `config`: Configuration object for the bot settings.
    - `trading_configs`: List of trading configurations handled by the notifier.
    - `logger`: Logger instance for the notifier.
    - `client_registered_event`: Event to signal when a client registration is acknowledged.
    - `broker`: Instance of the broker interface.
    - `symbols_to_telegram_configs`: A dictionary mapping trading symbols to lists of Telegram configurations.
    - `clients_to_topics`: A dictionary mapping Telegram clients (by chat ID) to their associated symbols/topics.

Methods:
    - `routine_start()`: Starts the routine to register all clients and symbols.
    - `routine_stop()`: Stops the notifier and performs cleanup.
    - `group_configs_by_symbol()`: Groups trading configurations by symbols, consolidating Telegram configurations.
    - `map_clients_to_topics()`: Maps Telegram clients to the symbols they are associated with.
    - `register_clients_for_symbol()`: Registers clients for a specific symbol and waits for acknowledgments.
    - `register_single_client()`: Registers a single client and waits for its acknowledgment.
    - `send_message_to_all_clients_for_symbol()`: Sends a notification to all Telegram clients for a given symbol,
      ensuring each client is notified only once.
    - `send_message_to_client()`: Sends a notification to an individual Telegram client.
    - `wait_client_registration()`: Waits for the client registration process to complete.
    - `start()`: Abstract method for subclass-specific startup logic.
    - `stop()`: Abstract method for subclass-specific cleanup logic.
"""

import asyncio
import re
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Optional

from brokers.broker_proxy import Broker
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger, with_bot_logger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import to_serializable
from services.service_rabbitmq import RabbitMQService

@with_bot_logger
class SymbolUnifiedNotifier(ABC):

    def __init__(self, agent: str, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        """
        Initializes an agent instance with the provided configuration and more trading settings.
        The agent is capable of registering itself with the Middleware for multiple clientsd (Telegram bots), each one linked to a Symbol
        and waits for a registration confirmation (acknowledgment) for each registered clients.
        This type of agent is capable of broadcasting a single message for a specific topic to all the clients (Telegram bot) linked to the symbol of the topic.
        This way, a message is sent only once for each client (Telegram bot) event if the same client is linked to more trading configurations.

        :param agent: The name of the agent.
        :param config: Configuration reader for the bot settings.
        :param trading_configs: List of trading configurations.
        """
        self.id = str(uuid.uuid4())
        self.agent = agent
        self.config = config
        self.trading_configs = trading_configs
        self.logger = BotLogger.get_logger(name=self.config.get_bot_name(), level=config.get_bot_logging_level())
        self.client_registered_event = asyncio.Event()
        self.broker = Broker()
        self.symbols = {config.symbol for config in self.trading_configs}  # Set of all symbols from trading configurations
        self.clients_registrations = defaultdict(dict)  # To store client registrations
        self.symbols_to_telegram_configs = self.group_configs_by_symbol()

    def to_camel_case(self, text: str) -> str:
        """
        Converts a given string to camelCase.

        :param text: The input string to convert.
        :return: The camelCase version of the string.
        """
        # Split string into words using spaces or other delimiters
        words = re.split(r'[\s_-]+', text.strip())

        # Convert first word to lowercase, and capitalize the subsequent words
        return words[0].lower() + ''.join(word.capitalize() for word in words[1:])

    def group_configs_by_symbol(self):
        """
        Group trading configurations by their associated symbol.

        :return: A dictionary where keys are symbols and values are lists of telegram configurations.
        """
        self.logger.debug("Grouping trading configurations by symbol.")
        symbol_map = defaultdict(set)
        for config in self.trading_configs:
            symbol_map[config.symbol].add(config.telegram_config)
        grouped_configs = {symbol: list(configs) for symbol, configs in symbol_map.items()}
        self.logger.info(f"Grouped symbols: {grouped_configs}")
        return grouped_configs

    async def routine_start(self):
        """
        Start the routine to register clients for all symbols and configurations.
        """
        self.logger.info("Starting agent for client registration.")
        for symbol, telegram_configs in self.symbols_to_telegram_configs.items():
            self.logger.debug(f"Registering clients for symbol '{symbol}'.")
            await self.register_clients_for_symbol(symbol, telegram_configs)
        self.logger.info("All clients registered. Starting custom logic.")
        await self.start()

    async def routine_stop(self):
        """
        Start the routine to register clients for all symbols and configurations.
        """
        self.logger.info("Stopping agent for client registration.")
        await self.stop()

    async def register_clients_for_symbol(self, symbol, telegram_configs):
        """
        Register clients for a specific symbol and handle ACK responses.

        :param symbol: The trading symbol.
        :param telegram_configs: List of telegram configurations for the symbol.
        """
        for telegram_config in telegram_configs:
            client_id = await self.register_single_client(symbol, telegram_config)
            try:
                await asyncio.wait_for(self.client_registered_event.wait(), timeout=60)
                self.logger.info(f"ACK received for {client_id}!")
                self.clients_registrations[symbol][client_id] = telegram_config
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout while waiting for ACK for {client_id}.")
            finally:
                # TODO unregister listener lo free memory
                pass
        await self.registration_ack(symbol, telegram_configs)

    @abstractmethod
    async def registration_ack(self, symbol, telegram_configs):
        pass

    async def register_single_client(self, symbol, telegram_config):
        """
        Register a single client by sending a registration message.

        :param symbol: The trading symbol.
        :param telegram_config: Telegram configuration for the client.
        :return: The unique client ID.
        """
        client_id = str(uuid.uuid4())
        self.logger.info(f"Sending registration message with ID {client_id} for symbol '{symbol}'.")
        registration_payload = to_serializable(telegram_config)
        registration_payload["routine_id"] = client_id

        await RabbitMQService.register_listener(
            exchange_name=RabbitExchange.REGISTRATION_ACK.name,
            callback=self.on_client_registration_ack,
            routing_key=client_id,
            exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type)

        self.client_registered_event.clear()
        await self.send_queue_message(
            exchange=RabbitExchange.REGISTRATION,
            routing_key=RabbitExchange.REGISTRATION.routing_key,
            symbol=symbol,
            payload=registration_payload,
            recipient="middleware"
        )
        return client_id

    @exception_handler
    async def on_client_registration_ack(self, routing_key: str, message: QueueMessage):
        self.logger.info(f"Client with id {routing_key} successfully registered, calling registration callback.")
        self.client_registered_event.set()

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 symbol: str,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):
        """
        Send a message to the queue.

        :param exchange: RabbitMQ exchange where the message will be sent.
        :param payload: The message payload.
        :param symbol: The trading symbol associated with the message.
        :param routing_key: Optional routing key.
        :param recipient: Optional recipient name.
        """
        self.logger.info(f"Publishing message to exchange '{exchange.name}' with payload: {payload}.")
        recipient = recipient if recipient is not None else "middleware"

        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        tc = {"symbol": symbol, "timeframe": None, "trading_direction": None, "bot_name": self.config.get_bot_name()}
        await RabbitMQService.publish_message(exchange_name=exchange_name,
                                              message=QueueMessage(sender=self.agent, payload=payload, recipient=recipient, trading_configuration=tc),
                                              routing_key=routing_key,
                                              exchange_type=exchange_type)

    @exception_handler
    async def send_message_to_all_clients_for_symbol(self, message: str, symbol: str):
        """
        Send a message to all registered clients for a specific symbol only once, even if the same client is linked to multiple trading
        configurations with the same symbol.

        :param message: The message to be sent.
        :param symbol: The trading symbol for which clients will receive the message.
        """
        self.logger.info(f"Publishing event message '{message}' for symbol '{symbol}'.")
        clients = self.clients_registrations.get(symbol, {})
        if not clients:
            self.logger.warning(f"No clients registered for symbol '{symbol}'.")
            return

        for client_id, client in clients.items():
            self.logger.debug(f"Sending message to client '{client_id}'.")
            await self.send_queue_message(
                exchange=RabbitExchange.NOTIFICATIONS,
                payload={"message": message},
                symbol=symbol,
                routing_key=client_id
            )

    @exception_handler
    async def wait_client_registration(self):
        """
        Wait until the client registration event is triggered.
        """
        self.logger.debug("Waiting for client registration event.")
        await self.client_registered_event.wait()

    @abstractmethod
    async def start(self):
        """
        Subclasses implement their specific start logic here.
        """
        pass

    @abstractmethod
    async def stop(self):
        """
        Subclasses implement their specific stop logic here.
        """
        pass
