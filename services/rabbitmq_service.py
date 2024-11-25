import asyncio
import aio_pika
from aio_pika import ExchangeType
from typing import Callable, Optional, Dict, Any
from aio_pika.abc import AbstractIncomingMessage
from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from asyncio import Lock
from contextlib import asynccontextmanager


class RabbitMQService:
    def __init__(
            self,
            routine_label: str,
            user: str,
            password: str,
            rabbitmq_host: str,
            port: int,
            loop: Optional[asyncio.AbstractEventLoop] = None,
            max_reconnect_attempts: int = 5,
            reconnect_delay: float = 5.0,
            connection_timeout: float = 30.0
    ):
        self.amqp_url = f"amqp://{user}:{password}@{rabbitmq_host}:{port}/"
        self.loop = loop or asyncio.get_event_loop()
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.listeners: Dict[str, Any] = {}
        self.logger = BotLogger.get_logger(routine_label)
        self.consumer_tasks: Dict[str, asyncio.Task] = {}
        self.started = False
        self.active_subscriptions = set()
        self.connection_lock = Lock()
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.connection_timeout = connection_timeout
        self.connection_attempt = 0

    @asynccontextmanager
    async def get_channel(self):
        """
        Context manager to ensure we have a valid channel and handle reconnection if needed.
        """
        if not self.started:
            raise RuntimeError("RabbitMQ service is not started")

        async with self.connection_lock:
            try:
                if not self.connection or self.connection.is_closed:
                    await self._reconnect()

                if not self.channel or self.channel.is_closed:
                    self.channel = await self.connection.channel()
                    await self.channel.set_qos(prefetch_count=10)

                yield self.channel

            except Exception as e:
                self.logger.error(f"Channel error: {e}")
                await self._reconnect()
                raise

    async def _reconnect(self):
        """
        Handle reconnection with exponential backoff.
        """
        self.connection_attempt += 1
        if self.connection_attempt > self.max_reconnect_attempts:
            raise RuntimeError("Max reconnection attempts reached")

        backoff = min(self.reconnect_delay * (2 ** (self.connection_attempt - 1)), 60)
        self.logger.info(f"Attempting to reconnect (attempt {self.connection_attempt}) after {backoff} seconds")
        await asyncio.sleep(backoff)

        try:
            if self.connection:
                await self.connection.close()

            self.connection = await aio_pika.connect_robust(
                self.amqp_url,
                loop=self.loop,
                timeout=self.connection_timeout,
                heartbeat=30
            )

            self.connection_attempt = 0  # Reset counter on successful connection
            self.logger.info("Successfully reconnected to RabbitMQ")

        except Exception as e:
            self.logger.error(f"Reconnection failed: {e}")
            raise

    @exception_handler
    async def publish_message(
            self,
            exchange_name: str,
            message: QueueMessage,
            routing_key: Optional[str] = None,
            exchange_type: ExchangeType = ExchangeType.FANOUT,
            retry_attempts: int = 3
    ):
        """
        Publishes a message with retry logic.
        """
        for attempt in range(retry_attempts):
            try:
                async with self.get_channel() as channel:
                    exchange = await channel.declare_exchange(
                        exchange_name,
                        exchange_type,
                        durable=True
                    )

                    json_message = message.to_json().encode()
                    message_obj = aio_pika.Message(
                        body=json_message,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    )

                    await exchange.publish(message_obj, routing_key=routing_key or "")
                    self.logger.info(f"Message published to exchange '{exchange_name}' with routing_key '{routing_key}'")
                    return

            except Exception as e:
                if attempt == retry_attempts - 1:
                    raise
                self.logger.warning(f"Publish attempt {attempt + 1} failed: {e}. Retrying...")
                await asyncio.sleep(1 * (attempt + 1))

    @exception_handler
    async def register_listener(
            self,
            exchange_name: str,
            callback: Callable[[str, QueueMessage], Any],
            exchange_type: ExchangeType = ExchangeType.FANOUT,
            routing_key: Optional[str] = None,
            queue_name: Optional[str] = None
    ):
        """
        Registers a listener with automatic recovery.
        """

        async def setup_listener():
            while self.started:
                try:
                    async with self.get_channel() as channel:
                        exchange = await channel.declare_exchange(
                            exchange_name,
                            exchange_type,
                            durable=True,
                            auto_delete=False
                        )

                        queue = await channel.declare_queue(
                            queue_name,
                            exclusive=True,
                            durable=True,
                            auto_delete=True
                        )

                        if exchange_type in (ExchangeType.TOPIC, ExchangeType.DIRECT):
                            if not routing_key:
                                raise ValueError(f"routing_key is required for '{exchange_type}' exchanges")
                            await queue.bind(exchange, routing_key)
                        else:
                            await queue.bind(exchange)

                        async def process_message(message: AbstractIncomingMessage):
                            async with message.process():
                                try:
                                    rec_routing_key = message.routing_key
                                    queue_message = QueueMessage.from_json(message.body.decode())
                                    await callback(rec_routing_key, queue_message)
                                except Exception as e:
                                    self.logger.error(f"Error processing message: {e}")
                                    await message.reject(requeue=True)

                        await queue.consume(process_message)
                        self.logger.info(f"Listener registered for exchange '{exchange_name}'")

                        # Wait until connection is lost or service is stopped
                        while not channel.is_closed and self.started:
                            await asyncio.sleep(1)

                except Exception as e:
                    if not self.started:
                        break
                    self.logger.error(f"Listener error: {e}. Attempting to recover...")
                    await asyncio.sleep(5)

        task = asyncio.create_task(setup_listener())
        self.consumer_tasks[exchange_name] = task