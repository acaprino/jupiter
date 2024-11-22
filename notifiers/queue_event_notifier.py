import asyncio
import json
from typing import List, Callable, Awaitable

import aio_pika

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum


class QueueEventNotifier:
    """
    An asynchronous notifier for RabbitMQ fanout exchanges using asyncio.
    """

    def __init__(self, worker_id, user, password, rabbitmq_host, port: int, exchange_name, topic: str):
        self.RABBITMQ_URL = f"amqp://{user}:{password}@{rabbitmq_host}:{port}/"
        self.exchange_name = exchange_name
        self.topic = topic
        self.connection = None
        self.exchange = None
        self.channel = None
        self.queue = None
        self.loop = asyncio.get_event_loop()
        self.logger = BotLogger.get_logger(worker_id)
        self._on_new_event_callbacks: List[Callable[[dict], Awaitable[None]]] = []
        self._consumer_task = None

    @exception_handler
    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(self.RABBITMQ_URL, heartbeat=30)
            self.channel = await self.connection.channel()

            # Declare the exchange
            self.exchange = await self.channel.declare_exchange(self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)

            # Declare the queue
            self.queue = await self.channel.declare_queue("", durable=True, exclusive=False)
            if not self.queue:
                raise ValueError(f"Failed to declare queue: {self.queue.name}")

            # Bind the queue to the exchange
            await self.queue.bind(self.exchange, routing_key=self.topic)
            self.logger.info(f"Queue '{self.queue.name}' topic declared and bound to exchange '{self.exchange_name}' and binding key '{self.topic}'")
        except Exception as e:
            self.logger.error(f"Error during connection setup: {e}")
            raise

    @exception_handler
    async def _consume_messages(self):
        """
        Internal method to consume messages in a background task.
        """
        try:
            if not self.queue:
                raise ValueError("Queue is not initialized. Ensure 'connect' was called successfully.")

            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        self.logger.info(f"Received: {message.body.decode()}")
                        # Call registered callbacks (if any)
                        event_obj = json.loads(message.body.decode())
                        event_obj["event"] = string_to_enum(EventType, event_obj["event"])
                        for callback in self._on_new_event_callbacks:
                            await callback(event_obj)
        except asyncio.CancelledError:
            self.logger.info("Consumer task cancelled.")
        except Exception as e:
            self.logger.error(f"Error in consumer task: {e}")

    @exception_handler
    async def start(self):
        """
        Start the notifier service by connecting to the RabbitMQ server and starting the consumer task.
        """
        self.logger.info("Starting the QueueEventNotifier service...")
        await self.connect()
        self._consumer_task = asyncio.create_task(self._consume_messages())
        self.logger.info("QueueEventNotifier service is now running.")

    @exception_handler
    async def stop(self):
        """
        Stop the notifier service gracefully.
        """
        if self._consumer_task:
            self.logger.info("Stopping the QueueEventNotifier service...")
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                self.logger.info("Consumer task stopped successfully.")
        if self.connection:
            await self.connection.close()
        self.logger.info("QueueEventNotifier service has been stopped.")

    def register_on_new_event(self, callback: Callable[[dict], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_new_event_callbacks.append(callback)
        self.logger.info("Callback registered for new event notifications.")
