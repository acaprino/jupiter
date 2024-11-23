import asyncio
import aio_pika
from aio_pika import ExchangeType
from typing import Callable, Optional, Dict, Any

from aio_pika.abc import AbstractIncomingMessage

from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class RabbitMQService:
    def __init__(
            self,
            worker_id: str,
            user: str,
            password: str,
            rabbitmq_host: str,
            port: int,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.amqp_url = f"amqp://{user}:{password}@{rabbitmq_host}:{port}/"
        self.loop = loop or asyncio.get_event_loop()
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.listeners: Dict[str, Any] = {}  # Unique key for each listener
        self.logger = BotLogger.get_logger(worker_id)
        self.consumer_tasks: Dict[str, asyncio.Task] = {}  # Track consumer tasks
        self.started = False

    @exception_handler
    async def connect(self):
        """
        Establishes the connection and creates a channel.
        """
        self.connection = await aio_pika.connect_robust(self.amqp_url, loop=self.loop, heartbeat=30)
        self.channel = await self.connection.channel()
        # Set the maximum number of unacknowledged messages
        await self.channel.set_qos(prefetch_count=10)
        self.logger.info("Connected to RabbitMQ")

    @exception_handler
    async def disconnect(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection:
            await self.connection.close()
            self.logger.info("Disconnected from RabbitMQ")

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
                Registers a listener for a specific exchange and routing key.

                :param exchange_name: Name of the exchange.
                :param callback: Asynchronous callback function to be called when a message arrives.
                :param exchange_type: Type of exchange (ExchangeType.FANOUT or ExchangeType.TOPIC).
                :param routing_key: Routing key (required for "topic").
                :param queue_name: Name of the queue. If None, an exclusive queue is generated.
                """
        if not self.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        # Declare or get the exchange
        exchange = await self.channel.declare_exchange(
            exchange_name, exchange_type, durable=True, auto_delete=False
        )

        # Declare a queue; if queue_name is None, create an exclusive queue
        # queue = await self.channel.declare_queue(queue_name, exclusive=(queue_name is None))
        queue = await self.channel.declare_queue(queue_name, exclusive=True, durable=True, auto_delete=True)

        # Bind the queue to the exchange with the routing key if applicable
        if exchange_type == ExchangeType.TOPIC:
            if not routing_key:
                raise ValueError("routing_key is required for 'topic' exchanges")
            await queue.bind(exchange, routing_key)
        elif exchange_type == ExchangeType.DIRECT:
            if not routing_key:
                raise ValueError("routing_key is required for 'direct' exchanges")
            await queue.bind(exchange, routing_key)
        else:
            # For fanout, routing key is not used
            await queue.bind(exchange)

        # Define the asynchronous message processor
        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    # Optionally, you can reject the message or send it to a dead-letter queue

            # Define the synchronous handler that schedules the async processing

        def on_message(message: AbstractIncomingMessage) -> Any:
            task = asyncio.create_task(process_message(message))
            # Optionally, track the task if you need to manage it later
            self.consumer_tasks[f"{exchange_name}:{message.delivery_tag}"] = task
            task.add_done_callback(lambda t: self.consumer_tasks.pop(f"{exchange_name}:{message.delivery_tag}", None))

        # Start consuming messages with the synchronous handler
        await queue.consume(on_message)
        self.logger.info(f"Listener registered for exchange '{exchange_name}' with routing_key '{routing_key}'")

    @exception_handler
    async def publish_message(
            self,
            exchange_name: str,
            message: QueueMessage,
            routing_key: Optional[str] = None,
            exchange_type: ExchangeType = ExchangeType.FANOUT
    ):
        """
        Publishes a message to a specific exchange.

        :param exchange_name: Name of the exchange.
        :param message: The message body as instance of QueueMessage.
        :param routing_key: Routing key (required for "topic").
        :param exchange_type: Type of exchange ("fanout" or "topic").
        """
        if not self.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        # Declare or get the exchange
        exchange = await self.channel.declare_exchange(
            exchange_name, exchange_type, durable=True
        )

        # Create the message
        message = aio_pika.Message(body=message.to_json().encode())

        # Publish the message
        await exchange.publish(message, routing_key=routing_key or "")
        self.logger.info(f"Message published to exchange '{exchange_name}' with routing_key '{routing_key}'")

    @exception_handler
    async def publish_to_queue(
            self,
            queue_name: str,
            message: QueueMessage
    ):
        """
        Publishes a message directly to a specific queue.

        :param queue_name: Name of the queue.
        :param message: The message body as instance of QueueMessage.
        """
        if not self.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        # Declare the queue (idempotent operation)
        await self.channel.declare_queue(queue_name, durable=True)

        # Create the message
        message = aio_pika.Message(body=message.to_json())

        # Publish the message directly to the queue using the default exchange with the queue name as routing key
        await self.channel.default_exchange.publish(message, routing_key=queue_name)
        self.logger.info(f"Message published directly to queue '{queue_name}'")

    @exception_handler
    async def send_message(
            self,
            destination: str,
            message: QueueMessage,
            routing_key: Optional[str] = None,
            exchange_type: Optional[ExchangeType] = None
    ):
        """
        Sends a message either to an exchange (fanout or topic) or directly to a queue.

        :param destination: Name of the exchange or queue.
        :param message: The message body as instance of QueueMessage.
        :param routing_key: Routing key (required for "topic" exchanges).
        :param exchange_type: Type of exchange ("fanout" or "topic"). If None, sends directly to a queue.
        """
        if exchange_type:
            # Sending to an exchange
            await self.publish_message(
                exchange_name=destination,
                message=message,
                routing_key=routing_key,
                exchange_type=exchange_type
            )
        else:
            # Sending directly to a queue
            await self.publish_to_queue(
                queue_name=destination,
                message=message
            )

    @exception_handler
    async def start(self):
        """
        Starts the RabbitMQ service by establishing a connection.
        """
        if self.started:
            raise RuntimeError("RabbitMQ service is already started.")
        await self.connect()
        self.started = True

    @exception_handler
    async def stop(self):
        """
        Stops the RabbitMQ service by cancelling all consumers and closing the connection.
        """
        # Cancel all consumer tasks
        if not self.started:
            raise RuntimeError("RabbitMQ service is not started.")
        self.logger.info("Cancelling all consumer tasks...")
        tasks = list(self.consumer_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.info("All consumer tasks cancelled.")

        # Disconnect from RabbitMQ
        await self.disconnect()
        self.started = False
