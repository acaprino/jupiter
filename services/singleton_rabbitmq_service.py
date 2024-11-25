import asyncio
import aio_pika
from aio_pika import ExchangeType
from typing import Callable, Optional, Dict, Any

from aio_pika.abc import AbstractIncomingMessage

from dto.QueueMessage import QueueMessage
from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class RabbitMQService:
    _instance: Optional['RabbitMQService'] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(RabbitMQService, cls).__new__(cls)
        return cls._instance

    def __init__(
            self,
            routine_label: str,
            user: str,
            password: str,
            rabbitmq_host: str,
            port: int,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        if not hasattr(self, 'initialized'):
            self.amqp_url = f"amqp://{user}:{password}@{rabbitmq_host}:{port}/"
            self.loop = loop or asyncio.get_event_loop()
            self.connection: Optional[aio_pika.RobustConnection] = None
            self.channel: Optional[aio_pika.RobustChannel] = None
            self.listeners: Dict[str, Any] = {}  # Unique key for each listener
            self.logger = BotLogger.get_logger(routine_label + "_RabbitMQ")
            self.consumer_tasks: Dict[str, asyncio.Task] = {}  # Track consumer tasks
            self.started = False
            self.active_subscriptions = set()
            self.initialized = True

    @staticmethod
    @exception_handler
    async def connect():
        """
        Establishes the connection and creates a channel.
        """
        instance = RabbitMQService._instance
        if instance:
            instance.connection = await aio_pika.connect_robust(instance.amqp_url, loop=instance.loop, heartbeat=30)
            instance.channel = await instance.connection.channel()
            await instance.channel.set_qos(prefetch_count=10)
            instance.logger.info("Connected to RabbitMQ")

    @staticmethod
    @exception_handler
    async def disconnect():
        """
        Closes the connection to RabbitMQ.
        """
        instance = RabbitMQService._instance
        if instance and instance.channel:
            await instance.channel.close()
        if instance and instance.connection:
            await instance.connection.close()
        instance.logger.info("Disconnected from RabbitMQ")

    @staticmethod
    @exception_handler
    async def register_listener(
            exchange_name: str,
            callback: Callable[[str, QueueMessage], Any],
            exchange_type: ExchangeType = ExchangeType.FANOUT,
            routing_key: Optional[str] = None,
            queue_name: Optional[str] = None
    ):
        """
        Registers a listener for a specific exchange and routing key.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        exchange = await instance.channel.declare_exchange(
            exchange_name, exchange_type, durable=True, auto_delete=False
        )

        queue = await instance.channel.declare_queue(queue_name, exclusive=True, durable=True, auto_delete=True)

        if exchange_type == ExchangeType.TOPIC:
            if not routing_key:
                raise ValueError("routing_key is required for 'topic' exchanges")
            await queue.bind(exchange, routing_key)
        elif exchange_type == ExchangeType.DIRECT:
            if not routing_key:
                raise ValueError("routing_key is required for 'direct' exchanges")
            await queue.bind(exchange, routing_key)
        else:
            await queue.bind(exchange)

        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())
                    instance.logger.info(f"Received message '{queue_message}' from exchange '{exchange_name}' with routing_key '{rec_routing_key}'")
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    instance.logger.error(f"Error processing message: {e}")
                    await message.reject(requeue=True)

        def on_message(message: AbstractIncomingMessage) -> Any:
            task = asyncio.create_task(process_message(message))
            instance.consumer_tasks[f"{exchange_name}:{message.delivery_tag}"] = task
            task.add_done_callback(lambda t: instance.consumer_tasks.pop(f"{exchange_name}:{message.delivery_tag}", None))

        await queue.consume(on_message)
        instance.logger.info(f"Listener registered for exchange '{exchange_name}' with routing_key '{routing_key}'")

    @staticmethod
    @exception_handler
    async def publish_message(
            exchange_name: str,
            message: QueueMessage,
            routing_key: Optional[str] = None,
            exchange_type: ExchangeType = ExchangeType.FANOUT
    ):
        """
        Publishes a message to a specific exchange.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        exchange = await instance.channel.declare_exchange(
            exchange_name, exchange_type, durable=True
        )

        json_message = message.to_json().encode()
        message = aio_pika.Message(body=json_message)

        await exchange.publish(message, routing_key=routing_key or "")
        instance.logger.info(f"Message {json_message} published to exchange '{exchange_name}' with routing_key '{routing_key}'")

    @staticmethod
    @exception_handler
    async def publish_to_queue(
            queue_name: str,
            message: QueueMessage
    ):
        """
        Publishes a message directly to a specific queue.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        await instance.channel.declare_queue(queue_name, durable=True)

        message = aio_pika.Message(body=message.to_json().encode())
        await instance.channel.default_exchange.publish(message, routing_key=queue_name)
        instance.logger.info(f"Message published directly to queue '{queue_name}'")

    @staticmethod
    @exception_handler
    async def start():
        """
        Starts the RabbitMQ service by establishing a connection.
        """
        instance = RabbitMQService._instance
        if instance.started:
            raise RuntimeError("RabbitMQ service is already started.")
        await RabbitMQService.connect()
        instance.started = True

    @staticmethod
    @exception_handler
    async def stop():
        """
        Stops the RabbitMQ service gracefully.
        """
        instance = RabbitMQService._instance
        try:
            for consumer_tag, task in list(instance.consumer_tasks.items()):
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    instance.logger.info(f"Consumer {consumer_tag} cancelled.")
                except Exception as e:
                    instance.logger.error(f"Error while cancelling consumer {consumer_tag}: {e}")
                finally:
                    await instance.consumer_tasks.pop(consumer_tag, None)
            instance.logger.info("All consumers have been cancelled.")

            await RabbitMQService.disconnect()
            instance.logger.info("RabbitMQ service stopped successfully.")
        except Exception as e:
            instance.logger.error(f"Error while stopping RabbitMQ service: {e}")
        finally:
            instance.started = False
