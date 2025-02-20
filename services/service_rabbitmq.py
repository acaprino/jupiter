import asyncio
import json

import aio_pika
from aio_pika import ExchangeType
from typing import Callable, Optional, Dict, Any, List, Literal

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustExchange, AbstractRobustQueue

from csv_loggers.logger_rabbit_messages import RabbitMessages
from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import to_serializable

HookType = Callable[[str, str, str, str, Dict, Dict, str, str], None]


class RabbitMQService(LoggingMixin):
    _instance: Optional['RabbitMQService'] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(RabbitMQService, cls).__new__(cls)
        return cls._instance

    def __init__(
            self,
            config: ConfigReader,
            user: str,
            password: str,
            rabbitmq_host: str,
            port: int,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        if not hasattr(self, 'initialized'):
            super().__init__(config)
            self.amqp_url = f"amqp://{user}:{password}@{rabbitmq_host}:{port}/"
            self.loop = loop or asyncio.get_event_loop()
            self.connection: Optional[aio_pika.RobustConnection] = None
            self.channel: Optional[aio_pika.RobustChannel] = None
            self.listeners: Dict[str, Any] = {}
            self.config = config
            self.agent = "RabbitMQ-Service"
            self.consumer_tasks: Dict[str, asyncio.Task] = {}
            self.exchanges: Dict[str, AbstractRobustExchange] = {}
            self.queues: Dict[str, AbstractRobustQueue] = {}
            self.started = False
            self.active_subscriptions = set()
            self.initialized = True
            self._hooks: List[HookType] = []

    @staticmethod
    def register_hook(hook: HookType) -> None:
        instance = RabbitMQService._instance
        instance._hooks.append(hook)

    @staticmethod
    def _notify_hooks(exchange: str,
                      routing_key: str,
                      sender: str,
                      recipient: str,
                      trading_configuration: Any,
                      payload: Any,
                      message_id: str,
                      direction: Literal["incoming", "outgoing"]) -> None:
        instance = RabbitMQService._instance
        for hook in instance._hooks:
            try:
                hook(exchange, routing_key, sender, recipient, trading_configuration, payload, message_id, direction)
            except Exception as e:
                instance.error("Error while calling callback", e)

    @staticmethod
    @exception_handler
    async def connect():
        """
        Establishes the connection and creates a channel.
        """
        instance = RabbitMQService._instance
        if instance:
            instance.connection = await aio_pika.connect_robust(
                instance.amqp_url,
                loop=instance.loop,
                heartbeat=60  # Increased to avoid timeout
            )
            instance.channel = await instance.connection.channel()
            await instance.channel.set_qos(prefetch_count=10)
            instance.info("Connected to RabbitMQ")

    @staticmethod
    @exception_handler
    async def disconnect():
        """
        Closes the connection to RabbitMQ.
        """
        instance = RabbitMQService._instance
        if instance:
            if instance.channel:
                await instance.channel.close()
                instance.channel = None
            if instance.connection:
                await instance.connection.close()
                instance.connection = None
            instance.info("Disconnected from RabbitMQ")

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

        exchange_name = f"{instance.config.get_bot_name()}_{exchange_name}"
        # Use or declare the exchange
        if exchange_name not in instance.exchanges:
            exchange = await instance.channel.declare_exchange(
                exchange_name, exchange_type, durable=True, auto_delete=False
            )
            instance.exchanges[exchange_name] = exchange
        else:
            exchange = instance.exchanges[exchange_name]

        # Use or declare the queue
        if queue_name:
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(
                    queue_name, exclusive=False, durable=True, auto_delete=False
                )
                instance.queues[queue_name] = queue
            else:
                queue = instance.queues[queue_name]
        else:
            # For anonymous queues, we cannot store them by name
            queue = await instance.channel.declare_queue(
                exclusive=False, durable=True, auto_delete=True
            )

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

        def custom_encoder(obj):
            from aio_pika.abc import AbstractIncomingMessage
            if isinstance(obj, AbstractIncomingMessage):
                body = obj.body.decode("utf-8") if isinstance(obj.body, bytes) else obj.body
                return {
                    "body": body,
                    "delivery_tag": obj.delivery_tag,
                    "exchange": obj.exchange,
                    "routing_key": obj.routing_key,
                    "properties": getattr(obj, "properties", None)
                }
            return to_serializable(obj)

        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())

                    instance.logger.info(f"Message received '{queue_message}' from exchange '{exchange_name}' with routing_key '{rec_routing_key}'")
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    instance.logger.error(f"Error processing message: {e}")
                    await message.reject(requeue=True)

        async def on_message(message: AbstractIncomingMessage) -> Any:
            obj_body = QueueMessage.from_json(message.body.decode())

            instance._notify_hooks(exchange=exchange_name,
                                   routing_key=routing_key or "",
                                   sender=obj_body.sender,
                                   recipient=obj_body.recipient,
                                   trading_configuration=obj_body.trading_configuration,
                                   payload=obj_body.payload,
                                   message_id=message.message_id,
                                   direction="incoming")

            task = asyncio.create_task(process_message(message))
            instance.consumer_tasks[f"{exchange_name}:{message.delivery_tag}"] = task

            def task_done_callback(t):
                instance.consumer_tasks.pop(f"{exchange_name}:{message.delivery_tag}", None)
                if t.exception():
                    instance.error(f"Task for message {message.delivery_tag} raised an exception: {t.exception()}")

            task.add_done_callback(task_done_callback)

        await queue.consume(on_message)
        instance.info(f"Listener registered for exchange '{exchange_name}' with routing_key '{routing_key}'")

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
            await RabbitMQService.connect()

        try:
            # Use or declare the exchange
            exchange_name = f"{instance.config.get_bot_name()}_{exchange_name}"
            if exchange_name not in instance.exchanges:
                exchange = await instance.channel.declare_exchange(
                    exchange_name, exchange_type, durable=True
                )
                instance.exchanges[exchange_name] = exchange
            else:
                exchange = instance.exchanges[exchange_name]

            json_message = message.to_json().encode()
            aio_message = aio_pika.Message(
                body=json_message,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            instance._notify_hooks(exchange=exchange_name,
                                   routing_key=routing_key or "",
                                   sender=message.sender,
                                   recipient=message.recipient,
                                   trading_configuration=message.trading_configuration,
                                   payload=message.payload,
                                   message_id=message.message_id,
                                   direction="outgoing")

            await exchange.publish(aio_message, routing_key=routing_key or "")
            instance.logger.info(f"Message {json_message} published to exchange '{exchange_name}' with routing_key '{routing_key}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.logger.error(f"Connection error during publishing: {e}")
            await RabbitMQService.connect()
            # Optionally, retry publishing the message here
        except Exception as e:
            instance.logger.error(f"Unexpected error during publishing: {e}")

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
            await RabbitMQService.connect()

        try:
            # Use or declare the queue
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(queue_name, durable=True)
                instance.queues[queue_name] = queue

            aio_message = aio_pika.Message(
                body=message.to_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await instance.channel.default_exchange.publish(aio_message, routing_key=queue_name)
            instance.logger.info(f"Message published directly to queue '{queue_name}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.logger.error(f"Connection error during queue publishing: {e}")
            await RabbitMQService.connect()
            # Optionally, retry publishing the message here
        except Exception as e:
            instance.logger.error(f"Unexpected error during queue publishing: {e}")

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
        Stops the RabbitMQ service safely.
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
                    instance.logger.error(f"Error cancelling consumer {consumer_tag}: {e}")
                finally:
                    await instance.consumer_tasks.pop(consumer_tag, None)
            instance.logger.info("All consumers have been cancelled.")

            await RabbitMQService.disconnect()
            instance.logger.info("RabbitMQ service stopped successfully.")
        except Exception as e:
            instance.logger.error(f"Error stopping RabbitMQ service: {e}")
        finally:
            instance.started = False
