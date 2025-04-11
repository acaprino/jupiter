import asyncio
from typing import Callable, Optional, Dict, Any, List, Literal

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustExchange, AbstractRobustQueue, AbstractRobustConnection, AbstractRobustChannel

from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin

HookType = Callable[[str, str, str, str, str], None]


class RabbitMQService(LoggingMixin):
    _instance: Optional['RabbitMQService'] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use class_name.get_instance() instead")
        return super().__new__(cls)

    @classmethod
    async def get_instance(cls, *args, **kwargs) -> 'RabbitMQService':
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls(*args, **kwargs)
            return cls._instance

    def __init__(
            self,
            config: ConfigReader,
            user: str,
            password: str,
            rabbitmq_host: str,
            port: Optional[int] = None,
            vhost: Optional[str] = None,
            ssl: Optional[bool] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        if not hasattr(self, 'initialized'):
            super().__init__(config)
            protocol = "amqps" if ssl else "amqp"

            port_str = f":{port}" if port is not None else ""
            vhost_str = f"/{vhost}" if vhost is not None else ""

            self.amqp_url = f"{protocol}://{user}:{password}@{rabbitmq_host}{port_str}{vhost_str}"
            self.loop = loop or asyncio.get_event_loop()
            self.connection: AbstractRobustConnection | None = None
            self.channel: AbstractRobustChannel | None = None
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
    async def register_hook(hook: HookType) -> None:
        instance = await RabbitMQService.get_instance()
        instance._hooks.append(hook)

    @staticmethod
    async def _notify_hooks(exchange: str,
                            routing_key: str,
                            body: str,
                            message_id: str,
                            direction: Literal["incoming", "outgoing"]) -> None:
        instance = await RabbitMQService.get_instance()
        for hook in instance._hooks:
            try:
                hook(exchange, routing_key, body, message_id, direction)
            except Exception as e:
                instance.error(f"Error while calling callback: {e}", exec_info=e)

    @staticmethod
    @exception_handler
    async def connect():
        """
        Establishes the connection and creates a channel.
        """
        instance = await RabbitMQService.get_instance()
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
        instance = await RabbitMQService.get_instance()
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
        instance = await RabbitMQService.get_instance()
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

        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())

                    instance.debug(f"Calling callback {callback}")
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    instance.error(f"Error processing message: {e}", exec_info=e)
                    await message.reject(requeue=True)

        async def on_message(message: AbstractIncomingMessage) -> Any:
            obj_body_str = message.body.decode()

            instance.info(f"Incoming message \"{obj_body_str}\"")

            await instance._notify_hooks(exchange=message.exchange,
                                         routing_key=message.routing_key,
                                         body=obj_body_str,
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
    async def unregister_listener(
            exchange_name: str,
            routing_key: Optional[str] = None,
            queue_name: Optional[str] = None
    ):
        """
        Unregisters a listener for a specific exchange and routing key.
        """
        instance = await RabbitMQService.get_instance()
        if not instance.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        exchange_name = f"{instance.config.get_bot_name()}_{exchange_name}"

        # Ensure the exchange exists
        if exchange_name not in instance.exchanges:
            instance.warning(f"Exchange '{exchange_name}' not found.")
            return

        exchange = instance.exchanges[exchange_name]

        # Ensure the queue exists
        if queue_name and queue_name not in instance.queues:
            instance.warning(f"Queue '{queue_name}' not found.")
            return

        queue = instance.queues.get(queue_name)

        # Cancel the consumer tasks associated with this exchange and queue
        for consumer_tag, task in list(instance.consumer_tasks.items()):
            if consumer_tag.startswith(f"{exchange_name}:"):
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    instance.info(f"Consumer {consumer_tag} cancelled.")
                except Exception as e:
                    instance.error(f"Error cancelling consumer {consumer_tag}: {e}", exec_info=e)
                finally:
                    await instance.consumer_tasks.pop(consumer_tag, None)

        # Unbind the queue from the exchange
        if queue:
            await queue.unbind(exchange, routing_key or "")

        instance.info(f"Listener unregistered for exchange '{exchange_name}' with routing_key '{routing_key}'")

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
        instance = await RabbitMQService.get_instance()
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

            json_message = message.to_json()
            aio_message = aio_pika.Message(
                body=json_message.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await instance._notify_hooks(exchange=exchange_name,
                                         routing_key=routing_key,
                                         body=json_message,
                                         message_id=message.message_id,
                                         direction="outgoing")

            instance.info(f"Publishing message {json_message} to exchange '{exchange_name}' with routing_key '{routing_key}'")
            await exchange.publish(aio_message, routing_key=routing_key or "")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.error(f"Connection error during publishing: {e}", exec_info=e)
            await RabbitMQService.connect()
            # Optionally, retry publishing the message here
        except Exception as e:
            instance.error(f"Unexpected error during publishing: {e}", exec_info=e)

    @staticmethod
    @exception_handler
    async def publish_to_queue(
            queue_name: str,
            message: QueueMessage
    ):
        """
        Publishes a message directly to a specific queue.
        """
        instance = await RabbitMQService.get_instance()
        if not instance.channel:
            await RabbitMQService.connect()

        try:
            # Use or declare the queue
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(
                    queue_name, durable=True, exclusive=False
                )
                instance.queues[queue_name] = queue

            aio_message = aio_pika.Message(
                body=message.to_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await instance.channel.default_exchange.publish(aio_message, routing_key=queue_name)
            instance.info(f"Message published directly to queue '{queue_name}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.error(f"Connection error during queue publishing: {e}", exec_info=e)
            await RabbitMQService.connect()
            # Optionally, retry publishing the message here
        except Exception as e:
            instance.error(f"Unexpected error during queue publishing: {e}", exec_info=e)

    @staticmethod
    @exception_handler
    async def start():
        """
        Starts the RabbitMQ service by establishing a connection.
        """
        instance = await RabbitMQService.get_instance()
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
        instance = await RabbitMQService.get_instance()
        try:
            for consumer_tag, task in list(instance.consumer_tasks.items()):
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    instance.info(f"Consumer {consumer_tag} cancelled.")
                except Exception as e:
                    instance.error(f"Error cancelling consumer {consumer_tag}: {e}", exec_info=e)
                finally:
                    await instance.consumer_tasks.pop(consumer_tag, None)
            instance.info("All consumers have been cancelled.")

            await RabbitMQService.disconnect()
            instance.info("RabbitMQ service stopped successfully.")
        except Exception as e:
            instance.error(f"Error stopping RabbitMQ service: {e}", exec_info=e)
        finally:
            instance.started = False
