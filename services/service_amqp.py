import asyncio
import json

import aio_pika

from typing import Callable, Optional, Dict, Any, List, Literal
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustExchange, AbstractRobustQueue, AbstractRobustConnection, AbstractRobustChannel
from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin

HookType = Callable[[str, str, str, str, str], None]


class AMQPService(LoggingMixin):
    _instance: Optional['AMQPService'] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        # Prevent direct instantiation if already initialized
        if cls._instance is not None:
            raise RuntimeError("Use class_name.get_instance() instead")
        return super().__new__(cls)

    @classmethod
    async def get_instance(cls, *args, **kwargs) -> 'AMQPService':
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls(*args, **kwargs)
            return cls._instance

    def __init__(
            self,
            config: ConfigReader,
            user: str,
            password: str,
            amqp_host: str,
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

            self.amqp_url = f"{protocol}://{user}:{password}@{amqp_host}{port_str}{vhost_str}"
            self.loop = loop or asyncio.get_event_loop()
            self.connection: AbstractRobustConnection | None = None
            self.channel: AbstractRobustChannel | None = None
            self.listeners: Dict[str, Any] = {}
            self.config = config
            self.agent = "AMQP-Service"
            self.consumer_tasks: Dict[str, asyncio.Task] = {}
            self.exchanges: Dict[str, AbstractRobustExchange] = {}
            self.queues: Dict[str, AbstractRobustQueue] = {}
            self.started = False
            self.active_subscriptions = set()
            self._hooks: List[HookType] = []

            # Default Declaration Parameters
            # Exchanges: Durable, not auto-deleted (persist even if no consumers/bindings)
            self.default_exchange_durable: bool = True
            self.default_exchange_auto_delete: bool = False

            # Queues: Durable, not auto-deleted, not exclusive (can be shared)
            self.default_queue_durable: bool = False
            self.default_queue_auto_delete: bool = True
            self.default_queue_exclusive: bool = False

            self.initialized = True # Mark as initialized

    @staticmethod
    async def register_hook(hook: HookType) -> None:
        instance = await AMQPService.get_instance()
        instance._hooks.append(hook)

    @staticmethod
    async def _notify_hooks(exchange: str,
                            routing_key: str,
                            body: str,
                            message_id: str,
                            direction: Literal["incoming", "outgoing"]) -> None:
        instance = await AMQPService.get_instance()
        for hook in instance._hooks:
            try:
                hook(exchange, routing_key, body, message_id, direction)
            except Exception as e:
                instance.error(f"Error while calling callback: {e}", exc_info=e)

    @staticmethod
    @exception_handler
    async def connect():
        """
        Establishes the connection and creates a channel.
        """
        instance = await AMQPService.get_instance()
        if instance:
            instance.connection = await aio_pika.connect_robust(
                instance.amqp_url,
                loop=instance.loop,
                heartbeat=60  # Increased to avoid timeout
            )
            instance.channel = await instance.connection.channel()
            await instance.channel.set_qos(prefetch_count=10)
            instance.info("Connected to AMQP")

    @staticmethod
    @exception_handler
    async def disconnect():
        """
        Closes the connection to AMQP.
        """
        instance = await AMQPService.get_instance()
        if instance:
            if instance.channel:
                await instance.channel.close()
                instance.channel = None
            if instance.connection:
                await instance.connection.close()
                instance.connection = None
            instance.info("Disconnected from AMQP")

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
        Uses consistent declaration parameters defined in __init__.
        """
        instance = await AMQPService.get_instance()
        if not instance.channel:
            raise RuntimeError("Connection is not established. Call connect() first.")

        exchange_name = f"{instance.config.get_bot_name()}_{exchange_name}"
        # Use or declare the exchange
        if exchange_name not in instance.exchanges:
            exchange = await instance.channel.declare_exchange(
                exchange_name,
                exchange_type,
                durable=instance.default_exchange_durable,         # Use instance default
                auto_delete=instance.default_exchange_auto_delete  # Use instance default
            )
            instance.exchanges[exchange_name] = exchange
        else:
            exchange = instance.exchanges[exchange_name]

        # Use or declare the queue
        if queue_name:
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(
                    queue_name,
                    exclusive=instance.default_queue_exclusive,    # Use instance default
                    durable=instance.default_queue_durable,       # Use instance default
                    auto_delete=instance.default_queue_auto_delete # Use instance default
                )
                instance.queues[queue_name] = queue
            else:
                queue = instance.queues[queue_name]
        else:
            # For anonymous queues, use the same defaults for consistency
            # Although they are inherently temporary, using consistent defaults avoids potential issues
            # if the same pattern were somehow used elsewhere with naming.
            queue = await instance.channel.declare_queue(
                exclusive=instance.default_queue_exclusive,    # Use instance default
                durable=instance.default_queue_durable,       # Use instance default
                auto_delete=instance.default_queue_auto_delete # Use instance default
            )

        # Bind queue based on exchange type
        if exchange_type in [ExchangeType.TOPIC, ExchangeType.DIRECT]:
            if not routing_key:
                raise ValueError(f"routing_key is required for '{exchange_type.value}' exchanges")
            await queue.bind(exchange, routing_key)
        else: # FANOUT or HEADERS
            await queue.bind(exchange) # No routing key needed/used for FANOUT

        # Message Processing Logic (unchanged)
        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())
                    instance.debug(f"Calling callback {callback}")
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    instance.error(f"Error processing message: {e}", exc_info=e)
                    await message.reject(requeue=False) # Reject without requeue on processing error

        async def on_message(message: AbstractIncomingMessage) -> Any:
            obj_body_str = message.body.decode()
            instance.info(f"Incoming message \"{obj_body_str}\"")
            try:
                message_internal_id = json.loads(obj_body_str).get('message_id')
            except Exception:
                message_internal_id = 'na'
            await instance._notify_hooks(exchange=message.exchange,
                                         routing_key=message.routing_key,
                                         body=obj_body_str,
                                         message_id=message_internal_id,
                                         direction="incoming")
            task = asyncio.create_task(process_message(message))
            consumer_tag = f"{exchange_name}:{queue_name or 'anon'}:{message.delivery_tag}"
            instance.consumer_tasks[consumer_tag] = task

            def task_done_callback(t):
                instance.consumer_tasks.pop(consumer_tag, None)
                if t.exception():
                    instance.error(f"Task for consumer {consumer_tag} raised an exception: {t.exception()}")

            task.add_done_callback(task_done_callback)

        # Consume messages from the queue
        consumer_tag = await queue.consume(on_message)
        instance.active_subscriptions.add(consumer_tag) # Track active consumer tags
        instance.info(f"Listener registered for exchange '{exchange_name}' queue '{queue.name}' with routing_key '{routing_key}' (Consumer Tag: {consumer_tag})")
        return consumer_tag # Return the consumer tag for potential unregistration

    @staticmethod
    @exception_handler
    async def unregister_listener(consumer_tag: str):
        """
        Unregisters a listener using its consumer tag.
        """
        instance = await AMQPService.get_instance()
        if not instance.channel:
            raise RuntimeError("Connection is not established.")

        if consumer_tag not in instance.active_subscriptions:
            instance.warning(f"No active subscription found for consumer tag '{consumer_tag}'.")
            return

        try:
            # Find the queue associated with the consumer tag (might require iterating queues if not directly mapped)
            # For simplicity, we assume the queue object is available or can be retrieved if needed.
            # aio_pika's queue object has a cancel method.
            # Find the queue name from the consumer_tag if possible (e.g., from instance.consumer_tasks keys)
            queue_name = None
            for key in instance.consumer_tasks.keys():
                if key.endswith(f":{consumer_tag.split(':')[-1]}"): # Match delivery tag part
                    parts = key.split(':')
                    if len(parts) >= 3:
                        queue_name = parts[1] if parts[1] != 'anon' else None
                        break

            if queue_name and queue_name in instance.queues:
                 queue = instance.queues[queue_name]
                 await queue.cancel(consumer_tag)
                 instance.info(f"Cancelled consumer with tag '{consumer_tag}' on queue '{queue_name}'.")
            # If queue_name is None or not found, we might need a more robust way
            # to map consumer_tag back to its queue, or rely on channel-level cancellation if available.
            # aio_pika focuses on queue.cancel(consumer_tag).
            else:
                 # Fallback or alternative method if direct queue mapping isn't easy
                 # This might involve iterating through all queues or using channel.basic_cancel
                 # For now, log a warning if queue isn't found directly
                 instance.warning(f"Could not directly find queue for consumer tag '{consumer_tag}' to cancel. Manual cleanup might be needed or rely on task cancellation.")


            # Cancel the associated task if it exists
            task = instance.consumer_tasks.pop(consumer_tag, None)
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    instance.debug(f"Task for consumer {consumer_tag} cancelled.")
                except Exception as e:
                    instance.error(f"Error cancelling task for consumer {consumer_tag}: {e}", exc_info=e)

            instance.active_subscriptions.remove(consumer_tag)
            instance.info(f"Listener unregistered for consumer tag '{consumer_tag}'")

        except Exception as e:
            instance.error(f"Error during unregister_listener for tag '{consumer_tag}': {e}", exc_info=e)


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
        Uses consistent declaration parameters defined in __init__.
        """
        instance = await AMQPService.get_instance()
        if not instance.channel:
            await AMQPService.connect() # Reconnect if channel is missing

        try:
            # Use or declare the exchange
            exchange_name = f"{instance.config.get_bot_name()}_{exchange_name}"
            if exchange_name not in instance.exchanges:
                exchange = await instance.channel.declare_exchange(
                    exchange_name,
                    exchange_type,
                    durable=instance.default_exchange_durable,         # Use instance default
                    auto_delete=instance.default_exchange_auto_delete  # Use instance default
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
            instance.error(f"Connection error during publishing: {e}", exc_info=e)
            await AMQPService.connect() # Attempt to reconnect
            # Optionally, retry publishing the message here after reconnect
            instance.warning("Connection lost during publish. Attempted reconnect. Message might need resending.")
        except Exception as e:
            instance.error(f"Unexpected error during publishing: {e}", exc_info=e)

    @staticmethod
    @exception_handler
    async def publish_to_queue(
            queue_name: str,
            message: QueueMessage
    ):
        """
        Publishes a message directly to a specific queue.
        Uses consistent declaration parameters defined in __init__.
        """
        instance = await AMQPService.get_instance()
        if not instance.channel:
            await AMQPService.connect() # Reconnect if channel is missing

        try:
            # Use or declare the queue
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(
                    queue_name,
                    durable=instance.default_queue_durable,       # Use instance default
                    exclusive=instance.default_queue_exclusive,    # Use instance default
                    auto_delete=instance.default_queue_auto_delete # Use instance default
                )
                instance.queues[queue_name] = queue

            aio_message = aio_pika.Message(
                body=message.to_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            # Publishing to default exchange with routing_key=queue_name sends to that queue
            await instance.channel.default_exchange.publish(aio_message, routing_key=queue_name)
            instance.info(f"Message published directly to queue '{queue_name}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.error(f"Connection error during queue publishing: {e}", exc_info=e)
            await AMQPService.connect() # Attempt to reconnect
            instance.warning("Connection lost during publish to queue. Attempted reconnect. Message might need resending.")
        except Exception as e:
            instance.error(f"Unexpected error during queue publishing: {e}", exc_info=e)

    @staticmethod
    @exception_handler
    async def start():
        """
        Starts the AMQP service by establishing a connection.
        """
        instance = await AMQPService.get_instance()
        if instance.started:
            # Log warning instead of raising error for idempotency
            instance.warning("AMQP service start() called but already started.")
            return
        await AMQPService.connect()
        instance.started = True

    @staticmethod
    @exception_handler
    async def stop():
        """
        Safely stops the AMQP service by:
          1) cancelling all active consumers via consumer tags;
          2) optionally deleting declared queues (consider if this is desired);
          3) closing the channel and connection.
        """
        instance = await AMQPService.get_instance()
        if not instance.started:
            instance.warning("AMQP service stop() called but not started.")
            return

        try:
            # 1) Cancel all active consumers using stored consumer tags
            instance.info(f"Cancelling {len(instance.active_subscriptions)} active consumers...")
            tasks_to_wait = []
            for consumer_tag in list(instance.active_subscriptions):
                 # Use the unregister_listener logic which includes task cancellation
                 await AMQPService.unregister_listener(consumer_tag)
                 # Collect task if cancellation was initiated inside unregister_listener
                 task = instance.consumer_tasks.get(consumer_tag) # Check if task still exists
                 if task:
                    tasks_to_wait.append(task)

            # Wait for any remaining associated tasks to finish cancellation
            if tasks_to_wait:
                await asyncio.gather(*tasks_to_wait, return_exceptions=True)

            instance.info("All active consumers have been cancelled.")
            instance.consumer_tasks.clear() # Clear any stragglers
            instance.active_subscriptions.clear()

            # 2) Optionally delete queues (Be careful with this in production)
            # If queues should persist across restarts, comment this section out.
            # If they are temporary/specific to this run, keep it.
            instance.info(f"Deleting {len(instance.queues)} declared queues...")
            for queue_name, queue in list(instance.queues.items()):
                try:
                    # Check if queue still exists before deleting
                    # Note: This check might not be perfectly reliable due to async nature
                    # await instance.channel.queue_declare(queue_name, passive=True) # Check existence
                    await queue.delete(if_unused=False, if_empty=False)
                    instance.info(f"Queue '{queue_name}' has been deleted.")
                except aio_pika.exceptions.ChannelClosed as e:
                     instance.warning(f"Channel closed while trying to delete queue '{queue_name}': {e}")
                except Exception as e:
                    # Log error but continue trying to delete others
                    instance.error(f"Error deleting queue '{queue_name}': {e}", exc_info=e)
                finally:
                    # Remove from internal tracking regardless of deletion success
                    instance.queues.pop(queue_name, None)
            instance.exchanges.clear() # Clear exchange cache too

            # 3) Disconnect channel and connection
            await AMQPService.disconnect()
            instance.info("AMQP service has been stopped successfully.")

        except Exception as e:
            instance.error(f"Unexpected error during stop(): {e}", exc_info=e)
        finally:
            # Mark the service as no longer started
            instance.started = False
            # Clear caches
            instance.queues.clear()
            instance.exchanges.clear()
            instance.consumer_tasks.clear()
            instance.active_subscriptions.clear()