import asyncio
import aio_pika
from aio_pika import ExchangeType
from typing import Callable, Optional, Dict, Any

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustExchange, AbstractRobustQueue

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
            bot_name: str,
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
            self.listeners: Dict[str, Any] = {}
            self.logger = BotLogger.get_logger(routine_label + "_RabbitMQ")
            self.bot_name = bot_name
            self.consumer_tasks: Dict[str, asyncio.Task] = {}
            self.exchanges: Dict[str, AbstractRobustExchange] = {}
            self.queues: Dict[str, AbstractRobustQueue] = {}
            self.started = False
            self.active_subscriptions = set()
            self.initialized = True

    @staticmethod
    @exception_handler
    async def connect():
        """
        Stabilisce la connessione e crea un canale.
        """
        instance = RabbitMQService._instance
        if instance:
            instance.connection = await aio_pika.connect_robust(
                instance.amqp_url,
                loop=instance.loop,
                heartbeat=60  # Aumentato per evitare timeout
            )
            instance.channel = await instance.connection.channel()
            await instance.channel.set_qos(prefetch_count=10)
            instance.logger.info("Connesso a RabbitMQ")

    @staticmethod
    @exception_handler
    async def disconnect():
        """
        Chiude la connessione a RabbitMQ.
        """
        instance = RabbitMQService._instance
        if instance:
            if instance.channel:
                await instance.channel.close()
                instance.channel = None
            if instance.connection:
                await instance.connection.close()
                instance.connection = None
            instance.logger.info("Disconnesso da RabbitMQ")

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
        Registra un listener per uno specifico exchange e routing key.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            raise RuntimeError("La connessione non è stabilita. Chiama connect() prima.")

        exchange_name = f"{instance.bot_name}_{exchange_name}"
        # Usa o dichiara l'exchange
        if exchange_name not in instance.exchanges:
            exchange = await instance.channel.declare_exchange(
                exchange_name, exchange_type, durable=True, auto_delete=False
            )
            instance.exchanges[exchange_name] = exchange
        else:
            exchange = instance.exchanges[exchange_name]

        # Usa o dichiara la coda
        if queue_name:
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(
                    queue_name, exclusive=False, durable=True, auto_delete=False
                )
                instance.queues[queue_name] = queue
            else:
                queue = instance.queues[queue_name]
        else:
            # Per code anonime, non possiamo memorizzarle per nome
            queue = await instance.channel.declare_queue(
                exclusive=True, durable=False, auto_delete=True
            )

        if exchange_type == ExchangeType.TOPIC:
            if not routing_key:
                raise ValueError("routing_key è richiesta per gli exchange di tipo 'topic'")
            await queue.bind(exchange, routing_key)
        elif exchange_type == ExchangeType.DIRECT:
            if not routing_key:
                raise ValueError("routing_key è richiesta per gli exchange di tipo 'direct'")
            await queue.bind(exchange, routing_key)
        else:
            await queue.bind(exchange)

        async def process_message(message: AbstractIncomingMessage):
            async with message.process():
                try:
                    rec_routing_key = message.routing_key
                    queue_message = QueueMessage.from_json(message.body.decode())
                    instance.logger.info(f"Messaggio ricevuto '{queue_message}' dall'exchange '{exchange_name}' con routing_key '{rec_routing_key}'")
                    await callback(rec_routing_key, queue_message)
                except Exception as e:
                    instance.logger.error(f"Errore nel processare il messaggio: {e}")
                    await message.reject(requeue=True)

        async def on_message(message: AbstractIncomingMessage) -> Any:
            task = asyncio.create_task(process_message(message))
            instance.consumer_tasks[f"{exchange_name}:{message.delivery_tag}"] = task

            def task_done_callback(t):
                instance.consumer_tasks.pop(f"{exchange_name}:{message.delivery_tag}", None)
                if t.exception():
                    instance.logger.error(f"Task per il messaggio {message.delivery_tag} ha generato un'eccezione: {t.exception()}")

            task.add_done_callback(task_done_callback)

        await queue.consume(on_message)
        instance.logger.info(f"Listener registrato per l'exchange '{exchange_name}' con routing_key '{routing_key}'")

    @staticmethod
    @exception_handler
    async def publish_message(
            exchange_name: str,
            message: QueueMessage,
            routing_key: Optional[str] = None,
            exchange_type: ExchangeType = ExchangeType.FANOUT
    ):
        """
        Pubblica un messaggio su uno specifico exchange.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            await RabbitMQService.connect()

        try:
            # Usa o dichiara l'exchange
            exchange_name = f"{instance.bot_name}_{exchange_name}"
            if exchange_name not in instance.exchanges:
                exchange = await instance.channel.declare_exchange(
                    exchange_name, exchange_type, durable=True
                )
                instance.exchanges[exchange_name] = exchange
            else:
                exchange = instance.exchanges[exchange_name]

            json_message = message.to_json().encode()
            aio_message = aio_pika.Message(body=json_message)

            await exchange.publish(aio_message, routing_key=routing_key or "")
            instance.logger.info(f"Messaggio {json_message} pubblicato sull'exchange '{exchange_name}' con routing_key '{routing_key}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.logger.error(f"Errore di connessione durante la pubblicazione: {e}")
            await RabbitMQService.connect()
            # Opzionalmente, ritenta la pubblicazione del messaggio qui
        except Exception as e:
            instance.logger.error(f"Errore inaspettato durante la pubblicazione: {e}")

    @staticmethod
    @exception_handler
    async def publish_to_queue(
            queue_name: str,
            message: QueueMessage
    ):
        """
        Pubblica un messaggio direttamente su una specifica coda.
        """
        instance = RabbitMQService._instance
        if not instance.channel:
            await RabbitMQService.connect()

        try:
            # Usa o dichiara la coda
            if queue_name not in instance.queues:
                queue = await instance.channel.declare_queue(queue_name, durable=True)
                instance.queues[queue_name] = queue

            aio_message = aio_pika.Message(body=message.to_json().encode())
            await instance.channel.default_exchange.publish(aio_message, routing_key=queue_name)
            instance.logger.info(f"Messaggio pubblicato direttamente sulla coda '{queue_name}'")
        except aio_pika.exceptions.AMQPConnectionError as e:
            instance.logger.error(f"Errore di connessione durante la pubblicazione sulla coda: {e}")
            await RabbitMQService.connect()
            # Opzionalmente, ritenta la pubblicazione del messaggio qui
        except Exception as e:
            instance.logger.error(f"Errore inaspettato durante la pubblicazione sulla coda: {e}")

    @staticmethod
    @exception_handler
    async def start():
        """
        Avvia il servizio RabbitMQ stabilendo una connessione.
        """
        instance = RabbitMQService._instance
        if instance.started:
            raise RuntimeError("Il servizio RabbitMQ è già avviato.")
        await RabbitMQService.connect()
        instance.started = True

    @staticmethod
    @exception_handler
    async def stop():
        """
        Ferma il servizio RabbitMQ in modo sicuro.
        """
        instance = RabbitMQService._instance
        try:
            for consumer_tag, task in list(instance.consumer_tasks.items()):
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    instance.logger.info(f"Consumer {consumer_tag} annullato.")
                except Exception as e:
                    instance.logger.error(f"Errore durante l'annullamento del consumer {consumer_tag}: {e}")
                finally:
                    await instance.consumer_tasks.pop(consumer_tag, None)
            instance.logger.info("Tutti i consumer sono stati annullati.")

            await RabbitMQService.disconnect()
            instance.logger.info("Servizio RabbitMQ fermato con successo.")
        except Exception as e:
            instance.logger.error(f"Errore durante l'arresto del servizio RabbitMQ: {e}")
        finally:
            instance.started = False
