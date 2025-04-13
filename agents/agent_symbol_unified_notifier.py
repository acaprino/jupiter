# agents/agent_symbol_unified_notifier.py
import uuid
from typing import List, Optional

from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import RabbitExchange  # Assicurati che BROADCAST_NOTIFICATIONS sia qui
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from services.service_rabbitmq import RabbitMQService


class SymbolUnifiedNotifier(LoggingMixin): # Non serve più ABC qui

    def __init__(self, agent: str, config: ConfigReader, trading_configs: List[TradingConfiguration]):
        """
        Inizializza l'agente notificatore. La registrazione è gestita da classi superiori
        (es. RegistrationAwareAgent). Questo si concentra sull'invio di richieste di broadcast.
        """
        super().__init__(config)
        self.id = str(uuid.uuid4()) # L'ID dovrebbe venire da RegistrationAwareAgent
        self.agent = agent # Il nome specifico dell'agente (es. "Market state notifier agent")
        self.config = config
        self.trading_configs = trading_configs
        # Raggruppa simboli per logica interna se necessario, non per registrazione
        self.symbols = {config.symbol for config in self.trading_configs}
        self.rabbitmq_s = None
        # Rimuovi:
        # self.all_clients_registered_event = asyncio.Event()
        # self.clients_registrations = defaultdict(dict)
        # self.symbols_to_telegram_configs = defaultdict(dict)

    # --- METODI DI REGISTRAZIONE RIMOSSI ---
    # Rimuovi: group_configs_by_symbol (se usato solo per registrazione)
    # Rimuovi: routine_start (la registrazione avviene altrove)
    # Rimuovi: routine_stop (la deregistrazione avviene altrove)
    # Rimuovi: register_clients_for_symbol
    # Rimuovi: register_single_client
    # Rimuovi: on_client_registration_ack
    # Rimuovi: registration_ack (non più abstract o necessario qui)
    # Rimuovi: wait_client_registration

    async def routine_start(self):
        """
        Start the routine to register clients for all symbols and configurations.
        """
        self.info("Starting agent for client registration.")
        self.rabbitmq_s = await RabbitMQService.get_instance()

        await self.start()

    async def routine_stop(self):
        """
        Start the routine to register clients for all symbols and configurations.
        """
        self.info("Stopping agent for client registration.")
        await self.stop()

    @exception_handler
    async def request_broadcast_notification(self, message_content: str, symbol: str, notification_type: str = "general"):
        """
        Invia una singola richiesta al Middleware per inoltrare la notifica
        a tutti i client interessati al simbolo specificato.
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        self.info(f"Requesting broadcast for {notification_type} notification on symbol '{symbol}': {message_content[:50]}...")

        payload = {
            "target_symbol": symbol,
            "message": message_content,
            "notification_type": notification_type # Utile per il Middleware per routing/logging
        }

        # Usa un routing key specifico per il tipo e simbolo
        # Esempio: "notification.general.EURUSD", "market.state.EURUSD"
        routing_key = f"{symbol}"

        # L'ID dell'agente (se disponibile da RegistrationAwareAgent)
        agent_id = getattr(self, 'id', self.agent) # Usa l'ID univoco se esiste

        # Invia al nuovo exchange TOPIC
        await self.send_queue_message(
            exchange=RabbitExchange.BROADCAST_NOTIFICATIONS, # Nuovo exchange
            payload=payload,
            routing_key=routing_key,
            sender_id=agent_id # Passa l'ID dell'agente
        )

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: dict,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = "middleware", # Default a middleware
                                 sender_id: Optional[str] = None): # Aggiunto sender_id
        """
        Metodo helper per inviare messaggi alla coda RabbitMQ.
        """
        if self.rabbitmq_s is None:
            self.rabbitmq_s = await RabbitMQService.get_instance()

        sender = sender_id or self.agent # Usa l'ID univoco se fornito, altrimenti il nome generico

        # Crea un contesto trading_configuration minimale per il messaggio
        # Potrebbe essere migliorato se l'agente ha accesso alla sua config specifica
        symbol_in_payload = payload.get("target_symbol")
        tc = {"symbol": symbol_in_payload, "bot_name": self.config.get_bot_name()}

        q_message = QueueMessage(
            sender=sender,
            payload=payload,
            recipient=recipient,
            trading_configuration=tc
        )

        self.info(f"Sending message to exchange '{exchange.name}' (RK: {routing_key}): {q_message}")
        await self.rabbitmq_s.publish_message(
            exchange_name=exchange.name,
            message=q_message,
            routing_key=routing_key,
            exchange_type=exchange.exchange_type
        )

    # Il vecchio metodo viene sostituito dalla chiamata a request_broadcast_notification
    @exception_handler
    async def send_message_to_all_clients_for_symbol(self, message: str, symbol: str):
        """
        Metodo wrapper per inviare una notifica broadcast generale per un simbolo.
        """
        await self.request_broadcast_notification(message, symbol, notification_type="general")

    # Metodi abstract start/stop devono essere implementati dalle sottoclassi
    # se SymbolUnifiedNotifier non è più abstract
    # @abstractmethod
    async def start(self):
        """Le sottoclassi implementano la loro logica di start (senza registrazione qui)."""
        pass # Implementato da sottoclassi

    # @abstractmethod
    async def stop(self):
        """Le sottoclassi implementano la loro logica di stop (senza deregistrazione qui)."""
        pass # Implementato da sottoclassi