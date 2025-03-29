from datetime import timedelta
from typing import Optional, List
import asyncio

from dto.Signal import Signal
from misc_utils.config import ConfigReader
from misc_utils.enums import TradingDirection, Timeframe
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc, dt_to_unix, to_serializable
from services.service_mongodb import MongoDBService


class SignalPersistenceService(LoggingMixin):
    """Singleton thread-safe e async-safe per la persistenza dei signal su MongoDB."""

    _instance: Optional['SignalPersistenceService'] = None
    _instance_lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            raise RuntimeError("Utilizza SignalPersistenceService.get_instance() per ottenere l'istanza")
        return super().__new__(cls)

    def __init__(self, config: ConfigReader):
        if getattr(self, '_initialized', False):
            return

        super().__init__(config)
        self.agent = "SignalPersistenceService"
        self.config = config

        # Lock per garantire sicurezza nelle operazioni asincrone
        self._async_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        # Event per segnalare che la connessione al DB è pronta
        self._db_ready = asyncio.Event()

        # Attributi per la connessione al DB
        self.db_service = None
        self.collection = None
        self.collection_name = "signals"

        self._initialized = True
        self._async_initialized = False

        # Avvia l'inizializzazione della connessione in background
        asyncio.create_task(self._init_db_connection())

    @classmethod
    async def get_instance(cls, config: ConfigReader) -> 'SignalPersistenceService':
        """
        Restituisce l'istanza singleton.
        La connessione al DB viene inizializzata in background.
        """
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = SignalPersistenceService(config)
            return cls._instance

    async def _init_db_connection(self):
        async with self._start_lock:
            if not self._async_initialized:
                try:
                    db_name = self.config.get_mongo_db_name()
                    host = self.config.get_mongo_host()
                    port = self.config.get_mongo_port()
                    username = self.config.get_mongo_username()
                    password = self.config.get_mongo_password()

                    self.db_service = MongoDBService(
                        config=self.config,
                        host=host,
                        port=port,
                        username=username,
                        password=password,
                        db_name=db_name
                    )

                    # Avvia la connessione e crea l'indice
                    await self.start()
                    self._async_initialized = True
                    self._db_ready.set()
                except Exception as e:
                    self.critical("Fallita l'inizializzazione della connessione al DB", exec_info=e)
                    raise

    async def _ensure_db_ready(self):
        """Assicura che la connessione al DB sia pronta prima di procedere."""
        await self._db_ready.wait()

    @exception_handler
    async def save_signal(self, signal: Signal) -> bool:
        """
        Salva (o upserisce) un signal nel DB in base al signal_id.
        """
        await self._ensure_db_ready()
        payload = to_serializable(signal)
        try:
            async with self._async_lock:
                await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=payload
                )
            self.info(f"Signal {signal.signal_id} salvato correttamente.")
            return True
        except Exception as e:
            self.critical(f"Errore nel salvataggio del signal {signal.signal_id}", exec_info=e)
            return False

    @exception_handler
    async def update_signal_status(self, signal: Signal) -> bool:
        """
        Aggiorna lo stato del signal individuato da signal_id.
        """
        await self._ensure_db_ready()
        try:
            async with self._async_lock:
                result = await self.db_service.upsert(
                    collection=self.collection_name,
                    id_object={"signal_id": signal.signal_id},
                    payload=to_serializable(signal)
                )
            if result and len(result) > 0:
                self.info(f"Signal {signal.signal_id} aggiornato a stato: {signal.confirmed}.")
                return True
            else:
                self.error(f"Signal {signal.signal_id} non trovato.", exec_info=False)
                return False
        except Exception as e:
            self.critical(f"Errore nell'aggiornamento del signal {signal.signal_id}", exec_info=e)
            return False

    @exception_handler
    async def retrieve_active_signals(
        self,
        symbol: str,
        timeframe: Timeframe,
        direction: TradingDirection,
        agent: Optional[str] = None
    ) -> List[Signal]:
        """
        Restituisce tutti i signal attivi (con candle_close_time > soglia calcolata).
        """
        await self._ensure_db_ready()
        try:
            # Calcola una soglia basata sul timeframe
            threshold = dt_to_unix(now_utc() - timedelta(seconds=timeframe.to_seconds()))
            find_filter = {
                "symbol": symbol,
                "timeframe": timeframe.name,
                "direction": direction.name,
                "candle.time_close": {"$gt": threshold},
                "candle.time_open": {"$lt": threshold}
            }
            if agent:
                find_filter["agent"] = agent

            async with self._async_lock:
                documents = await self.db_service.find_many(
                    collection=self.collection_name,
                    filter=find_filter
                )
            signals = [Signal.from_json(doc) for doc in documents] if documents else []
            return signals
        except Exception as e:
            self.error("Errore nel recupero dei signal attivi", exec_info=e)
            return []

    @exception_handler
    async def get_signal(self, signal_id: str) -> Optional[Signal]:
        """
        Recupera un signal dal DB in base al signal_id.
        """
        await self._ensure_db_ready()
        try:
            async with self._async_lock:
                document = await self.db_service.find_one(
                    collection=self.collection_name,
                    filter={"signal_id": signal_id}
                )
            if document:
                return Signal.from_json(document)
            return None
        except Exception as e:
            self.error(f"Errore nel recupero del signal {signal_id}", exec_info=e)
            return None

    @exception_handler
    async def start(self):
        """
        Inizializza la connessione al DB e crea l'indice sul campo signal_id.
        """
        try:
            await self.db_service.connect()
            if not await self.db_service.test_connection():
                raise Exception("Impossibile connettersi all'istanza MongoDB.")
            self.collection = await self.db_service.create_index(
                collection=self.collection_name,
                index_field="signal_id",
                unique=True
            )
            self.info("SignalPersistenceManager avviato. Indice creato su 'signal_id'.")
        except Exception as e:
            self.critical("Fallito l'avvio di SignalPersistenceManager", exec_info=e)
            raise

    @exception_handler
    async def stop(self):
        """
        Chiude la connessione al DB.
        """
        await self._ensure_db_ready()
        async with self._async_lock:
            await self.db_service.disconnect()
            self._async_initialized = False
            self._db_ready.clear()
            self.info("SignalPersistenceManager fermato.")
