import asyncio
import threading
from typing import TypeVar, Generic, Optional, Type, Dict

from misc_utils.error_handler import exception_handler

T = TypeVar('T')  # Generic type for the broker implementation


class Broker(Generic[T]):
    """
    Un proxy thread-safe che implementa il pattern singleton.
    Gestisce automaticamente l'inizializzazione e la configurazione del broker.
    """
    _instance: Optional['Broker'] = None
    _lock = threading.Lock()

    def __new__(cls) -> 'Broker':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._broker_instance = None
                    cls._instance.async_lock = asyncio.Lock()
        return cls._instance

    @exception_handler
    async def initialize(self, broker_class: Type[T], agent: str, configuration: Dict) -> 'Broker':
        """
        Inizializza il broker con i parametri forniti.
        Se il broker è già inizializzato, solleva un'eccezione.
        """
        if self._broker_instance is not None:
            raise Exception("Broker is already initialized")

        async with self.async_lock:
            self._broker_instance = broker_class()
            self._broker_instance.configure(agent, configuration)
            await self._broker_instance.startup()
            return self

    @property
    def is_initialized(self) -> bool:
        """Controlla se il broker è stato inizializzato"""
        return self._broker_instance is not None

    def __getattr__(self, name):
        """
        Proxy automatico per tutti i metodi del broker.
        Aggiunge thread-safety automaticamente.
        """
        if not self.is_initialized:
            raise Exception("Broker not initialized. Call initialize() first")

        attr = getattr(self._broker_instance, name)

        if callable(attr):
            async def proxy_wrapper(*args, **kwargs):
                async with self.async_lock:
                    return await attr(*args, **kwargs)

            return proxy_wrapper

        return attr
