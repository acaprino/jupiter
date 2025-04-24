import asyncio
import datetime
import logging
from datetime import timezone
from typing import Optional, Dict, Type

from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.logger_mixing import LoggingMixin
from services.service_mongodb import MongoDBService

try:
    from pymongo.results import UpdateResult
except ImportError:
    class UpdateResult:
        def __init__(self, upserted_id=None, modified_count=0, matched_count=0):
            self.upserted_id = upserted_id
            self.modified_count = modified_count
            self.matched_count = matched_count

AGENT_STATE_COLLECTION = "agent_states"


class AdrasteaGeneratorStateManager(LoggingMixin):
    """
    Manages persistent state for potentially multiple Adrastea Generator instances,
    each identified by a unique configuration signature (bot, instance, symbol,
    timeframe, direction).

    Each instance manages its state variables:
    - active_signal_id (str | None)
    - market_close_timestamp (float | None) <-- Stores Unix timestamp

    Handles its own database connection internally.
    Implements a factory pattern using an async get_instance class method
    to retrieve or create instances based on the unique configuration key.
    """
    _instances: Dict[str, 'AdrasteaGeneratorStateManager'] = {}
    _instances_lock = asyncio.Lock()  # Class level lock for managing the instances dictionary

    @classmethod
    async def get_instance(cls, config: 'ConfigReader', trading_config: 'TradingConfiguration') -> 'AdrasteaGeneratorStateManager':
        """
        Gets the singleton instance for the specific configuration,
        initializing it asynchronously if necessary.
        """
        instance_key = cls._generate_instance_key(config, trading_config)

        # Fast path: Check if instance already exists without lock first
        # (reduces contention if instance is frequently accessed)
        if instance_key in cls._instances:
            # Ensure initialization completed if accessed quickly after creation start
            instance = cls._instances[instance_key]
            await instance._ensure_db_ready()  # Wait if init is still in progress
            return instance

        async with cls._instances_lock:
            # Double-check if another task created the instance while waiting for the lock
            if instance_key in cls._instances:
                instance = cls._instances[instance_key]
                # Ensure initialization completed
                await instance._ensure_db_ready()
                return instance

            # --- Instance does not exist, create and initialize it ---
            print(f"Creating new State Manager instance for key: {instance_key}")  # Use print or logger before logger is setup
            instance = cls(config, trading_config, instance_key)
            cls._instances[instance_key] = instance

            # Initialize asynchronously *after* adding to dict but within the lock scope initially
            # to prevent race conditions where another task tries to create the same instance.
            # The actual DB connection happens within initialize().
            try:
                # Start initialization but don't block other get_instance calls for *different* keys
                # The instance itself will use its internal lock (_init_lock)
                # and event (_db_ready) to manage its state.
                # Callers using the instance will wait via _ensure_db_ready if needed.
                await instance.initialize()
                print(f"Instance for key {instance_key} initialized.")  # Use print or logger
            except Exception as e:
                # If initialization fails, remove the instance from the registry
                # so future calls will attempt to create it again.
                print(f"ERROR: Initialization failed for {instance_key}: {e}. Removing from registry.")  # Use print or logger
                cls._instances.pop(instance_key, None)
                # Propagate the error to the caller
                raise RuntimeError(f"Failed to initialize State Manager for key {instance_key}") from e

            return instance

    @classmethod
    def _generate_instance_key(cls, config: 'ConfigReader', trading_config: 'TradingConfiguration') -> str:
        """Generates a unique string key based on the configuration."""
        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        symbol = trading_config.get_symbol()
        # Handle potential None values gracefully for key generation
        timeframe_name = getattr(trading_config.get_timeframe(), 'name', 'None')
        direction_name = getattr(trading_config.get_trading_direction(), 'name', 'None')

        if not all([bot_name, instance_name, symbol, timeframe_name != 'None', direction_name != 'None']):
            # Raise error here as __init__ also checks this, preventing partial keys
            raise ValueError("Cannot build state key: missing configuration parameters "
                             f"(bot={bot_name}, instance={instance_name}, symbol={symbol}, "
                             f"tf={timeframe_name}, dir={direction_name})")

        # Simple concatenation (ensure consistent order)
        key_string = f"{bot_name}_{instance_name}_{symbol}_{timeframe_name}_{direction_name}"
        # Optional: Use hashing if keys become too long or complex
        # key_hash = hashlib.sha256(key_string.encode()).hexdigest()
        # return key_hash
        return key_string

    # Remove the __new__ method check, as we now manage multiple instances
    # def __new__(cls, *args, **kwargs):
    #     # This check is invalid in the multi-instance pattern
    #     # if cls._instance is not None:
    #     #    raise RuntimeError("Use AdrasteaGeneratorStateManager.get_instance() instead")
    #     return super().__new__(cls)

    def __init__(self, config: 'ConfigReader', trading_config: 'TradingConfiguration', instance_key: str):
        """
        Initializes the state manager synchronously for a specific instance key.
        Should only be called by get_instance.
        """
        # Prevent re-initialization of the same object instance
        if hasattr(self, '_initialized') and self._initialized:
            # This might indicate an issue in get_instance logic if reached
            # but serves as a safeguard.
            print(f"WARN: Instance {instance_key} already initialized. Skipping __init__.")  # Use print or logger
            return

        # Initialize LoggingMixin first
        super().__init__(config)

        self.instance_key = instance_key
        self.config = config
        self.trading_config: 'TradingConfiguration' = trading_config

        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        symbol = trading_config.get_symbol()
        timeframe_name = trading_config.get_timeframe().name
        direction_name = trading_config.get_trading_direction().name

        if not all([bot_name, instance_name, symbol, timeframe_name, direction_name]):
            self.error(f"[{self.instance_key}] Cannot build state key: missing parameters")
            raise ValueError("Cannot build state key: missing parameters")

        self.agent = f"StateManager_{bot_name}_{instance_name}_{symbol}_{timeframe_name}_{direction_name}"

        self.info(f"[{self.agent}] Running __init__.")

        self.db_service: Optional[MongoDBService] = None
        self._db_ready = asyncio.Event()
        self._async_initialized = False
        self._init_lock = asyncio.Lock()

        self._active_signal_id: Optional[str] = None
        self._market_close_timestamp: Optional[float] = None

        self._initialized = True  # Mark synchronous init as complete
        self.info(f"[{self.agent}] __init__ completed.")

    async def initialize(self):
        """
        Handles asynchronous initialization for this specific instance:
        DB connection, index creation, and state loading.
        This method should only be called once by get_instance.
        """
        async with self._init_lock:
            if self._async_initialized:
                self.info(f"[{self.agent}] Asynchronous initialization already completed.")
                return

            self.info(f"[{self.agent}] Starting asynchronous initialization (DB Connect & Load State)...")
            try:
                db_host = self.config.get_mongo_host()
                db_port = self.config.get_mongo_port()
                db_user = self.config.get_mongo_username()
                db_pass = self.config.get_mongo_password()
                db_name = self.config.get_mongo_db_name()
                is_cluster = self.config.get_mongo_is_cluster()

                if not all([db_host, db_name]):
                    self.error(f"[{self.agent}] MongoDB config missing.")
                    raise ValueError("MongoDB config missing.")

                self.db_service = MongoDBService(
                    config=self.config, host=db_host, port=db_port,
                    username=db_user, password=db_pass, db_name=db_name,
                    is_cluster=is_cluster
                )
                await self.db_service.connect()

                if not await self.db_service.test_connection():
                    self.error(f"[{self.agent}] MongoDB connection failed.")
                    raise ConnectionError("MongoDB connection failed.")

                self.info(f"[{self.agent}] Ensuring indexes on collection '{AGENT_STATE_COLLECTION}'...")

                await self.db_service.create_index(
                    collection=AGENT_STATE_COLLECTION,
                    index_field="agent",
                    unique=True
                )

                # Load the state specific to this instance
                await self._load_state()

                self._async_initialized = True
                self._db_ready.set()  # Signal readiness for THIS instance
                self.info(f"[{self.agent}] Asynchronous initialization completed successfully.")

            except Exception as e:
                self.critical(f"[{self.agent}] Failed async initialization: {e}", exec_info=True)
                self._db_ready.clear()  # Ensure it's not set on failure

                # Clean up DB service if partially created
                if self.db_service:
                    try:
                        await self.db_service.disconnect()
                    except Exception as disconnect_e:
                        self.error(f"[{self.agent}] Error during disconnect after init failure: {disconnect_e}", exec_info=True)
                    finally:
                        self.db_service = None
                raise

    async def _ensure_db_ready(self):
        """Waits until the database connection and initialization for THIS instance are ready."""
        if not self._async_initialized:
            self.debug(f"[{self.agent}] Waiting for async initialization lock/event...")
            async with self._init_lock:
                # Check again after acquiring lock, in case init finished while waiting
                if not self._async_initialized:
                    # If it's still not initialized after acquiring the lock, it means
                    # initialize() failed or was never properly called/completed.
                    self.error(f"[{self.agent}] State Manager async initialization failed or never completed.")
                    raise RuntimeError(f"[{self.agent}] State Manager async initialization failed or never completed.")

        # If initialized flag is true, check the event (covers cases where init succeeded but event wasn't set?)
        if not self._db_ready.is_set():
            self.debug(f"[{self.agent}] DB initialized but event not set, waiting...")
            try:
                # Wait for the event to be set by initialize()
                await asyncio.wait_for(self._db_ready.wait(), timeout=60.0)
                self.debug(f"[{self.agent}] DB readiness signal received.")
            except asyncio.TimeoutError:
                self.error(f"[{self.agent}] Timeout waiting for DB readiness signal.")
                # Attempt cleanup or raise specific error
                await self.stop()  # Attempt graceful shutdown of this instance
                raise TimeoutError(f"[{self.agent}] Timeout waiting for DB readiness.")

        # Final check after waiting
        if not self.db_service or not await self.db_service.test_connection():
            self.error(f"[{self.agent}] DB service not available or disconnected after waiting.")
            raise ConnectionError(f"[{self.agent}] DB service unavailable after initialization.")

    @property
    def active_signal_id(self) -> Optional[str]:
        return self._active_signal_id

    @property
    def market_close_timestamp(self) -> Optional[float]:
        """Returns the market close timestamp as a Unix float (seconds since epoch) or None."""
        return self._market_close_timestamp

    def update_active_signal_id(self, signal_id: Optional[str]) -> bool:
        """Updates the active signal ID. Returns True if changed."""
        changed = False
        if not isinstance(signal_id, (str, type(None))):
            self.warning(f"[{self.agent}] Invalid type for active_signal_id update: {type(signal_id)}. Expected str or None.")
            return False
        if self._active_signal_id != signal_id:
            old_value = self._active_signal_id
            self._active_signal_id = signal_id
            self.debug(f"[{self.agent}] Updated internal active_signal_id from '{old_value}' to: '{signal_id if signal_id else 'None'}'")
            changed = True
        return changed

    def update_market_close_timestamp(self, timestamp_unix: Optional[float]) -> bool:
        """
        Updates the market close timestamp (stores Unix float). Returns True if changed.
        """
        changed = False
        processed_timestamp: Optional[float] = None
        if timestamp_unix is not None:
            if not isinstance(timestamp_unix, (float, int)):
                self.warning(f"[{self.agent}] Invalid type for market close timestamp: {type(timestamp_unix)}. Expected float or int.")
                return False
            processed_timestamp = float(timestamp_unix)
        if self._market_close_timestamp != processed_timestamp:
            old_value_str = str(self._market_close_timestamp) if self._market_close_timestamp is not None else "None"
            self._market_close_timestamp = processed_timestamp
            new_value_str = str(processed_timestamp) if processed_timestamp is not None else "None"
            self.debug(f"[{self.agent}] Updated internal market_close_timestamp (Unix float) from {old_value_str} to: {new_value_str}")
            changed = True
        return changed

    async def save_state(self) -> bool:
        """
        Saves the current state for THIS instance to MongoDB with state fields at the top level.
        """
        await self._ensure_db_ready()  # Ensure THIS instance's DB is ready

        if not self.db_service:
            self.error(f"[{self.agent}] Cannot save state: DB service is not available.")
            return False

        try:
            state_payload = {
                "active_signal_id": self._active_signal_id,
                "market_close_timestamp": self._market_close_timestamp,
                "timestamp_saved": datetime.datetime.now(timezone.utc).isoformat(),
                "agent": self.instance_key
            }

            self.debug(f"[{self.agent}] Saving state with filter: {self.agent} and $set payload: {state_payload}")

            result: Optional[dict] = await self.db_service.upsert(
                collection=AGENT_STATE_COLLECTION,
                id_object={"agent": self.agent},
                payload=state_payload
            )

            if result is not None:
                if 'ids' in result and len(result['ids']) > 0:
                    self.info(f"[{self.agent}] State CREATED or UPDATED successfully (Filter: {self.agent}, ID: {result['ids']}).")
                else:
                    self.warning(f"[{self.agent}] Upsert for {self.agent} matched 0 documents and did not insert.")
                return True
            else:
                self.error(f"[{self.agent}] Upsert operation for {self.agent} returned None, indicating a possible service error.")
                return False

        except (ConnectionError, TimeoutError) as db_e:
            self.error(f"[{self.agent}] DB connection/timeout error during save_state: {db_e}", exec_info=True)
            return False
        except Exception as e:
            self.error(f"[{self.agent}] Unexpected error saving state for filter {self.agent}: {e}", exec_info=True)
            return False

    async def _load_state(self):
        """
        Loads the state for THIS instance from MongoDB, reading fields from the top level.
        Should only be called during its initialization.
        """
        if not self.db_service:
            self.error(f"[{self.agent}] Cannot load state: DB service is not initialized.")
            return

        filter_query = {"agent": self.agent}
        self.info(f"[{self.agent}] Attempting to load state using filter: {filter_query}")

        try:
            document = await self.db_service.find_one(
                collection=AGENT_STATE_COLLECTION,
                id_object=filter_query
            )

            if document:
                doc_id = document.get('_id', 'N/A')
                self.info(f"[{self.agent}] Found previous state document (ID: {doc_id}). Loading state from top-level fields...")

                self._active_signal_id = document.get("active_signal_id")
                self._market_close_timestamp = document.get("market_close_timestamp")

                # Log the loaded state values for debugging
                loaded_ts = document.get("timestamp_saved", "N/A")
                loaded_agent = document.get("agent", "N/A")
                self.info(f"[{self.agent}] State loaded: active_signal_id='{self._active_signal_id}', "
                          f"market_close_timestamp='{self._market_close_timestamp}', "
                          f"timestamp_saved='{loaded_ts}', agent='{loaded_agent}'")
            else:
                # No previous state document found for this key
                self.info(f"[{self.agent}] No previous state found for filter {filter_query}. Initializing state variables to None.")
                self._active_signal_id = None
                self._market_close_timestamp = None

        except Exception as e:
            self.error(f"[{self.agent}] Unexpected error loading state for filter {self.agent}: {e}", exec_info=True)
            self.warning(f"[{self.agent}] Proceeding with default empty state due to load error.")
            self._active_signal_id = None
            self._market_close_timestamp = None

    async def stop(self):
        """Disconnects the MongoDB service for THIS specific instance gracefully."""
        self.info(f"[{self.agent}] Stopping State Manager instance and disconnecting its DB service...")

        if self.db_service:
            try:
                await self.db_service.disconnect()
                self.info(f"[{self.agent}] MongoDB service disconnected successfully.")
            except Exception as e:
                self.error(f"[{self.agent}] Error during DB disconnection: {e}", exec_info=True)
            finally:
                self.db_service = None

        self._db_ready.clear()
        self._async_initialized = False

        self.info(f"[{self.agent}] State Manager instance stopped.")

    @classmethod
    def get_logger(cls) -> logging.Logger:
        return logging.getLogger(cls.__name__)
