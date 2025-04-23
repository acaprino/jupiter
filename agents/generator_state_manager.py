import asyncio
import datetime
import logging
from datetime import timezone
from typing import Optional, Dict

from misc_utils.config import TradingConfiguration, ConfigReader
# Assume pymongo UpdateResult is available
# from pymongo.results import UpdateResult
# Assume ConfigReader, TradingConfiguration are defined elsewhere
# from misc_utils.config import ConfigReader, TradingConfiguration
# Assume LoggingMixin is defined elsewhere
from misc_utils.logger_mixing import LoggingMixin
# Assume MongoDBService is defined elsewhere
from services.service_mongodb import MongoDBService

# Mock UpdateResult if not available for standalone execution
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
    Manages persistent state for an Adrastea Generator instance using
    individual update methods for each state variable:
    - active_signal_id (str | None)
    - market_close_timestamp (float | None) <-- Stores Unix timestamp

    Handles its own database connection internally. Identified by configuration key.
    Implements the Singleton pattern using an async get_instance method.
    """
    _instance: Optional['AdrasteaGeneratorStateManager'] = None
    _instance_lock = asyncio.Lock()  # Class level lock for instance creation

    @classmethod
    async def get_instance(cls, config: 'ConfigReader', trading_config: 'TradingConfiguration') -> 'AdrasteaGeneratorStateManager':
        """
        Gets the singleton instance, initializing it if necessary.
        The config and trading_config are only used during the first call.
        """
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(config, trading_config)
            return cls._instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            raise RuntimeError("Use AdrasteaGeneratorStateManager.get_instance() instead")
        return super().__new__(cls)

    def __init__(self, config: 'ConfigReader', trading_config: 'TradingConfiguration'):
        """Initializes the state manager synchronously. Should only run once."""

        if hasattr(self, '_initialized') and self._initialized:
            self.debug(f"Instance already initialized. Skipping __init__.")

        super().__init__(config)

        self.info(f"Running __init__ for AdrasteaGeneratorStateManager.")
        self.config = config
        self.trading_config: 'TradingConfiguration' = trading_config

        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        symbol = trading_config.get_symbol()
        timeframe_name = trading_config.get_timeframe().name
        direction_name = trading_config.get_trading_direction().name

        if not all([bot_name, instance_name, symbol, timeframe_name, direction_name]):
            self.error("Cannot build state key: missing parameters")
            raise ValueError("Cannot build state key: missing parameters")

        self._state_key: Dict[str, str] = {
            "bot_name": bot_name, "instance_name": instance_name, "symbol": symbol,
            "timeframe": timeframe_name, "trading_direction": direction_name
        }
        self.agent = f"StateManager_{instance_name}_{symbol}_{timeframe_name}_{direction_name}"

        self.logger_name = self.agent

        self.db_service: Optional[MongoDBService] = None
        self._db_ready = asyncio.Event()
        self._async_initialized = False
        self._init_lock = asyncio.Lock()

        self._active_signal_id: Optional[str] = None

        self._market_close_timestamp: Optional[float] = None

        self._initialized = True
        self.info(f"__init__ completed for {self.agent}")

    async def initialize(self):
        """
        Handles asynchronous initialization: DB connection, index creation, and state loading.
        This method should only be called once by get_instance.
        """
        async with self._init_lock:
            if self._async_initialized:
                self.info("Asynchronous initialization already completed.")
                return

            self.info("Starting asynchronous initialization (DB Connect & Load State)...")
            try:
                db_host = self.config.get_mongo_host()
                db_port = self.config.get_mongo_port()
                db_user = self.config.get_mongo_username()
                db_pass = self.config.get_mongo_password()
                db_name = self.config.get_mongo_db_name()
                is_cluster = self.config.get_mongo_is_cluster()

                if not all([db_host, db_name]):
                    self.error("MongoDB config missing.")
                    raise ValueError("MongoDB config missing.")

                # Create and connect the DB service instance
                self.db_service = MongoDBService(
                    config=self.config, host=db_host, port=db_port,
                    username=db_user, password=db_pass, db_name=db_name,
                    is_cluster=is_cluster
                )
                await self.db_service.connect()

                if not await self.db_service.test_connection():
                    self.error("MongoDB connection failed.")
                    raise ConnectionError("MongoDB connection failed.")

                self.info(f"Ensuring indexes on collection '{AGENT_STATE_COLLECTION}'...")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "bot_name")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "instance_name")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "symbol")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "timeframe")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "trading_direction")

                await self._load_state()

                self._async_initialized = True
                self._db_ready.set()
                self.info("Asynchronous initialization completed successfully.")

            except Exception as e:
                self.critical(f"Failed async initialization: {e}", exc_info=True)
                self._db_ready.clear()

                if self.db_service:
                    try:
                        await self.db_service.disconnect()
                    except Exception as disconnect_e:
                        self.error(f"Error during disconnect after init failure: {disconnect_e}", exc_info=True)
                    finally:
                        self.db_service = None
                raise

    async def _ensure_db_ready(self):
        """Waits until the database connection and initialization are ready."""
        if not self._async_initialized:

            self.warning("Attempted DB operation before async initialization completed. Waiting...")

            async with self._init_lock:
                if not self._async_initialized:
                    self.error("State Manager async initialization failed or never completed.")
                    raise RuntimeError("State Manager async initialization failed or never completed.")

        if not self._db_ready.is_set():
            self.debug("Waiting for DB readiness signal...")
            try:
                await asyncio.wait_for(self._db_ready.wait(), timeout=60.0)
                self.debug("DB readiness signal received.")
            except asyncio.TimeoutError:
                self.error("Timeout waiting for DB readiness signal.")
                raise TimeoutError("Timeout waiting for DB readiness.")

        if not self.db_service:
            self.error("DB service not available after waiting.")
            raise ConnectionError("DB service unavailable after initialization.")

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
            self.warning(f"Invalid type for active_signal_id update: {type(signal_id)}. Expected str or None.")
            return False

        if self._active_signal_id != signal_id:
            old_value = self._active_signal_id
            self._active_signal_id = signal_id
            self.debug(f"Updated internal active_signal_id from '{old_value}' to: '{signal_id if signal_id else 'None'}'")
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
                self.warning(f"Invalid type for market close timestamp: {type(timestamp_unix)}. Expected float or int.")
                return False
            processed_timestamp = float(timestamp_unix)

        if self._market_close_timestamp != processed_timestamp:
            old_value_str = str(self._market_close_timestamp) if self._market_close_timestamp is not None else "None"
            self._market_close_timestamp = processed_timestamp
            new_value_str = str(processed_timestamp) if processed_timestamp is not None else "None"

            self.debug(f"Updated internal market_close_timestamp (Unix float) from {old_value_str} to: {new_value_str}")
            changed = True

        return changed

    async def save_state(self) -> bool:
        """Saves the current state (active_signal_id, market_close_timestamp as float) to MongoDB."""
        await self._ensure_db_ready()  # Ensure DB is ready before proceeding

        if not self.db_service:
            self.error("Cannot save state: DB service is not available.")
            return False

        try:
            state_data = {
                "active_signal_id": self._active_signal_id,
                "market_close_timestamp": self._market_close_timestamp,
                "timestamp_saved": datetime.datetime.now(timezone.utc).isoformat()
            }

            # Use the unique state key as the filter for the upsert operation
            filter_query = self._state_key
            self.debug(f"Saving state with filter: {filter_query} and data: {state_data}")

            # Perform the upsert operation, nesting state under a specific key
            result: Optional[UpdateResult] = await self.db_service.upsert(
                collection=AGENT_STATE_COLLECTION,
                id_object=filter_query,  # Use the state key as the filter
                payload={"$set": {"state_manager_state": state_data}}  # Update nested document
            )

            # Process the result of the upsert operation
            if result is not None:
                if result.upserted_id:
                    self.info(f"State created successfully for key {self._state_key} (ID: {result.upserted_id}).")
                elif result.modified_count > 0:
                    self.info(f"State updated successfully for key {self._state_key}.")
                elif result.matched_count > 0:
                    self.debug(f"State save for key {self._state_key} completed, no changes detected.")
                else:
                    self.warning(f"Upsert for {self._state_key} matched 0 documents and did not insert.")
                return True
            else:
                self.error(f"Upsert operation for {self._state_key} returned None, indicating a possible service error.")
                return False

        except (ConnectionError, TimeoutError) as db_e:
            # Catch specific DB errors if possible
            self.error(f"DB connection/timeout error during save_state: {db_e}", exc_info=True)
            return False
        except Exception as e:
            # Catch any other unexpected exceptions during the save process
            self.error(f"Unexpected error saving state for key {self._state_key}: {e}", exc_info=True)
            return False

    async def _load_state(self):
        """
        Loads the state (active_signal_id, market_close_timestamp as float) from MongoDB.
        Should only be called during initialization.
        """
        if not self.db_service:
            self.error("Cannot load state: DB service is not initialized.")
            return  # Should not happen if called correctly

        filter_query = self._state_key
        self.info(f"Attempting to load state for key: {filter_query}")

        try:
            # Find the document matching the state key
            document = await self.db_service.find_one(
                collection=AGENT_STATE_COLLECTION,
                id_object=filter_query
            )

            # Check if a document was found and if it contains the expected state data
            if document and 'state_manager_state' in document:
                state_data = document['state_manager_state']
                self.info(f"Found previous state document (ID: {document.get('_id')}). Loading state...")

                self._active_signal_id = state_data.get("active_signal_id")
                self._market_close_timestamp = state_data.get("market_close_timestamp")

                # Log the loaded state values for debugging
                self.info(f"State loaded: active_signal_id='{self._active_signal_id}', market_close_timestamp='{self._market_close_timestamp}'")

            else:
                # No previous state document found for this key
                self.info(f"No previous state found for key {self._state_key}. Initializing state variables to None.")
                self._active_signal_id = None
                self._market_close_timestamp = None

        except Exception as e:
            self.error(f"Unexpected error loading state for key {self._state_key}: {e}", exc_info=True)
            self._active_signal_id = None;
            self._market_close_timestamp = None

    async def stop(self):
        """Disconnects the internal MongoDB service instance gracefully."""
        self.info("Stopping State Manager and disconnecting DB service...")

        async with self._init_lock:
            if self.db_service:
                try:
                    await self.db_service.disconnect()
                    self.info("MongoDB service disconnected successfully.")
                except Exception as e:
                    self.error(f"Error during DB disconnection: {e}", exc_info=True)
                finally:
                    self.db_service = None  # Ensure db_service is cleared

            # Reset internal state flags
            self._db_ready.clear()
            self._async_initialized = False

            self.info("State Manager stopped.")
