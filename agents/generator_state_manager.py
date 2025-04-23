import asyncio
import datetime
from datetime import timezone
from typing import Optional, Dict

from pymongo.results import UpdateResult

from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.logger_mixing import LoggingMixin
from services.service_mongodb import MongoDBService

AGENT_STATE_COLLECTION = "agent_states"


class AdrasteaGeneratorStateManager(LoggingMixin):
    """
    Manages persistent state for an Adrastea Generator instance using
    individual update methods for each state variable:
    - active_signal_id
    - market_close_timestamp (naive UTC / float)

    Handles its own database connection internally. Identified by configuration key.
    Implements the Singleton pattern using an async get_instance method.
    """
    _instance: Optional['AdrasteaGeneratorStateManager'] = None
    _lock = asyncio.Lock()  # Class level lock for instance creation

    @classmethod
    async def get_instance(cls, config: ConfigReader, trading_config: TradingConfiguration) -> 'AdrasteaGeneratorStateManager':
        """
        Gets the singleton instance, initializing it if necessary.
        The config and trading_config are only used during the first call.
        """
        if cls._instance is None:
            async with cls._lock:
                # Double-check locking
                if cls._instance is None:
                    print(f"Creating new AdrasteaGeneratorStateManager instance for {trading_config.get_symbol()}")
                    instance = cls.__new__(cls)  # Create instance without calling __init__ yet
                    # Call __init__ manually AFTER instance creation
                    instance.__init__(config, trading_config)
                    # Perform async initialization
                    await instance.initialize()
                    cls._instance = instance
                    print(f"AdrasteaGeneratorStateManager instance created and initialized.")
        else:
            print(f"Returning existing AdrasteaGeneratorStateManager instance.")
        return cls._instance

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """Initializes the state manager synchronously. Should only run once."""
        # Prevent re-initialization
        if hasattr(self, '_initialized') and self._initialized:
            print(f"Instance already initialized. Skipping __init__.")
            return

        print(f"Running __init__ for AdrasteaGeneratorStateManager.")
        super().__init__(config)  # Initialize LoggingMixin
        self.config = config
        self.trading_config: TradingConfiguration = trading_config

        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        symbol = trading_config.get_symbol()
        timeframe_name = trading_config.get_timeframe().name
        direction_name = trading_config.get_trading_direction().name

        if not all([bot_name, instance_name, symbol, timeframe_name, direction_name]):
            raise ValueError("Cannot build state key: missing parameters")

        self._state_key: Dict[str, str] = {
            "bot_name": bot_name, "instance_name": instance_name, "symbol": symbol,
            "timeframe": timeframe_name, "trading_direction": direction_name
        }
        # Use a more descriptive agent name for the logger
        self.agent = f"StateManager_{instance_name}_{symbol}_{timeframe_name}_{direction_name}"

        # Database and state variables
        self.db_service: Optional[MongoDBService] = None
        self._db_ready = asyncio.Event()
        self._async_initialized = False  # Tracks async initialization completion
        self._init_lock = asyncio.Lock()  # Lock specifically for async initialize method

        self._active_signal_id: Optional[str] = None
        # Changed to store naive UTC datetime directly for consistency
        self._market_close_timestamp_dt: Optional[datetime.datetime] = None

        self._initialized = True  # Mark synchronous init as done
        print(f"__init__ completed for AdrasteaGeneratorStateManager.")

    async def initialize(self):
        """
        Handles asynchronous initialization: DB connection, index creation, and state loading.
        This method should only be called once by get_instance.
        """
        async with self._init_lock:  # Protect async initialization
            if self._async_initialized:
                self.info("Asynchronous initialization already completed.")
                return

            self.info("Starting asynchronous initialization (DB Connect & Load State)...")
            try:
                # --- Instantiate and connect DB service ---
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

                # --- Ensure Indexes ---
                # Create indexes necessary for querying agent states
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "bot_name")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "instance_name")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "symbol")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "timeframe")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "trading_direction")
                # Consider a compound index if queries often use multiple fields:
                # await self.db_service.create_index(AGENT_STATE_COLLECTION, [("bot_name", 1), ("instance_name", 1), ...])

                # Load initial state from the database
                await self._load_state()

                self._async_initialized = True  # Mark async init as done
                self._db_ready.set()  # Signal that DB is ready
                self.info("Asynchronous initialization completed successfully.")

            except Exception as e:
                self.critical(f"Failed async initialization: {e}", exc_info=True)
                self._db_ready.clear()  # Ensure DB is not marked as ready
                # Attempt to clean up DB connection if partially initialized
                if self.db_service:
                    try:
                        await self.db_service.disconnect()
                    except Exception as disconnect_e:
                        self.error(f"Error during disconnect after init failure: {disconnect_e}", exc_info=True)
                    finally:
                        self.db_service = None
                raise  # Re-raise the exception to signal failure

    async def _ensure_db_ready(self):
        """Waits until the database connection and initialization are ready."""
        if not self._async_initialized:
            # This case should ideally not happen if get_instance is used correctly
            self.warning("Attempted DB operation before async initialization completed. Waiting...")
            # Fallback: Wait for the init lock to release (implies initialize is running or done)
            async with self._init_lock:
                if not self._async_initialized:
                    raise RuntimeError("State Manager async initialization failed or never completed.")
        # If async init completed, wait for the DB ready signal
        if not self._db_ready.is_set():
            self.debug("Waiting for DB readiness signal...")
            try:
                await asyncio.wait_for(self._db_ready.wait(), timeout=60.0)
                self.debug("DB readiness signal received.")
            except asyncio.TimeoutError:
                self.error("Timeout waiting for DB readiness signal.")
                raise TimeoutError("Timeout waiting for DB readiness.")
        # Final check after waiting
        if not self.db_service or not await self.db_service.test_connection():
            self.error("DB service not available or connection lost after waiting.")
            raise ConnectionError("DB service unavailable after initialization.")

    @property
    def active_signal_id(self) -> Optional[str]:
        return self._active_signal_id

    @property
    def market_close_timestamp(self) -> Optional[datetime.datetime]:
        """Returns the market close timestamp as a naive UTC datetime object."""
        return self._market_close_timestamp_dt

    def update_active_signal_id(self, signal_id: Optional[str]) -> bool:
        """Updates the active signal ID. Returns True if changed."""
        changed = False
        # Basic type validation
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
        Updates the market close timestamp. Input is expected as a Unix float (seconds since epoch).
        Internally stores as a naive UTC datetime object. Returns True if changed.
        """
        changed = False
        new_timestamp_dt: Optional[datetime.datetime] = None

        if timestamp_unix is not None:
            # Input validation
            if not isinstance(timestamp_unix, (float, int)):
                self.warning(f"Invalid type for market close timestamp: {type(timestamp_unix)}. Expected float or int.")
                return False
            try:
                # Convert Unix float to aware UTC datetime, then make naive
                new_timestamp_dt = datetime.datetime.fromtimestamp(float(timestamp_unix), tz=timezone.utc).replace(tzinfo=None)
            except (ValueError, OSError) as e:
                self.error(f"Error converting Unix timestamp {timestamp_unix} to datetime: {e}", exc_info=True)
                return False  # Invalid timestamp value

        # Compare the new naive datetime with the stored one
        if self._market_close_timestamp_dt != new_timestamp_dt:
            old_value = self._market_close_timestamp_dt.isoformat() if self._market_close_timestamp_dt else "None"
            self._market_close_timestamp_dt = new_timestamp_dt
            new_value_str = new_timestamp_dt.isoformat() if new_timestamp_dt else "None"
            self.debug(f"Updated internal market_close_timestamp (naive UTC datetime) from '{old_value}' to: {new_value_str}")
            changed = True

        return changed

    async def save_state(self) -> bool:
        """Saves the current state (active_signal_id, market_close_timestamp) to MongoDB."""
        await self._ensure_db_ready()  # Ensure DB is ready before proceeding

        if not self.db_service:
            self.error("Cannot save state: DB service is not available.")
            return False

        try:
            # Prepare the market close timestamp for saving
            # Convert the internal naive UTC datetime back to an ISO 8601 string (aware UTC)
            market_close_ts_iso = None
            if self._market_close_timestamp_dt:
                # Make it timezone-aware (UTC) before formatting
                aware_dt = self._market_close_timestamp_dt.replace(tzinfo=timezone.utc)
                market_close_ts_iso = aware_dt.isoformat()

            # Construct the state data payload for MongoDB
            state_data = {
                "active_signal_id": self._active_signal_id,
                # Store the timestamp in a standardized format (ISO 8601 aware UTC)
                "market_close_timestamp": market_close_ts_iso,
                # Add a timestamp for when the state was saved
                "timestamp_saved": datetime.datetime.now(timezone.utc).isoformat()
            }

            # Use the unique state key as the filter for the upsert operation
            filter_query = self._state_key
            self.debug(f"Saving state with filter: {filter_query} and data: {state_data}")

            # Perform the upsert operation
            result: Optional[UpdateResult] = await self.db_service.upsert(
                collection=AGENT_STATE_COLLECTION,
                id_object=filter_query,  # Use the state key as the filter
                payload={"$set": {"state_manager_state": state_data}}  # Nest state under a specific key
                # Use $set to update specific fields within a nested document
            )

            # Process the result of the upsert operation
            if result is not None:
                if result.upserted_id:
                    self.info(f"State created successfully for key {self._state_key} (ID: {result.upserted_id}).")
                elif result.modified_count > 0:
                    self.info(f"State updated successfully for key {self._state_key}.")
                elif result.matched_count > 0:
                    # This means a document matching the key existed, but no fields were changed by the $set operation
                    self.debug(f"State save for key {self._state_key} completed, no changes detected.")
                else:
                    # This case (matched_count=0, modified_count=0, upserted_id=None) should ideally not happen with upsert=True
                    # unless there was a very specific concurrency issue or error.
                    self.warning(f"Upsert for {self._state_key} matched 0 documents and did not insert.")
                return True
            else:
                # This indicates a failure in the db_service.upsert method itself
                self.error(f"Upsert operation for {self._state_key} returned None, indicating a possible service error.")
                return False

        except ConnectionError as ce:
            self.error(f"Connection error during save_state: {ce}", exc_info=True)
            return False
        except TimeoutError as te:
            self.error(f"Timeout error during save_state: {te}", exc_info=True)
            return False
        except Exception as e:
            # Catch any other unexpected exceptions during the save process
            self.error(f"Unexpected error saving state for key {self._state_key}: {e}", exc_info=True)
            return False

    async def _load_state(self):
        """
        Loads the state (active_signal_id, market_close_timestamp) from MongoDB.
        Should only be called during initialization. Stores timestamps as naive UTC datetime.
        """
        # This method assumes db_service is available because it's called within initialize
        if not self.db_service:
            self.error("Cannot load state: DB service is not initialized.")
            return  # Should not happen if called correctly

        filter_query = self._state_key
        self.info(f"Attempting to load state for key: {filter_query}")

        try:
            # Find the document matching the state key
            document = await self.db_service.find_one(
                collection_name=AGENT_STATE_COLLECTION,
                filter_query=filter_query
            )

            # Check if a document was found and if it contains the expected state data
            if document and 'state_manager_state' in document:
                state_data = document['state_manager_state']
                self.info(f"Found previous state document (ID: {document.get('_id')}). Loading state...")

                # Load active_signal_id
                self._active_signal_id = state_data.get("active_signal_id")  # Defaults to None if key missing

                # Load market_close_timestamp
                market_ts_iso_str = state_data.get("market_close_timestamp")
                self._market_close_timestamp_dt = None  # Default to None

                if market_ts_iso_str:
                    try:
                        # Parse the ISO 8601 string (aware UTC) and convert to naive UTC datetime
                        aware_dt = datetime.datetime.fromisoformat(market_ts_iso_str)
                        # Ensure it's UTC before making naive
                        if aware_dt.tzinfo is None or aware_dt.tzinfo.utcoffset(aware_dt) != datetime.timedelta(0):
                            self.warning(f"Loaded market_close_timestamp '{market_ts_iso_str}' is not timezone-aware UTC. Assuming UTC.")
                            aware_dt = aware_dt.replace(tzinfo=timezone.utc)  # Assume UTC if naive or different timezone
                        else:
                            aware_dt = aware_dt.astimezone(timezone.utc)  # Convert to UTC if aware but different timezone

                        self._market_close_timestamp_dt = aware_dt.replace(tzinfo=None)
                    except (ValueError, TypeError) as e:
                        self.error(f"Error parsing market_close_timestamp '{market_ts_iso_str}': {e}", exc_info=True)
                        # Keep self._market_close_timestamp_dt as None

                # Log the loaded state values for debugging
                mc_ts_str = self._market_close_timestamp_dt.isoformat() if self._market_close_timestamp_dt else "None"
                self.info(f"State loaded: active_signal_id='{self._active_signal_id}', market_close_timestamp='{mc_ts_str}'")

            else:
                # No previous state document found for this key
                self.info(f"No previous state found for key {self._state_key}. Initializing state variables to None.")
                self._active_signal_id = None
                self._market_close_timestamp_dt = None

        except ConnectionError as ce:
            # Handle potential connection errors during find_one
            self.error(f"Connection error during _load_state: {ce}", exc_info=True)
            # Set state to None as a fallback
            self._active_signal_id = None
            self._market_close_timestamp_dt = None
        except TimeoutError as te:
            # Handle potential timeouts during find_one
            self.error(f"Timeout error during _load_state: {te}", exc_info=True)
            # Set state to None as a fallback
            self._active_signal_id = None
            self._market_close_timestamp_dt = None
        except Exception as e:
            # Catch any other unexpected errors during the load process
            self.error(f"Unexpected error loading state for key {self._state_key}: {e}", exc_info=True)
            # Reset state variables to None as a safe default
            self._active_signal_id = None
            self._market_close_timestamp_dt = None

    async def stop(self):
        """Disconnects the internal MongoDB service instance gracefully."""
        self.info("Stopping State Manager and disconnecting DB service...")
        async with self._lock:  # Use class lock for stopping as well
            if self.db_service:
                try:
                    await self.db_service.disconnect()
                    self.info("MongoDB service disconnected successfully.")
                except Exception as e:
                    self.error(f"Error during DB disconnection: {e}", exc_info=True)
                finally:
                    self.db_service = None  # Ensure db_service is cleared

            # Reset flags
            self._db_ready.clear()
            self._async_initialized = False
            # Reset singleton instance (optional, depending on desired lifecycle)
            # AdrasteaGeneratorStateManager._instance = None
            # self.info("Singleton instance reference cleared.")

            self.info("State Manager stopped.")
