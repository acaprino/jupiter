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
    - market_close_timestamp (naive UTC)
    - last_processed_candle_timestamp (naive UTC)

    Handles its own database connection internally. Identified by configuration key.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """Initializes the state manager synchronously."""
        super().__init__(config)
        self.config = config
        self.trading_config: TradingConfiguration = trading_config

        bot_name = config.get_bot_name(); instance_name = config.get_instance_name()
        symbol = trading_config.get_symbol(); timeframe_name = trading_config.get_timeframe().name
        direction_name = trading_config.get_trading_direction().name
        if not all([bot_name, instance_name, symbol, timeframe_name, direction_name]):
             raise ValueError("Cannot build state key: missing parameters")
        self._state_key: Dict[str, str] = {
            "bot_name": bot_name, "instance_name": instance_name, "symbol": symbol,
            "timeframe": timeframe_name, "trading_direction": direction_name
        }
        self.agent = f"StateManager_{instance_name}_{symbol}_{timeframe_name}_{direction_name}"

        self.db_service: Optional[MongoDBService] = None
        self._db_ready = asyncio.Event()
        self._async_initialized = False
        self._init_lock = asyncio.Lock()

        self._active_signal_id: Optional[str] = None
        self._market_close_timestamp: Optional[float] = None
        self._last_processed_candle_timestamp: Optional[datetime.datetime] = None


    async def initialize(self):
        """Handles asynchronous initialization: DB connection, index, state load."""
        async with self._init_lock:
            if self._async_initialized: return
            self.info("Starting asynchronous initialization (DB Connect & Load State)...")
            try:
                # --- Instantiate and connect DB service ---
                db_host=self.config.get_mongo_host(); db_port=self.config.get_mongo_port() # etc.
                db_user=self.config.get_mongo_username(); db_pass=self.config.get_mongo_password()
                db_name=self.config.get_mongo_db_name(); is_cluster=self.config.get_mongo_is_cluster()
                if not all([db_host, db_name]): raise ValueError("MongoDB config missing.")
                self.db_service = MongoDBService(config=self.config, host=db_host, port=db_port, username=db_user, password=db_pass, db_name=db_name, is_cluster=is_cluster)
                await self.db_service.connect()
                if not await self.db_service.test_connection(): raise ConnectionError("MongoDB connection failed.")
                # --- Ensure Indexes ---
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "bot_name")
                # ... create other indexes ...
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "instance_name")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "symbol")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "timeframe")
                await self.db_service.create_index(AGENT_STATE_COLLECTION, "trading_direction")

                await self._load_state() # Load initial state

                self._async_initialized = True; self._db_ready.set()
                self.info("Asynchronous initialization completed successfully.")
            except Exception as e:
                self.critical(f"Failed async initialization: {e}", exec_info=True)
                self._db_ready.clear()
                if self.db_service: await self.db_service.disconnect(); self.db_service = None
                raise

    async def _ensure_db_ready(self):
        """Waits until the database connection is ready."""
        if not self._async_initialized: raise RuntimeError("State Manager not initialized.")
        if not self._db_ready.is_set():
            self.debug("Waiting for DB readiness signal..."); await asyncio.wait_for(self._db_ready.wait(), timeout=60.0)

    @property
    def active_signal_id(self) -> Optional[str]: return self._active_signal_id
    @property
    def market_close_timestamp(self) -> Optional[datetime.datetime]: return self._market_close_timestamp
    @property
    def last_processed_candle_timestamp(self) -> Optional[datetime.datetime]: return self._last_processed_candle_timestamp

    def update_active_signal_id(self, signal_id: Optional[str]) -> bool:
        """Updates the active signal ID. Returns True if changed."""
        changed = False
        if not isinstance(signal_id, (str, type(None))):
             self.warning(f"Invalid type for active_signal_id update: {type(signal_id)}")
             return False

        if self._active_signal_id != signal_id:
            self._active_signal_id = signal_id
            self.debug(f"Updated internal active_signal_id to: {signal_id if signal_id else 'None'}")
            changed = True
        return changed

    def update_market_close_timestamp(self, timestamp_unix: Optional[float]) -> bool:
        """Updates the market close timestamp (stores naive UTC). Returns True if changed."""
        changed = False
        processed_timestamp: Optional[float] = None

        if timestamp_unix is not None:
            # Check if the input is a valid number (float or int)
            if not isinstance(timestamp_unix, (float, int)):
                self.warning(f"Invalid type for market close timestamp: {type(timestamp_unix)}. Expected float or int.")
                return False

            processed_timestamp = float(timestamp_unix)

        if self._market_close_timestamp != processed_timestamp:
            self._market_close_timestamp = processed_timestamp
            # Log the new value (or None)
            ts_str = str(processed_timestamp) if processed_timestamp is not None else "None"
            self.debug(f"Updated internal market_close_timestamp (Unix float) to: {ts_str}")
            changed = True

        return changed

    def update_last_processed_candle(self, timestamp: Optional[datetime.datetime]) -> bool:
        """Updates the last processed candle timestamp (stores naive UTC). Returns True if changed and newer."""
        changed = False
        timestamp_naive_utc: Optional[datetime.datetime] = None
        if timestamp is not None:
            if not isinstance(timestamp, datetime.datetime):
                 self.warning(f"Invalid type for last processed candle timestamp: {type(timestamp)}.")
                 return False
            if timestamp.tzinfo is None: timestamp_naive_utc = timestamp # Assume naive is UTC
            else: timestamp_naive_utc = timestamp.astimezone(timezone.utc).replace(tzinfo=None) # Convert aware to naive UTC

        # Proceed only if we have a valid naive UTC timestamp
        if timestamp_naive_utc is not None:
            # Update only if newer or first time
            if self._last_processed_candle_timestamp is None or timestamp_naive_utc > self._last_processed_candle_timestamp:
                if self._last_processed_candle_timestamp != timestamp_naive_utc: # Check if really changed
                     self._last_processed_candle_timestamp = timestamp_naive_utc
                     self.debug(f"Updated internal last_processed_candle_timestamp (naive UTC) to: {timestamp_naive_utc.isoformat()}")
                     changed = True
        elif timestamp is None: # Handle explicit reset to None?
            if self._last_processed_candle_timestamp is not None:
                 self._last_processed_candle_timestamp = None
                 self.debug("Cleared internal last_processed_candle_timestamp.")
                 changed = True
        return changed

    async def save_state(self) -> bool:
        """Saves the current state (all 3 fields) to MongoDB."""
        await self._ensure_db_ready()
        if not self.db_service: return False
        try:
            # Prepare timestamps for saving (convert naive UTC back to aware ISO)
            market_close_ts_iso = None
            if self._market_close_timestamp: market_close_ts_iso = self._market_close_timestamp.replace(tzinfo=timezone.utc).isoformat()
            last_processed_ts_iso = None
            if self._last_processed_candle_timestamp: last_processed_ts_iso = self._last_processed_candle_timestamp.replace(tzinfo=timezone.utc).isoformat()

            state_data = {
                "active_signal_id": self._active_signal_id,
                "market_close_timestamp": market_close_ts_iso,
                "last_processed_candle_timestamp": last_processed_ts_iso,
                "timestamp_saved": datetime.datetime.now(timezone.utc).isoformat()
            }
            filter_query = self._state_key
            result: Optional[UpdateResult] = await self.db_service.upsert(
                collection=AGENT_STATE_COLLECTION, id_object=filter_query, payload=state_data
            )
            # ... (Result checking logic unchanged) ...
            if result is not None:
                 if result.upserted_id: self.info(f"State created for key {self._state_key}.")
                 elif result.modified_count > 0: self.info(f"State updated for key {self._state_key}.")
                 elif result.matched_count > 0: self.debug(f"State save for key {self._state_key} completed, no changes.")
                 else: self.warning(f"Upsert for {self._state_key} matched 0 docs.")
                 return True
            else:
                 self.error(f"Upsert for {self._state_key} returned None."); return False
        except Exception as e:
            self.error(f"Error saving state: {e}", exec_info=e); return False

    async def _load_state(self):
        """Loads the state (all 3 fields) from MongoDB. Stores timestamps as naive UTC."""
        if not self.db_service: return
        filter_query = self._state_key
        self.info(f"Attempting to load state for key: {filter_query}")
        try:
            document = await self.db_service.find_one(
                collection_name=AGENT_STATE_COLLECTION, filter_query=filter_query
            )
            if document and 'state_manager_state' in document:
                state_data = document['state_manager_state']
                self.info("Found previous state. Loading...")
                # Load fields, converting timestamps to naive UTC
                self._active_signal_id = state_data.get("active_signal_id")
                market_ts_str = state_data.get("market_close_timestamp")
                self._market_close_timestamp = None
                if market_ts_str:
                     try: self._market_close_timestamp = datetime.datetime.fromisoformat(market_ts_str).astimezone(timezone.utc).replace(tzinfo=None)
                     except Exception: self.error("Error parsing market_close_timestamp", exc_info=True)
                last_proc_ts_str = state_data.get("last_processed_candle_timestamp")
                self._last_processed_candle_timestamp = None
                if last_proc_ts_str:
                     try: self._last_processed_candle_timestamp = datetime.datetime.fromisoformat(last_proc_ts_str).astimezone(timezone.utc).replace(tzinfo=None)
                     except Exception: self.error("Error parsing last_processed_candle_timestamp", exc_info=True)
                # Log loaded values
                mc_ts_str=self._market_close_timestamp.isoformat() if self._market_close_timestamp else "None"
                lp_ts_str=self._last_processed_candle_timestamp.isoformat() if self._last_processed_candle_timestamp else "None"
                self.info(f"State loaded: ID='{self._active_signal_id}', CloseTS='{mc_ts_str}', LastProcTS='{lp_ts_str}'")
            else:
                self.info(f"No previous state found for key {self._state_key}. Init state to None.")
                self._active_signal_id = None; self._market_close_timestamp = None; self._last_processed_candle_timestamp = None
        except Exception as e:
            self.error(f"Error loading state: {e}", exec_info=e)
            self._active_signal_id = None; self._market_close_timestamp = None; self._last_processed_candle_timestamp = None

    async def stop(self):
        """Disconnects the internal MongoDB service instance."""
        self.info("Stopping State Manager and disconnecting DB service...")
        if self.db_service: await self.db_service.disconnect(); self.db_service = None
        self._db_ready.clear(); self._async_initialized = False
        self.info("State Manager stopped.")