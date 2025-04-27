# strategies/agent_strategy_adrastea.py

import asyncio
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, List

import pandas as pd
from pandas import Series, DataFrame

# Local application/library specific imports
from agents.agent_registration_aware import RegistrationAwareAgent
from agents.generator_state_manager import AdrasteaGeneratorStateManager
from csv_loggers.logger_candles import CandlesLogger
from csv_loggers.logger_strategy_events import StrategyEventsLogger
from dto.QueueMessage import QueueMessage
from dto.Signal import Signal, SignalStatus
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Indicators, Timeframe, TradingDirection, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import (
    describe_candle, dt_to_unix, unix_to_datetime, round_to_point,
    to_serializable, now_utc, new_id
)
from notifiers.notifier_tick_updates import NotifierTickUpdates
from services.service_amqp import AMQPService
from services.service_signal_persistence import SignalPersistenceService
from strategies.base_strategy import SignalGeneratorAgent
from strategies.indicators import supertrend, stochastic, average_true_range


class AdrasteaSignalGeneratorAgent(SignalGeneratorAgent, RegistrationAwareAgent, LoggingMixin):
    """
    Generates trading signals based on the Adrastea strategy.

    This agent analyzes market data using Heikin Ashi candles, SuperTrend (fast and slow),
    and Stochastic indicators to identify potential trading opportunities (long/short).
    It manages its state across restarts using AdrasteaGeneratorStateManager and
    communicates opportunities and entry signals via AMQP.
    """

    # Indicator Configuration
    SUPER_TREND_FAST_PERIOD: int = 10
    SUPER_TREND_FAST_MULTIPLIER: float = 1.0
    SUPER_TREND_SLOW_PERIOD: int = 40
    SUPER_TREND_SLOW_MULTIPLIER: float = 3.0

    STOCH_K_PERIOD: int = 24
    STOCH_D_PERIOD: int = 5
    STOCH_SMOOTH_K: int = 3

    ATR_TP_SHORT_PERIOD: int = 2  # Used for TP calculation in SHORT trades
    ATR_TP_LONG_PERIOD: int = 5  # Used for TP calculation in LONG trades

    # Indicator DataFrame Keys
    # These keys match the output of the indicator functions in strategies.indicators
    STOCH_K_KEY: str = f"{Indicators.STOCHASTIC_K.name}_{STOCH_K_PERIOD}_{STOCH_D_PERIOD}_{STOCH_SMOOTH_K}"
    STOCH_D_KEY: str = f"{Indicators.STOCHASTIC_D.name}_{STOCH_K_PERIOD}_{STOCH_D_PERIOD}_{STOCH_SMOOTH_K}"
    SUPERTREND_FAST_KEY: str = f"{Indicators.SUPERTREND.name}_{SUPER_TREND_FAST_PERIOD}_{SUPER_TREND_FAST_MULTIPLIER}"
    SUPERTREND_SLOW_KEY: str = f"{Indicators.SUPERTREND.name}_{SUPER_TREND_SLOW_PERIOD}_{SUPER_TREND_SLOW_MULTIPLIER}"
    ATR_5_KEY: str = f"{Indicators.ATR.name}_{ATR_TP_LONG_PERIOD}"
    ATR_2_KEY: str = f"{Indicators.ATR.name}_{ATR_TP_SHORT_PERIOD}"

    # Constants
    HA_CANDLES_BUFFER_MULTIPLIER: int = 1000  # Multiplied by timeframe hours
    BOOTSTRAP_RATES_COUNT_MULTIPLIER: int = 500  # Multiplied by 1 / timeframe hours
    GAP_TOLERANCE_SECONDS: float = 5.0  # Tolerance for tick gap check
    CONDITION_5_TIME_TOLERANCE_SECONDS: int = 30  # Tolerance for condition 5 timing

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initializes the AdrasteaSignalGeneratorAgent.

        Args:
            config: The application's configuration object.
            trading_config: The specific trading configuration for this agent instance.
        """
        # Initialize parent classes first
        RegistrationAwareAgent.__init__(self, config, trading_config)
        # LoggingMixin is implicitly initialized by RegistrationAwareAgent if it inherits it,
        # otherwise, call super().__init__(config) if LoggingMixin is a direct parent.

        # Core Attributes
        self.state_manager: Optional[AdrasteaGeneratorStateManager] = None
        self.persistence_manager: Optional[SignalPersistenceService] = None
        self.amqp_s: Optional[AMQPService] = None  # Set during start

        # Strategy State
        self.initialized: bool = False  # Becomes True after bootstrap
        self.prev_condition_candle: Optional[Series] = None  # Candle meeting the previous state's condition
        self.cur_condition_candle: Optional[Series] = None  # Candle meeting the current state's condition
        self.prev_state: int = 0  # Previous state in the signal condition sequence (0-5)
        self.cur_state: int = 0  # Current state in the signal condition sequence (0-5)
        self.should_enter: bool = False  # Flag indicating if entry condition (State 5) is met
        self.active_signal_id: Optional[str] = None  # ID of the currently active opportunity signal
        self._last_processed_candle_close_time: Optional[datetime] = None  # Close time of the last processed candle (UTC naive)

        # Market State
        self.market_open_event = asyncio.Event()  # Set when market is open
        self.market_closed_duration: float = 0.0  # Cumulative duration market was closed (seconds)
        self.market_close_timestamp: Optional[datetime] = None  # Timestamp when market last closed (UTC naive)

        # Control Flags/Events
        self.bootstrap_completed_event = asyncio.Event()  # Set when bootstrap finishes
        self.first_tick: bool = True  # Flag for the very first tick processed after start/restart

        # Configuration & Calculation
        self.heikin_ashi_candles_buffer: int = int(
            self.HA_CANDLES_BUFFER_MULTIPLIER * trading_config.get_timeframe().to_hours()
        )
        bootstrap_rates_count: int = int(
            self.BOOTSTRAP_RATES_COUNT_MULTIPLIER * (1 / trading_config.get_timeframe().to_hours())
        ) if trading_config.get_timeframe().to_hours() > 0 else 500  # Avoid division by zero

        self.tot_candles_count: int = (
                self.heikin_ashi_candles_buffer
                + bootstrap_rates_count
                + self._get_minimum_frames_count()
        )

        # Logging
        self.live_candles_logger = CandlesLogger(
            config,
            trading_config.get_symbol(),
            trading_config.get_timeframe(),
            trading_config.get_trading_direction()
        )
        self.strategy_events_logger = StrategyEventsLogger(
            config,
            trading_config.get_symbol(),
            trading_config.get_timeframe(),
            trading_config.get_trading_direction()
        )

        self.debug(f"Total candles needed for processing: {self.tot_candles_count}")
        self.debug(f"Adrastea agent initialized for {self.context}")

    # Public Methods (Agent Lifecycle & Callbacks)

    @exception_handler
    async def start(self):
        """
        Starts the strategy agent.

        Initializes necessary services (state manager, persistence),
        registers for tick updates, and initiates the bootstrap process.
        This method is called after successful registration acknowledgment.
        """
        self.info("Starting Adrastea strategy agent...")

        if not await self._initialize_services():
            self.critical("Failed to initialize required services. Aborting start.")
            # Consider notifying middleware about the failure if possible
            return

        # Restore state from persistence
        await self._restore_state_from_persistence()

        # Register for tick updates
        tick_notifier = await NotifierTickUpdates.get_instance(self.config)
        await tick_notifier.register_observer(
            self.trading_config.timeframe,
            self.on_new_tick,
            self.id  # Use agent's unique ID
        )
        self.info("Registered for tick updates.")

        # Start bootstrap process in the background
        self.debug("Creating bootstrap task...")
        asyncio.create_task(self.bootstrap())
        self.debug("Bootstrap task created.")

        self.info("Adrastea strategy agent start sequence completed.")

    @exception_handler
    async def bootstrap(self):
        """
        Initializes the strategy state using historical data.

        Fetches historical candles, calculates indicators, and processes candles
        up to the second-to-last available one to set the initial state.
        The very last closed candle is left for the first `on_new_tick` call.
        """
        self.info("Starting strategy bootstrap...")
        bootstrap_candles_logger = CandlesLogger(
            self.config,
            self.trading_config.get_symbol(),
            self.trading_config.get_timeframe(),
            self.trading_config.get_trading_direction(),
            custom_name='bootstrap'
        )

        async with self.execution_lock:  # Ensure bootstrap is not interrupted
            try:
                # 1. Fetch historical candles
                self.info(f"Fetching {self.tot_candles_count} historical candles...")
                candles = await self._fetch_bootstrap_candles()
                if candles.empty:
                    self.error("Failed to retrieve candles during bootstrap. Aborting.")
                    await self._finalize_bootstrap(success=False)
                    return
                self.info(f"Retrieved {len(candles)} candles.")

                # 2. Calculate indicators on historical data
                self.info("Calculating indicators on historical data...")
                await self.calculate_indicators(candles)
                self.info("Indicator calculation complete.")

                # 3. Process historical candles (up to second-to-last)
                await self._process_historical_candles(candles, bootstrap_candles_logger)

                # 4. Finalize bootstrap state
                await self._finalize_bootstrap(success=True, last_candle_time=candles.iloc[-1]['time_close'])
                # Notify state change based on the state *after* processing the second-to-last candle
                await self._notify_state_change(candles, len(candles) - 2)  # Index of second-to-last

            except Exception as e:
                self.error("Critical error during bootstrap", exc_info=e)
                await self._finalize_bootstrap(success=False)
            finally:
                # Ensure bootstrap completion event is always set
                self.bootstrap_completed_event.set()

    @exception_handler
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        """
        Handles incoming ticks for the subscribed timeframe.

        Fetches the latest candle data, checks for time gaps, calculates indicators,
        evaluates signals based on the most recent closed candle, logs data,
        and potentially triggers opportunity or entry signals.

        Args:
            timeframe: The timeframe of the tick (should match agent's config).
            timestamp: The timestamp of the tick event (not directly used, logic relies on candle times).
        """
        # Wait for bootstrap if it's not finished
        if not self.bootstrap_completed_event.is_set():
            self.debug("Waiting for bootstrap completion...")
            await self.bootstrap_completed_event.wait()
            self.debug("Bootstrap completed. Proceeding with tick.")

        # Ensure strategy is initialized
        if not self.initialized:
            self.info("Strategy not initialized; skipping tick processing.")
            return

        async with self.execution_lock:  # Prevent concurrent processing for this agent
            try:
                # 1. Fetch latest candles
                candles = await self._fetch_latest_candles()
                if candles.empty or len(candles) < self._get_minimum_frames_count():
                    self.error(f"Insufficient candles ({len(candles) if candles is not None else 0}) retrieved. Skipping tick.")
                    return

                last_candle = candles.iloc[-1]
                last_candle_close_time = last_candle['time_close']  # Naive UTC

                # 2. Check for time gaps
                if not await self._is_candle_timing_valid(last_candle_close_time):
                    # Error/warning is logged within _is_candle_timing_valid
                    return

                # 3. Process the latest candle
                self.debug(f"Processing candle: {describe_candle(last_candle)}")
                await self._process_live_candle(candles)

                # 4. Update last processed time and reset market closed duration
                self._last_processed_candle_close_time = last_candle_close_time
                if self.market_closed_duration > 0:
                    self.debug("Resetting market_closed_duration after successful gap check.")
                    self.market_closed_duration = 0.0  # Reset after successful processing

                # 5. Persist state changes if necessary
                await self._persist_state_if_changed()

                # 6. Notify agent readiness on the very first tick processed
                if self.first_tick:
                    self.first_tick = False
                    await self.agent_is_ready()  # Notify middleware

            except Exception as e:
                self.error("Unexpected error during tick processing", exc_info=e)

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool,
                                      closing_time: Optional[float], opening_time: Optional[float],
                                      initializing: bool):
        """
        Handles market open/close events.

        Updates the internal market open status, calculates the duration the market
        was closed, and persists the closing timestamp if applicable.

        Args:
            symbol: The symbol whose status changed (should match agent's config).
            is_open: True if the market is now open, False otherwise.
            closing_time: Unix timestamp (UTC) of market close, if applicable.
            opening_time: Unix timestamp (UTC) of market open, if applicable.
            initializing: True if this is an initial status update during startup.
        """
        async with self.execution_lock:  # Protect state variables
            self.debug(f"Market status change received: symbol={symbol}, is_open={is_open}, initializing={initializing}")
            if symbol != self.trading_config.get_symbol():
                self.warning(f"Received market status for unexpected symbol {symbol}. Ignoring.")
                return

            current_status = self.market_open_event.is_set()

            if is_open:
                self.market_open_event.set()
                if not current_status and self.market_close_timestamp is not None:  # Market just opened
                    try:
                        opening_dt = unix_to_datetime(opening_time)  # Naive UTC
                        closed_duration = (opening_dt - self.market_close_timestamp).total_seconds()
                        self.market_closed_duration = max(0.0, closed_duration)
                        self.info(f"Market was closed for {self.market_closed_duration:.2f} seconds.")
                        self.market_close_timestamp = None  # Reset timestamp
                    except Exception as e:
                        self.error("Error calculating market closed duration", exc_info=e)
                        self.market_closed_duration = 0.0
                        self.market_close_timestamp = None
                else:
                    # Reset duration if initializing or already open
                    self.market_closed_duration = 0.0
                    self.market_close_timestamp = None  # Ensure timestamp is None if market is open

            else:  # Market is closed
                self.market_open_event.clear()
                if current_status:  # Market just closed
                    try:
                        self.market_close_timestamp = unix_to_datetime(closing_time)  # Store naive UTC
                        self.info(f"Market closed. Recording close timestamp: {self.market_close_timestamp}")
                    except Exception as e:
                        self.error("Error converting closing_time to datetime", exc_info=e)
                        self.market_close_timestamp = None
                # Also record timestamp if initializing and market is closed
                elif initializing and closing_time is not None and closing_time > 0:
                    try:
                        self.market_close_timestamp = unix_to_datetime(closing_time)
                        self.debug(f"Market is closed (initializing). Close timestamp recorded: {self.market_close_timestamp}")
                    except Exception as e:
                        self.error("Error converting closing_time during initialization", exc_info=e)
                        self.market_close_timestamp = None

            # Persist the market close timestamp state
            await self._persist_market_close_timestamp()

    @exception_handler
    async def stop(self):
        """Stops the strategy agent and cleans up resources."""
        self.info("Stopping Adrastea strategy agent...")
        await self.shutdown()  # Call specific shutdown logic if any

        # Save final state before stopping
        await self._persist_state_if_changed(force_save=True)
        if self.state_manager:
            await self.state_manager.stop()

        # Unregister from tick updates
        try:
            tick_notifier = await NotifierTickUpdates.get_instance(self.config)
            await tick_notifier.unregister_observer(
                self.trading_config.timeframe,
                self.id
            )
            self.info("Unregistered from tick updates.")
        except Exception as e:
            self.error("Error unregistering from tick updates during stop", exc_info=e)

        self.info("Adrastea strategy agent stopped.")

    @exception_handler
    async def shutdown(self):
        """Performs specific cleanup tasks for this agent."""
        # Currently no specific shutdown actions needed beyond 'stop'
        self.info("Executing shutdown specific tasks (if any)...")
        # Add specific cleanup here if necessary in the future
        self.info("Shutdown specific tasks complete.")

    # Private Helper Methods

    async def _initialize_services(self) -> bool:
        """Initializes State Manager and Persistence Service."""
        try:
            self.info("Initializing State Manager...")
            self.state_manager = await AdrasteaGeneratorStateManager.get_instance(
                config=self.config,
                trading_config=self.trading_config,
                instance_key=self.agent  # Use unique agent name as key
            )
            # Initialize method now handles DB connection and state loading
            # await self.state_manager.initialize() # Already called within get_instance logic
            self.info("State Manager obtained/initialized.")

            self.info("Initializing Signal Persistence Service...")
            self.persistence_manager = await SignalPersistenceService.get_instance(self.config)
            # await self.persistence_manager.start() # DB connection handled within persistence service now
            self.info("Signal Persistence Service obtained/initialized.")

            self.amqp_s = await AMQPService.get_instance()
            self.info("AMQP Service instance obtained.")
            return True
        except Exception as e:
            self.critical("Failed to initialize core services.", exc_info=e)
            return False

    async def _restore_state_from_persistence(self):
        """Loads initial state from the state manager and reconciles active signals."""
        try:
            # Load state managed by AdrasteaGeneratorStateManager
            self.active_signal_id = self.state_manager.active_signal_id
            self.market_close_timestamp = (
                unix_to_datetime(self.state_manager.market_close_timestamp)
                if self.state_manager.market_close_timestamp is not None else None
            )
            self.info(f"Loaded initial state: active_signal_id='{self.active_signal_id}', "
                      f"market_close_timestamp='{self.market_close_timestamp}'")

            # Reconcile active_signal_id with SignalPersistenceService
            await self._reconcile_active_signal()

        except Exception as e:
            self.error("Error during state restoration.", exc_info=e)
            # Reset state on error to avoid using potentially corrupt data
            self.active_signal_id = None
            self.market_close_timestamp = None

    async def _reconcile_active_signal(self):
        """Checks persistence for active signals and updates the internal state if needed."""
        try:
            last_candle_time = await self._get_last_candle_time()
            if not last_candle_time:
                self.warning("Cannot reconcile active signals without last candle time.")
                return

            active_signals: List[Signal] = await self.persistence_manager.retrieve_active_signals(
                self.trading_config.get_symbol(),
                self.trading_config.get_timeframe(),
                self.trading_config.get_trading_direction(),
                self.agent,
                last_candle_time  # Pass naive UTC time
            )
            self.debug(f"Found {len(active_signals)} active signal DTOs in persistence for reconciliation.")

            if active_signals:
                # Assuming the latest signal by creation time is the relevant one
                latest_signal = max(active_signals, key=lambda s: s.creation_tms)
                if self.active_signal_id != latest_signal.signal_id:
                    self.warning(f"Loaded active_signal_id '{self.active_signal_id}' differs from latest in SignalPersistence '{latest_signal.signal_id}'. Using latest.")
                    self.active_signal_id = latest_signal.signal_id
                else:
                    self.info(f"Loaded active_signal_id '{self.active_signal_id}' matches latest from SignalPersistence.")
            elif self.active_signal_id:
                self.warning(f"Loaded active_signal_id '{self.active_signal_id}' not found as active in SignalPersistence. Clearing state.")
                self.active_signal_id = None

            # Update the state manager with the potentially reconciled ID
            if self.state_manager.update_active_signal_id(self.active_signal_id):
                await self.state_manager.save_state()

        except Exception as e:
            self.error("Error during active signal reconciliation.", exc_info=e)

    async def _get_last_candle_time(self) -> Optional[datetime]:
        """Fetches the close time of the most recent candle."""
        try:
            # Fetch only 1 candle starting from the most recent closed one (position=1)
            candles = await self.broker().get_last_candles(
                symbol=self.trading_config.get_symbol(),
                timeframe=self.trading_config.get_timeframe(),
                count=1,
                position=1  # Start from the first completed bar
            )
            if not candles.empty:
                last_time = candles.iloc[0]['time_close']  # Naive UTC
                self.debug(f"Last candle time for signal retrieval: {last_time.isoformat()}")
                return last_time
            else:
                self.warning("Could not get last candle time from broker.")
                return None
        except Exception as e:
            self.error("Failed to fetch last candle time from broker.", exc_info=e)
            return None

    async def _fetch_bootstrap_candles(self) -> DataFrame:
        """Fetches historical candles required for bootstrapping."""
        return await self.broker().get_last_candles(
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            count=self.tot_candles_count,
            position=1  # Exclude the currently forming bar
        )

    async def _fetch_latest_candles(self) -> DataFrame:
        """Fetches the latest set of candles required for live processing."""
        # Fetch candles starting from the current bar (position=0)
        # get_last_candles handles removing the open bar if necessary
        return await self.broker().get_last_candles(
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            count=self.tot_candles_count,
            position=0
        )

    async def _process_historical_candles(self, candles: DataFrame, logger: CandlesLogger):
        """Iterates through historical candles to initialize strategy state."""
        first_index = self.heikin_ashi_candles_buffer + self._get_minimum_frames_count() - 1
        # Process up to the second-to-last candle
        last_process_index = len(candles) - 1

        self.info(f"Processing bootstrap candles from index {first_index} to {last_process_index}")
        for i in range(first_index, last_process_index + 1):  # Inclusive of the last index to process
            candle_to_log = candles.iloc[i]
            logger.add_candle(candle_to_log)  # Log candle data

            # Check signals based on the current candle 'i'
            # The state is updated based on candle 'i' relative to 'i-1'
            _should_enter, self.prev_state, self.cur_state, \
                self.prev_condition_candle, self.cur_condition_candle = self._check_signals(
                rates=candles,
                i=i,
                trading_direction=self.trading_config.get_trading_direction(),
                state=self.cur_state,
                cur_condition_candle=self.cur_condition_candle,
                log=False  # Less verbose logging during bootstrap
            )
            # Note: _should_enter is ignored during bootstrap

        self.info(f"Bootstrap candle processing complete. Final state before last candle: {self.cur_state}")

    async def _finalize_bootstrap(self, success: bool, last_candle_time: Optional[datetime] = None):
        """Sets the final state after bootstrap attempt."""
        self.initialized = success
        if success and last_candle_time:
            self._last_processed_candle_close_time = last_candle_time  # Naive UTC
            self.info(f"Bootstrap successful. Last historical candle close time: {self._last_processed_candle_close_time}")
            if not self.config.is_silent_start():
                await self._send_generator_update("🚀 Bootstrapping complete - <b>Bot ready for trading.</b>")
        elif not success:
            self.error("Bootstrap failed. Strategy agent remains uninitialized.")
            if not self.config.is_silent_start():
                await self._send_generator_update("❌ Bootstrapping failed. Bot is not ready.")
        else:  # Success but no last candle time (should not happen if candles were fetched)
            self.warning("Bootstrap successful but missing last candle time.")

        self.bootstrap_completed_event.set()  # Signal completion

    async def _process_live_candle(self, candles: DataFrame):
        """Calculates indicators and checks signals for the latest live candle."""
        try:
            # Calculate indicators on the updated DataFrame
            self.debug("Calculating indicators for live data...")
            await self.calculate_indicators(candles)
            self.debug("Indicators calculated.")

            last_candle_index = len(candles) - 1
            last_candle_with_indicators = candles.iloc[last_candle_index]
            self.info(f"Candle post-indicators: {describe_candle(last_candle_with_indicators)}")

            # Evaluate trading signals on the last candle
            self.debug("Evaluating trading signals...")
            (self.should_enter, self.prev_state, self.cur_state,
             self.prev_condition_candle, self.cur_condition_candle) = self._check_signals(
                rates=candles,
                i=last_candle_index,
                trading_direction=self.trading_config.get_trading_direction(),
                state=self.cur_state,
                cur_condition_candle=self.cur_condition_candle,
                log=True  # Enable detailed logging for live ticks
            )
            self.debug(f"Signal check result: should_enter={self.should_enter}, prev_state={self.prev_state}, cur_state={self.cur_state}.")

            # Notify state changes and handle signal generation/entry
            await self._notify_state_change(candles, last_candle_index)
            await self._handle_opportunity_or_entry(last_candle_with_indicators)

            # Log the processed candle
            self.live_candles_logger.add_candle(last_candle_with_indicators)

        except ValueError as ve:
            self.error(f"Value error during live candle processing: {ve}", exc_info=False)
        except Exception as e:
            self.error("Unexpected error processing live candle", exc_info=e)

    async def _is_candle_timing_valid(self, last_candle_close_time: datetime) -> bool:
        """
        Checks if the time gap between the last processed candle and the current
        one is valid, considering market close durations.

        Args:
            last_candle_close_time: Naive UTC close time of the latest fetched candle.

        Returns:
            True if the timing is valid, False otherwise.
        """
        if self._last_processed_candle_close_time is None:
            self.info("First tick after (re)start or no previous state, skipping gap check.")
            return True  # No previous candle to compare against

        # Ensure timestamps are naive UTC datetimes
        if not isinstance(last_candle_close_time, datetime) or not isinstance(self._last_processed_candle_close_time, datetime):
            self.warning("Skipping gap check due to invalid internal timestamp types.")
            return False  # Cannot perform check

        time_diff = last_candle_close_time - self._last_processed_candle_close_time
        gap_seconds = time_diff.total_seconds()
        candle_interval = self.trading_config.get_timeframe().to_seconds()

        # Calculate the maximum expected gap: normal interval + market closed time + tolerance
        expected_max_gap = candle_interval + self.market_closed_duration + self.GAP_TOLERANCE_SECONDS

        self.debug(
            f"Gap Check: Current candle closes at {last_candle_close_time.isoformat()}. "
            f"Last processed closed at {self._last_processed_candle_close_time.isoformat()}. "
            f"Diff: {gap_seconds:.2f}s. Market closed duration: {self.market_closed_duration:.2f}s. "
            f"Expected max gap: ~{expected_max_gap:.2f}s."
        )

        # Check for significant positive gap (missed ticks)
        if gap_seconds > expected_max_gap:
            time_over_expected = gap_seconds - expected_max_gap + self.GAP_TOLERANCE_SECONDS
            num_missed_estimate = round(time_over_expected / candle_interval) if candle_interval > 0 else 0
            ex_msg = (f"Unexpected gap detected: {gap_seconds:.2f}s > ~{expected_max_gap:.2f}s. "
                      f"Possible {max(1, num_missed_estimate)} missed tick(s). Skipping current candle.")
            self.error(ex_msg, exc_info=False)
            return False

        # Check for negative gap (clock sync issue?)
        if gap_seconds < -self.GAP_TOLERANCE_SECONDS:
            self.warning(f"Negative gap detected ({gap_seconds:.2f}s). Clock sync issue? Skipping current candle.")
            return False

        # Check for reprocessing the same candle
        # Use a small tolerance (e.g., 1 second) to account for float precision
        if abs(gap_seconds) < 1.0 and last_candle_close_time == self._last_processed_candle_close_time:
            self.debug(f"Candle closing at {last_candle_close_time.isoformat()} already processed. Skipping.")
            return False

        # Gap is within expected range
        return True

    async def _persist_state_if_changed(self, force_save: bool = False):
        """Saves the agent's state if relevant fields have changed or if forced."""
        state_changed = False
        if self.state_manager:
            # Check if active_signal_id changed
            if self.state_manager.update_active_signal_id(self.active_signal_id):
                state_changed = True
            # Check if market_close_timestamp changed (persisted in on_market_status_change)
            # No need to check here again unless logic changes

        if state_changed or force_save:
            self.debug("State changed or force_save=True, saving state...")
            if self.state_manager and await self.state_manager.save_state():
                self.debug("State saved successfully via State Manager.")
            elif self.state_manager:
                self.error("Failed to save state via State Manager.")
            else:
                self.error("State Manager not available, cannot save state.")

    async def _persist_market_close_timestamp(self):
        """Persists only the market close timestamp if it changed."""
        if self.state_manager:
            # Convert potential naive datetime back to unix float for storage
            timestamp_unix = dt_to_unix(self.market_close_timestamp) if self.market_close_timestamp else None
            if self.state_manager.update_market_close_timestamp(timestamp_unix):
                if await self.state_manager.save_state():
                    self.debug(f"Persisted market close timestamp state change (new value: {self.market_close_timestamp}).")
                else:
                    self.error("Failed to save market close timestamp state.")

    async def _handle_opportunity_or_entry(self, current_candle: pd.Series):
        """Handles logic for creating/sending opportunity or entry signals."""
        # Opportunity Signal (State 4 reached)
        if self.prev_state == 3 and self.cur_state == 4:
            await self._create_and_send_opportunity_signal(current_candle)

        # Entry Signal (State 5 reached)
        if self.should_enter:
            await self._process_entry_condition(current_candle)

    async def _create_and_send_opportunity_signal(self, opportunity_candle: pd.Series):
        """Creates a new signal DTO, saves it, and sends an opportunity message."""
        self.debug("State transition 3->4 detected (Opportunity). Creating signal...")

        signal = Signal(
            bot_name=self.config.get_bot_name(),
            instance_name=self.config.get_instance_name(),
            signal_id=new_id(length=20),  # Generate unique signal ID
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            direction=self.trading_config.get_trading_direction(),
            opportunity_candle=to_serializable(opportunity_candle),  # Candle triggering state 4
            signal_candle=None,  # Not yet applicable
            creation_tms=dt_to_unix(now_utc()),  # Naive UTC timestamp
            agent=self.agent,
            confirmed=None,  # Confirmation pending
            update_tms=None,
            user=None,
            status=SignalStatus.GENERATED
        )

        self.info(f"Generated signal opportunity: {signal.signal_id}. Saving...")
        save_ok = await self.persistence_manager.save_signal(signal)

        if save_ok:
            # Update internal state *only after* successful save
            if self.active_signal_id != signal.signal_id:
                self.active_signal_id = signal.signal_id
                # Persist this change immediately
                await self._persist_state_if_changed(force_save=True)

            # Send message to middleware
            routing_key = f"event.signal.opportunity.{self.topic}"
            self.info(f"Sending opportunity signal {signal.signal_id} to Middleware via RK: {routing_key}")
            await self._send_amqp_signal(
                exchange=RabbitExchange.jupiter_events,
                payload={"signal_id": signal.signal_id},
                routing_key=routing_key
            )
        else:
            self.error(f"Failed to save signal DTO {signal.signal_id} to persistence! Opportunity not sent.")

    async def _process_entry_condition(self, entry_trigger_candle: pd.Series):
        """Handles the logic when should_enter becomes True."""
        self.debug(f"Entry condition (State 5) met by candle closing at {entry_trigger_candle['time_close']}.")

        if not self.active_signal_id:
            self.warning("Entry condition met, but no active_signal_id found. Cannot send entry signal.")
            self.should_enter = False  # Reset flag
            self.cur_state = 0  # Reset state machine as the sequence is broken
            await self._persist_state_if_changed(force_save=True)
            return

        signal_id_to_enter = self.active_signal_id
        self.info(f"Entry condition met for active signal {signal_id_to_enter}. Preparing to send enter signal.")

        # Check Entry Validity (Market Open, Candle Timing)
        market_is_open = self.market_open_event.is_set()
        # Use the candle that triggered the state 5 transition (self.cur_condition_candle)
        # assuming it's correctly set by _update_state_machine
        is_current_candle = self._is_candle_current(self.cur_condition_candle)

        if not market_is_open or not is_current_candle:
            reason = "<b>Market Closed</b>" if not market_is_open else "<b>Historical Candle</b> (not real-time)"
            candle_time_str = self._format_candle_time_range(self.cur_condition_candle)
            notification_text = (
                f"⏳ <b>Entry Blocked</b>: Signal conditions met on candle {candle_time_str}, "
                f"but entry is blocked. Reason: {reason}."
            )
            self.warning(f"Entry for signal {signal_id_to_enter} blocked. Reason: {reason}. State reset.")
            await self._send_generator_update(notification_text)
            # Reset state as entry cannot happen
            self.active_signal_id = None
            self.cur_state = 0
            self.should_enter = False
            await self._persist_state_if_changed(force_save=True)
            return
        # End Entry Validity Check

        # Update signal status in persistence to FIRED
        update_ok = await self._update_signal_before_entry(signal_id_to_enter, entry_trigger_candle)
        if not update_ok:
            self.error(f"Failed to update signal {signal_id_to_enter} status to FIRED before sending entry. Entry aborted.")
            # Consider resetting state if update fails critically
            return

        # Send entry signal to executor via AMQP
        payload = {"signal_id": signal_id_to_enter}
        routing_key = f"event.signal.enter.{self.topic}"
        await self._send_amqp_signal(
            exchange=RabbitExchange.jupiter_events,
            payload=payload,
            routing_key=routing_key
        )
        self.info(f"Enter signal ID {signal_id_to_enter} sent to Executor via RK: {routing_key}.")

        # Reset state after successfully sending entry signal
        self.active_signal_id = None
        self.cur_state = 0  # Reset state machine after successful entry signal
        self.should_enter = False
        await self._persist_state_if_changed(force_save=True)  # Persist the reset state

    async def _update_signal_before_entry(self, signal_id: str, entry_trigger_candle: pd.Series) -> bool:
        """Updates the signal status to FIRED in persistence."""
        try:
            signal_dto: Optional[Signal] = await self.persistence_manager.get_signal(signal_id)
            if not signal_dto:
                self.error(f"Could not retrieve signal DTO {signal_id} to update before entry.")
                return False

            # Update the signal candle (the one triggering state 5) and status
            signal_dto.signal_candle = to_serializable(entry_trigger_candle)
            signal_dto.status = SignalStatus.FIRED
            signal_dto.update_tms = dt_to_unix(now_utc())

            update_dto_ok = await self.persistence_manager.update_signal_status(signal_dto)
            if not update_dto_ok:
                self.error(f"Failed to update Signal DTO {signal_id} status to FIRED.")
                return False
            self.debug(f"Successfully updated signal {signal_id} status to FIRED.")
            return True
        except Exception as e:
            self.error(f"Error updating signal DTO {signal_id} before entry.", exc_info=e)
            return False

    async def calculate_indicators(self, rates: DataFrame):
        """
        Calculates all required indicators and adds them to the rates DataFrame.

        Args:
            rates: DataFrame with OHLC or Heikin Ashi OHLC data.
        """
        await self._heikin_ashi_values(rates)  # Ensure HA values are present/updated
        # Calculate indicators using the class constants
        supertrend(self.SUPER_TREND_FAST_PERIOD, self.SUPER_TREND_FAST_MULTIPLIER, rates)
        supertrend(self.SUPER_TREND_SLOW_PERIOD, self.SUPER_TREND_SLOW_MULTIPLIER, rates)
        stochastic(self.STOCH_K_PERIOD, self.STOCH_D_PERIOD, self.STOCH_SMOOTH_K, rates)
        average_true_range(self.ATR_TP_LONG_PERIOD, rates)  # For Long TP
        average_true_range(self.ATR_TP_SHORT_PERIOD, rates)  # For Short TP
        # No return needed as DataFrame is modified in-place

    async def _heikin_ashi_values(self, df: DataFrame):
        """
        Computes and adds Heikin-Ashi OHLC values to the DataFrame.

        Modifies the DataFrame in-place.

        Args:
            df: DataFrame containing standard 'open', 'high', 'low', 'close' columns.

        Raises:
            ValueError: If the required columns are missing.
        """
        required_cols = {'open', 'high', 'low', 'close'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Input DataFrame must contain columns: {required_cols}")

        symbol_info: Optional[SymbolInfo] = await self.broker().get_market_info(self.trading_config.get_symbol())
        if not symbol_info:
            # Handle case where symbol info is not available
            self.error(f"Could not retrieve SymbolInfo for {self.trading_config.get_symbol()}. Cannot calculate HA accurately.")
            # Decide how to proceed: raise error, return, or use default point value?
            # Using a default might lead to inaccurate rounding. Raising is safer.
            raise ValueError(f"SymbolInfo not available for {self.trading_config.get_symbol()}")

        point = symbol_info.point

        # Calculate HA Close
        df['HA_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        # Calculate HA Open iteratively
        ha_open_list = [(df['open'].iloc[0] + df['close'].iloc[0]) / 2]  # First HA Open
        for i in range(1, len(df)):
            ha_open_list.append((ha_open_list[i - 1] + df['HA_close'].iloc[i - 1]) / 2)
        df['HA_open'] = pd.Series(ha_open_list, index=df.index)

        # Calculate HA High and HA Low
        df['HA_high'] = df[['HA_open', 'HA_close', 'high']].max(axis=1)
        df['HA_low'] = df[['HA_open', 'HA_close', 'low']].min(axis=1)

        # Round HA values according to symbol's point precision
        df['HA_open'] = round_to_point(df['HA_open'], point)
        df['HA_close'] = round_to_point(df['HA_close'], point)
        df['HA_high'] = round_to_point(df['HA_high'], point)
        df['HA_low'] = round_to_point(df['HA_low'], point)

    def _check_signals(
            self,
            rates: DataFrame,
            i: int,
            trading_direction: TradingDirection,
            state: int,
            cur_condition_candle: Optional[Series],
            log: bool = True
    ) -> Tuple[bool, int, int, Optional[Series], Optional[Series]]:
        """
        Checks the Adrastea strategy conditions sequentially for a given candle index.

        Updates the internal state based on whether conditions are met or fail.
        Conditions C1-C3 must be met on consecutive candles relative to the *last time a condition was met*.
        Condition C4 can be met on the same candle as C3.
        Condition C5 (entry) requires a subsequent candle within a time tolerance.

        Args:
            rates: DataFrame containing market data and calculated indicators.
            i: The index of the current candle to evaluate within the DataFrame.
            trading_direction: The trading direction (LONG or SHORT).
            state: The current state of the signal sequence (0-5).
            cur_condition_candle: The candle on which the *current* state was achieved.
            log: If True, enables detailed debug logging of condition checks.

        Returns:
            A tuple containing:
            - should_enter (bool): True if all conditions are met including the entry timing (State 5).
            - prev_state (int): The state *before* processing the current candle.
            - cur_state (int): The state *after* processing the current candle.
            - prev_condition_candle (Optional[Series]): The candle associated with the previous state.
            - cur_condition_candle (Optional[Series]): The candle associated with the new current state.
        """
        current_state = state
        previous_state = current_state  # Store the state before checks
        previous_cond_candle_out = cur_condition_candle  # Will be updated if state changes
        current_cond_candle_out = cur_condition_candle  # Will be updated if state advances

        should_enter_flag = False
        current_candle = rates.iloc[i]

        # Extract necessary data
        ha_close = current_candle['HA_close']
        supertrend_fast_prev = rates[self.SUPERTREND_FAST_KEY][i - 1]
        supertrend_slow_prev = rates[self.SUPERTREND_SLOW_KEY][i - 1]
        supertrend_fast_cur = rates[self.SUPERTREND_FAST_KEY][i]
        # supertrend_slow_cur = rates[self.SUPERTREND_SLOW_KEY][i] # Not used in conditions C1-C4
        stoch_k_cur = rates[self.STOCH_K_KEY][i]
        stoch_d_cur = rates[self.STOCH_D_KEY][i]

        is_long = (trading_direction == TradingDirection.LONG)
        is_short = (trading_direction == TradingDirection.SHORT)

        # Helper to get naive UTC Unix timestamp from a candle Series
        def get_candle_open_unix(candle: Optional[Series]) -> Optional[int]:
            if candle is None: return None
            # Assume 'time_open' is naive UTC datetime
            return int(dt_to_unix(candle['time_open']))

        current_candle_open_unix = get_candle_open_unix(current_candle)
        condition_candle_open_unix = get_candle_open_unix(cur_condition_candle)

        # Condition Checks

        # Condition 1: HA Close vs Slow Supertrend (Previous)
        # Must hold true continuously from state 1 onwards. Checked first.
        # Can advance state from 0 to 1.
        cond1_met = (is_long and ha_close >= supertrend_slow_prev) or \
                    (is_short and ha_close < supertrend_slow_prev)

        if current_state >= 1 and not cond1_met:
            # Condition 1 failed while in state 1 or higher -> Regress to state 0
            if log: self.debug(f"C1 FAIL (State {current_state}): HA={ha_close:.5f}, ST_Slow_Prev={supertrend_slow_prev:.5f}. Regressing to State 0.")
            previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 0, current_state)
        elif current_state == 0 and cond1_met:
            # Condition 1 met, advancing from state 0 to 1
            if log: self.debug(f"C1 PASS (State 0): HA={ha_close:.5f}, ST_Slow_Prev={supertrend_slow_prev:.5f}. Advancing to State 1.")
            previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 1, current_state)
        # Else: Condition 1 holds while in state >= 1, or state is 0 and C1 not met - no state change based on C1

        # Condition 2: HA Close vs Fast Supertrend (Current)
        # Only needs to be met once on a *new* candle after C1 was met.
        # Can advance state from 1 to 2. Does *not* cause regression if fails later.
        can_check_2 = current_state == 1 and \
                      current_candle_open_unix is not None and \
                      condition_candle_open_unix is not None and \
                      current_candle_open_unix > condition_candle_open_unix

        if can_check_2:
            cond2_met = (is_long and ha_close <= supertrend_fast_cur) or \
                        (is_short and ha_close > supertrend_fast_cur)
            if cond2_met:
                if log: self.debug(f"C2 PASS (State 1): HA={ha_close:.5f}, ST_Fast_Cur={supertrend_fast_cur:.5f}. Advancing to State 2.")
                previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 2, current_state)
            # else: C2 not met on this new candle, remain in state 1

        # Condition 3: HA Close vs Fast Supertrend (Previous)
        # Must hold true continuously from state 3 onwards. Checked after C2.
        # Can advance state from 2 to 3. Causes regression to state 2 if fails later.
        can_check_3 = current_state >= 2  # Check if we are in state 2 or higher

        if can_check_3:
            cond3_met = (is_long and ha_close >= supertrend_fast_prev) or \
                        (is_short and ha_close < supertrend_fast_prev)

            if cond3_met:
                if current_state == 2:  # Advance from 2 to 3
                    if log: self.debug(f"C3 PASS (State 2): HA={ha_close:.5f}, ST_Fast_Prev={supertrend_fast_prev:.5f}. Advancing to State 3.")
                    previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 3, current_state)
                # else: C3 holds while in state >= 3, no change
            elif current_state >= 3:  # Condition 3 failed while in state 3 or higher -> Regress to state 2
                if log: self.debug(f"C3 FAIL (State {current_state}): HA={ha_close:.5f}, ST_Fast_Prev={supertrend_fast_prev:.5f}. Regressing to State 2.")
                previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 2, current_state)
            # else: state is 2 and C3 not met, remain in state 2

        # Condition 4: Stochastic K vs D and D level
        # Only needs to be met once. Can happen on the *same* candle as C3 was met.
        # Can advance state from 3 to 4. Does *not* cause regression.
        can_check_4 = current_state == 3  # Check only when in state 3

        if can_check_4:
            cond4_met = (is_long and stoch_k_cur > stoch_d_cur and stoch_d_cur < 50) or \
                        (is_short and stoch_k_cur < stoch_d_cur and stoch_d_cur > 50)
            if cond4_met:
                if log: self.debug(f"C4 PASS (State 3): K={stoch_k_cur:.3f}, D={stoch_d_cur:.3f}. Advancing to State 4.")
                previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 4, current_state)
            # else: C4 not met, remain in state 3

        # Condition 5: Entry Timing
        # Checked only if we are in state 4 and on a *new* candle after C4 was met.
        can_check_5 = current_state == 4 and \
                      current_candle_open_unix is not None and \
                      condition_candle_open_unix is not None and \
                      current_candle_open_unix > condition_candle_open_unix

        if can_check_5:
            # Check if current candle's open time is within tolerance of the condition candle's *close* time
            condition_candle_close_unix = int(dt_to_unix(cur_condition_candle['time_close']))
            lower_bound = condition_candle_close_unix
            upper_bound = condition_candle_close_unix + self.CONDITION_5_TIME_TOLERANCE_SECONDS

            # Use current candle's *open* time for the check
            cond5_timing_met = lower_bound <= current_candle_open_unix <= upper_bound

            if log: self.debug(f"C5 Check (State 4): Lower={lower_bound}, Upper={upper_bound}, CurrentOpen={current_candle_open_unix}")

            if cond5_timing_met:
                if log: self.debug(f"C5 PASS (State 4): Timing condition met. Advancing to State 5 and setting should_enter.")
                previous_state, current_state, previous_cond_candle_out, current_cond_candle_out = self._update_state_machine(current_candle, previous_cond_candle_out, current_cond_candle_out, 5, current_state)
                should_enter_flag = True
            # else: Timing not met, remain in state 4 (or potentially regress if C1/C3 fail on this candle)

        if log:
            self.debug(f"CheckSignals Result: should_enter={should_enter_flag}, prev_state={previous_state}, "
                       f"cur_state={current_state}, cur_cond_candle_time={current_cond_candle_out['time_open'] if current_cond_candle_out is not None else 'None'}")

        return should_enter_flag, previous_state, current_state, previous_cond_candle_out, current_cond_candle_out

    def _update_state_machine(
            self,
            triggering_candle: Series,
            prev_cond_candle_in: Optional[Series],
            cur_cond_candle_in: Optional[Series],
            new_state: int,
            old_state: int
    ) -> Tuple[int, int, Optional[Series], Optional[Series]]:
        """
        Updates the state machine variables based on a state transition.

        Args:
            triggering_candle: The candle that caused the state transition.
            prev_cond_candle_in: The candle associated with the state before the 'old_state'.
            cur_cond_candle_in: The candle associated with the 'old_state'.
            new_state: The target state.
            old_state: The state from which the transition occurs.

        Returns:
            Tuple containing (old_state, new_state, previous_condition_candle, new_current_condition_candle).
        """
        self.debug(f"State transition from {old_state} to {new_state}")

        if new_state == 0:
            # Resetting to state 0 clears the condition candle
            new_current_condition_candle = None
        else:
            # Advancing or regressing to a state > 0 updates the candle
            new_current_condition_candle = triggering_candle

        # The 'previous' condition candle becomes the one from the 'old_state'
        previous_condition_candle = cur_cond_candle_in

        # Validate timing (should not happen if logic is correct, but as a safeguard)
        cur_time_unix = dt_to_unix(triggering_candle['time_open']) if triggering_candle is not None else None
        prev_time_unix = dt_to_unix(previous_condition_candle['time_open']) if previous_condition_candle is not None else None
        if cur_time_unix is not None and prev_time_unix is not None and cur_time_unix < prev_time_unix:
            self.error(f"Timing Error: Current candle time {triggering_candle['time_open']} "
                       f"is earlier than previous condition candle time {previous_condition_candle['time_open']}. State change might be invalid.",
                       exc_info=False)  # Log as error but don't raise to allow potential recovery

        self.debug(f"State Updated: Old={old_state}, New={new_state}, "
                   f"PrevCandleTime={previous_condition_candle['time_open'] if previous_condition_candle is not None else 'None'}, "
                   f"NewCandleTime={new_current_condition_candle['time_open'] if new_current_condition_candle is not None else 'None'}")

        return old_state, new_state, previous_condition_candle, new_current_condition_candle

    async def _notify_state_change(self, rates: DataFrame, i: int):
        """
        Logs the state change and sends a generator update message if the state changed.

        Args:
            rates: DataFrame with indicator data.
            i: Index of the current candle.
        """
        if self.cur_state == self.prev_state:
            # No state change occurred on this candle
            return

        current_candle = rates.iloc[i]
        ha_close = current_candle['HA_close']

        # Extract indicator values safely, providing defaults if keys missing
        supert_fast_prev = rates.get(self.SUPERTREND_FAST_KEY, pd.Series(dtype=float)).iloc[i - 1] if i > 0 else float('nan')
        supert_slow_prev = rates.get(self.SUPERTREND_SLOW_KEY, pd.Series(dtype=float)).iloc[i - 1] if i > 0 else float('nan')
        supert_fast_cur = current_candle.get(self.SUPERTREND_FAST_KEY, float('nan'))
        supert_slow_cur = current_candle.get(self.SUPERTREND_SLOW_KEY, float('nan'))
        stoch_k_cur = current_candle.get(self.STOCH_K_KEY, float('nan'))
        stoch_d_cur = current_candle.get(self.STOCH_D_KEY, float('nan'))

        is_long = self.trading_config.get_trading_direction() == TradingDirection.LONG
        event_message = self._get_state_change_message(ha_close, supert_fast_prev, supert_slow_prev, supert_fast_cur, stoch_k_cur, stoch_d_cur, is_long)

        if event_message:
            self.info(f"State changed: {self.prev_state} -> {self.cur_state}. Reason: {event_message}")  # Log info level
            # Log detailed event to CSV
            self.strategy_events_logger.add_event(
                time_open=current_candle['time_open'],
                time_close=current_candle['time_close'],
                close_price=ha_close,
                state_pre=self.prev_state,
                state_cur=self.cur_state,
                message=event_message,
                supert_fast_prev=supert_fast_prev,
                supert_slow_prev=supert_slow_prev,
                supert_fast_cur=supert_fast_cur,
                supert_slow_cur=supert_slow_cur,  # Included for completeness in log
                stoch_k_cur=stoch_k_cur,
                stoch_d_cur=stoch_d_cur
            )
            # Send update via AMQP
            await self._send_generator_update(f"{event_message}")  # Send only the message part

    def _get_state_change_message(self, ha_close, st_fast_prev, st_slow_prev, st_fast_cur, k, d, is_long) -> Optional[str]:
        """Generates the notification message based on the state transition."""
        # Format numbers consistently
        f = lambda x: f"{x:.5f}" if pd.notna(x) else "N/A"

        # State Change Logic (Previous State -> Current State)
        transitions = {
            (0, 1): f"1️⃣ ✅ C1 PASS: Price {f(ha_close)} {'above' if is_long else 'below'} Slow ST {f(st_slow_prev)}.",
            (1, 0): f"1️⃣ ❌ C1 FAIL: Price {f(ha_close)} {'below' if is_long else 'above'} Slow ST {f(st_slow_prev)}.",
            (1, 2): f"2️⃣ ✅ C2 PASS: Price {f(ha_close)} {'below' if is_long else 'above'} Fast ST {f(st_fast_cur)}.",
            (2, 1): f"2️⃣ ❌ C2 REGRESS: Price {f(ha_close)} no longer {'below' if is_long else 'above'} Fast ST {f(st_fast_cur)}.",  # Regression from C2 -> C1
            (2, 3): f"3️⃣ ✅ C3 PASS: Price {f(ha_close)} {'above' if is_long else 'below'} Prev Fast ST {f(st_fast_prev)}.",
            (3, 2): f"3️⃣ ❌ C3 FAIL: Price {f(ha_close)} {'below' if is_long else 'above'} Prev Fast ST {f(st_fast_prev)}.",
            (3, 4): f"4️⃣ ✅ C4 PASS: Stoch K ({f(k)}) {'crossed above' if is_long else 'crossed below'} D ({f(d)}) with D {'<50' if is_long else '>50'}.",
            (4, 3): f"4️⃣ ❌ C4 REGRESS: Stoch K ({f(k)}) no longer {'above' if is_long else 'below'} D ({f(d)}).",  # Regression from C4 -> C3
            (4, 5): f"5️⃣ ✅ C5 PASS: Entry timing condition met.",
            # Include other potential regressions for completeness, although C1/C3 failure handles most
            (5, 0): f"🔄 RESET: State reset to 0 after entry signal.",  # Assuming state resets after entry
            (5, 2): f"🔄 RESET: State reset to 2 (C3 failed after entry signal?).",  # Example hypothetical regression
            (5, 3): f"🔄 RESET: State reset to 3 (C4 failed after entry signal?).",  # Example hypothetical regression
        }
        return transitions.get((self.prev_state, self.cur_state))

    async def _send_amqp_signal(self, exchange: RabbitExchange, payload: Dict, routing_key: Optional[str] = None):
        """Helper to send messages via AMQP service."""
        if self.amqp_s is None:
            self.error("AMQP service not initialized. Cannot send message.")
            return

        # Create metadata object
        meta_inf = MessageMetaInf(
            routine_id=self.id,
            agent_name=self.agent,
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            direction=self.trading_config.get_trading_direction(),
            instance_name=self.config.get_instance_name(),
            bot_name=self.config.get_bot_name()
            # ui_token and ui_users are not typically needed for generator->middleware signals
        )

        q_message = QueueMessage(
            sender=self.agent,
            payload=payload,
            recipient="middleware",  # Default recipient for signals
            meta_inf=meta_inf
        )

        self.debug(f"Sending message to exchange {exchange.name} (RK: {routing_key}): {q_message}")
        try:
            await self.amqp_s.publish_message(
                exchange_name=exchange.name,
                message=q_message,
                routing_key=routing_key,
                exchange_type=exchange.exchange_type
            )
        except Exception as e:
            self.error(f"Failed to publish message to {exchange.name}", exc_info=e)

    async def _send_generator_update(self, message: str):
        """Sends a user-specific notification update via AMQP."""
        self.info(f"Publishing update message: {message}")
        # Use the helper method to ensure correct metadata and formatting
        await self._send_amqp_signal(
            exchange=RabbitExchange.jupiter_notifications,
            payload={"message": message},
            routing_key=f"notification.user.{self.id}"  # Target specific user queue
        )

    def _get_minimum_frames_count(self) -> int:
        """Calculates the minimum number of candles required for indicator calculations."""
        return max(
            self.SUPER_TREND_FAST_PERIOD,
            self.SUPER_TREND_SLOW_PERIOD,
            self.STOCH_K_PERIOD,
            self.STOCH_D_PERIOD,
            self.STOCH_SMOOTH_K,
            self.ATR_TP_LONG_PERIOD,
            self.ATR_TP_SHORT_PERIOD
            # Add other indicator periods here if needed
        ) + 1  # Add 1 to have a previous value available

    def _is_candle_current(self, candle: Optional[pd.Series]) -> bool:
        """Checks if the provided candle's close time is recent enough."""
        if candle is None:
            return False
        try:
            current_time_utc = now_utc()  # Naive UTC
            timeframe_seconds = self.trading_config.get_timeframe().to_seconds()
            # Buffer to account for delays (e.g., half timeframe + 15s)
            buffer_seconds = (timeframe_seconds / 2.0) + 15.0
            # Ensure candle close time is naive UTC datetime
            candle_close_time = candle['time_close']  # Already naive UTC

            is_current = candle_close_time >= (current_time_utc - timedelta(seconds=timeframe_seconds + buffer_seconds))
            if not is_current:
                self.debug(f"Candle ending {candle_close_time} is historical (Current: {current_time_utc}, Threshold: {current_time_utc - timedelta(seconds=timeframe_seconds + buffer_seconds)})")
            return is_current
        except Exception as e:
            self.error(f"Error in _is_candle_current check: {e}", exc_info=e)
            return False

    def _format_candle_time_range(self, candle: Optional[pd.Series]) -> str:
        """Formats the open and close time of a candle for display."""
        if candle is None:
            return "N/A"
        try:
            # Assume time_open and time_close are naive UTC datetimes
            open_str = candle['time_open'].strftime('%H:%M')
            close_str = candle['time_close'].strftime('%H:%M')
            return f"{open_str} - {close_str} UTC"
        except Exception:
            return "Invalid Time"
