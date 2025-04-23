# strategies/my_strategy.py
import asyncio
import pandas as pd

from datetime import datetime
from typing import Tuple, Optional, Dict, List
from pandas import Series
from agents.agent_registration_aware import RegistrationAwareAgent
from agents.generator_state_manager import AdrasteaGeneratorStateManager
from csv_loggers.logger_candles import CandlesLogger
from csv_loggers.logger_strategy_events import StrategyEventsLogger
from dto.QueueMessage import QueueMessage
from dto.Signal import Signal
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Indicators, Timeframe, TradingDirection, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import describe_candle, dt_to_unix, unix_to_datetime, round_to_point, to_serializable, \
    now_utc, new_id, get_recent_past_multiple_of_timeframe
from notifiers.notifier_tick_updates import NotifierTickUpdates
from services.service_rabbitmq import RabbitMQService
from services.service_signal_persistence import SignalPersistenceService
from strategies.base_strategy import SignalGeneratorAgent
from strategies.indicators import supertrend, stochastic, average_true_range

leverages = {
    "FOREX": [10, 30, 100],
    "METALS": [10],
    "OIL": [10],
    "CRYPTOS": [5]
}

# Indicator parameters
super_trend_fast_period = 10
super_trend_fast_multiplier = 1
super_trend_slow_period = 40
super_trend_slow_multiplier = 3

stoch_k_period = 24
stoch_d_period = 5
stoch_smooth_k = 3

# Series keys prefix
STOCHASTIC_K = Indicators.STOCHASTIC_K.name
STOCHASTIC_D = Indicators.STOCHASTIC_D.name
SUPER_TREND = Indicators.SUPERTREND.name
MOVING_AVERAGE = Indicators.MOVING_AVERAGE.name
ATR = Indicators.ATR.name

# Indicators series keys
supertrend_fast_key = SUPER_TREND + '_' + str(super_trend_fast_period) + '_' + str(super_trend_fast_multiplier)
supertrend_slow_key = SUPER_TREND + '_' + str(super_trend_slow_period) + '_' + str(super_trend_slow_multiplier)
stoch_k_key = STOCHASTIC_K + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)
stoch_d_key = STOCHASTIC_D + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)


class AdrasteaSignalGeneratorAgent(SignalGeneratorAgent, RegistrationAwareAgent, LoggingMixin):
    """
    Concrete implementation of the trading strategy.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        """
        Initialize the strategy agent with the given configuration and trading parameters.

        Sets up internal state variables, logging, and required buffers.
        """
        RegistrationAwareAgent.__init__(self, config, trading_config)
        # Internal state initialization
        self.db_service = None
        self.initialized = False
        self.prev_condition_candle = None
        self.cur_condition_candle = None
        self.prev_state = 0  # Initial state
        self.cur_state = 0  # Initial state
        self.should_enter = False
        self.heikin_ashi_candles_buffer = int(1000 * trading_config.get_timeframe().to_hours())
        self.market_open_event = asyncio.Event()
        self.bootstrap_completed_event = asyncio.Event()
        self.live_candles_logger = CandlesLogger(config, trading_config.get_symbol(), trading_config.get_timeframe(), trading_config.get_trading_direction())
        self.countries_of_interest = []
        bootstrap_rates_count = int(500 * (1 / trading_config.get_timeframe().to_hours()))
        self.tot_candles_count = self.heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()
        self._last_processed_candle_close_time: Optional[datetime] = None  # Stores the close time of the last successfully processed candle
        self.market_closed_duration = 0.0  # Cumulative market closed duration in seconds
        self.market_close_timestamp = None  # Timestamp when market closed
        self.gap_tolerance_seconds = 5.0  # Tolerance for gap check in seconds
        self.persistence_manager = None
        self.active_signal_id = None
        self.state_manager = None
        self.debug(f"Calculated total of {self.tot_candles_count} candles needed for strategy processing")

    @exception_handler
    async def start(self):
        """
        Start the strategy by registering observers for economic events and tick updates,
        and then run the bootstrap process.
        """
        self.info("Starting the strategy.")

        try:
            self.info("Initializing State Manager...")

            self.state_manager = await AdrasteaGeneratorStateManager.get_instance(
                config=self.config,
                trading_config=self.trading_config
            )

            await self.state_manager.initialize()
            self.info("State Manager initialized successfully.")

            self.active_signal_id = self.state_manager.active_signal_id
            self.market_close_timestamp = self.state_manager.market_close_timestamp

            self.info(f"Loaded initial state: active_signal_id='{self.active_signal_id}', market_close_timestamp='{self.market_close_timestamp}'")

        except Exception as e:
            self.critical("Failed to initialize State Manager or load initial state. Aborting start.", exec_info=e)
            return

        self.persistence_manager = await SignalPersistenceService.get_instance(self.config)

        # Restore current active signal id if present (in case of reboot between an opportunity and an enter signal
        try:
            self.persistence_manager = await SignalPersistenceService.get_instance(self.config)
            self.info("SignalPersistenceService instance obtained.")

            # Get last candle time to check for relevant signals
            last_candle_time = None
            candles = await self.broker().get_last_candles(symbol=self.trading_config.get_symbol(),
                                                           timeframe=self.trading_config.get_timeframe(), count=1,
                                                           position=0)
            if not candles.empty:
                last_candle_time = candles.iloc[0]['time_close']
                self.debug(f"Last candle time for signal retrieval: {last_candle_time.isoformat()}")
            else:
                self.warning("Could not get last candle time for retrieving active signals.")

            # Reconcile loaded active_signal_id with SignalPersistenceService results
            if last_candle_time and self.persistence_manager:
                active_signals: List[Signal] = await self.persistence_manager.retrieve_active_signals(
                    self.trading_config.get_symbol(), self.trading_config.get_timeframe(),
                    self.trading_config.get_trading_direction(), self.agent, last_candle_time
                )
                self.debug(f"Found {len(active_signals)} active signal DTOs in persistence.")

                if active_signals:
                    latest_signal = max(active_signals, key=lambda s: s.creation_tms)

                    if self.active_signal_id != latest_signal.signal_id:
                        self.warning(f"Loaded active_signal_id '{self.active_signal_id}' differs from latest in SignalPersistence '{latest_signal.signal_id}'. Using latest.")
                        self.active_signal_id = latest_signal.signal_id
                    else:
                        self.info(f"Loaded active_signal_id '{self.active_signal_id}' matches latest from SignalPersistence.")
                elif self.active_signal_id:
                    self.warning(f"Loaded active_signal_id '{self.active_signal_id}' not found as active in SignalPersistence. Clearing state.")
                    self.active_signal_id = None

                if self.state_manager.update_active_signal_id(self.active_signal_id):
                    await self.state_manager.save_state()

            elif not last_candle_time:
                self.warning("Cannot reconcile active signals without last candle time.")
            else:
                self.error("SignalPersistenceService not initialized, cannot reconcile active signals.")

        except Exception as e:
            self.error("Error during SignalPersistenceService initialization or signal reconciliation.", exec_info=e)

        tick_notif = await NotifierTickUpdates.get_instance(self.config)
        await tick_notif.register_observer(
            self.trading_config.timeframe,
            self.on_new_tick,
            self.id
        )

        self.debug("Creating bootstrap task...")
        asyncio.create_task(self.bootstrap())
        self.debug("Bootstrap task created.")

        self.info("Strategy start sequence completed. Bootstrap running in background.")

    @exception_handler
    async def stop(self):
        """
        Stop the strategy and unregister observers.
        """
        self.info("Stopping the strategy.")
        await self.shutdown()

        self.info("Saving final state before stopping...")
        try:
            changed = self.state_manager.update_active_signal_id(self.active_signal_id)
            changed |= self.state_manager.update_market_close_timestamp(self.market_close_timestamp)
            if await self.state_manager.save_state():
                self.info("Agent state saved successfully.")
            else:
                self.error("Failed to save agent state.")
        except Exception as e:
            self.error("Exception during final state save.", exec_info=e)

        await self.state_manager.stop()

        tick_notif = await NotifierTickUpdates.get_instance(self.config)
        await tick_notif.unregister_observer(
            self.trading_config.timeframe,
            self.id
        )

    def get_minimum_frames_count(self):
        """
        Return the minimum number of frames required for indicator calculation.
        """
        return max(super_trend_fast_period,
                   super_trend_slow_period,
                   stoch_k_period,
                   stoch_d_period,
                   stoch_smooth_k) + 1

    async def notify_state_change(self, rates, i):
        """
        Log and notify the state change based on the current candle's indicators.

        Sends an event update with relevant state and indicator details.
        """
        symbol, timeframe, trading_direction = (
            self.trading_config.get_symbol(), self.trading_config.get_timeframe(),
            self.trading_config.get_trading_direction()
        )
        events_logger = StrategyEventsLogger(self.config, symbol, timeframe, trading_direction)
        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']

        # Extract indicator values from the candles
        supert_fast_prev = rates[supertrend_fast_key][i - 1]
        supert_slow_prev = rates[supertrend_slow_key][i - 1]
        supert_fast_cur = rates[supertrend_fast_key][i]
        supert_slow_cur = rates[supertrend_slow_key][i]
        stoch_k_cur = rates[stoch_k_key][i]
        stoch_d_cur = rates[stoch_d_key][i]

        is_long = trading_direction == TradingDirection.LONG
        is_short = trading_direction == TradingDirection.SHORT

        async def _notify_event(event):
            self.debug(event)
            events_logger.add_event(
                time_open=cur_candle['time_open'],
                time_close=cur_candle['time_close'],
                close_price=close,
                state_pre=self.prev_state,
                state_cur=self.cur_state,
                message=event,
                supert_fast_prev=supert_fast_prev,
                supert_slow_prev=supert_slow_prev,
                supert_fast_cur=supert_fast_cur,
                supert_slow_cur=supert_slow_cur,
                stoch_k_cur=stoch_k_cur,
                stoch_d_cur=stoch_d_cur
            )
            await self.send_generator_update(event)

        # Process state transitions and send notifications based on the conditions
        if self.cur_state == 1 and self.prev_state == 0:
            if is_long:
                await _notify_event(
                    f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is above the slow Supertrend level {supert_slow_prev:.3f}, supporting a long position.")
            elif is_short:
                await _notify_event(
                    f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is below the slow Supertrend level {supert_slow_prev:.3f}, supporting a short position.")
        elif self.cur_state == 0 and self.prev_state == 1:
            if is_long:
                await _notify_event(
                    f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now below the slow Supertrend level {supert_slow_prev:.3f}, invalidating the long signal.")
            elif is_short:
                await _notify_event(
                    f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now above the slow Supertrend level {supert_slow_prev:.3f}, invalidating the short signal.")
        elif self.cur_state == 2 and self.prev_state == 1:
            if is_long:
                await _notify_event(
                    f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is below the fast Supertrend level {supert_fast_cur:.3f}, favorable for a long trade.")
            elif is_short:
                await _notify_event(
                    f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is above the fast Supertrend level {supert_fast_cur:.3f}, favorable for a short trade.")
        elif self.cur_state == 1 and self.prev_state == 2:
            if is_long:
                await _notify_event(
                    f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} is no longer below the fast Supertrend level {supert_fast_cur:.3f}.")
            elif is_short:
                await _notify_event(
                    f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} is no longer above the fast Supertrend level {supert_fast_cur:.3f}.")
        elif self.cur_state == 3 and self.prev_state == 2:
            if is_long:
                await _notify_event(
                    f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains above the fast Supertrend level {supert_fast_prev:.3f}, confirming a long trade.")
            elif is_short:
                await _notify_event(
                    f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains below the fast Supertrend level {supert_fast_prev:.3f}, confirming a short trade.")
        elif self.cur_state == 2 and self.prev_state == 3:
            if is_long:
                await _notify_event(
                    f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} is no longer above the fast Supertrend level {supert_fast_prev:.3f}, invalidating the long signal.")
            elif is_short:
                await _notify_event(
                    f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} is no longer below the fast Supertrend level {supert_fast_prev:.3f}, invalidating the short signal.")
        elif self.cur_state == 4 and self.prev_state == 3:
            if is_long:
                await _notify_event(
                    f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur:.3f}) crossed above D ({stoch_d_cur:.3f}) with D below 50, indicating bullish momentum.")
            elif is_short:
                await _notify_event(
                    f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur:.3f}) crossed below D ({stoch_d_cur:.3f}) with D above 50, indicating bearish momentum.")
        elif self.cur_state == 3 and self.prev_state == 4:
            if is_long:
                await _notify_event(
                    f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur:.3f}) is no longer above D ({stoch_d_cur:.3f}).")
            elif is_short:
                await _notify_event(
                    f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur:.3f}) is no longer below D ({stoch_d_cur:.3f}).")

        elif self.should_enter:
            dir_str = "long" if is_long else "short"
            cur_candle_time = f"{cur_candle['time_open'].strftime('%H:%M')} - {cur_candle['time_close'].strftime('%H:%M')}"
            last_condition_candle_time = f"{self.cur_condition_candle['time_open'].strftime('%H:%M')} - {self.cur_condition_candle['time_close'].strftime('%H:%M')}"
            await _notify_event(
                f"5️⃣ ✅ <b>Condition 5 matched</b>: Final signal generated for {dir_str} trade. Current candle time {cur_candle_time} follows the last condition candle: {last_condition_candle_time}"
            )

    @exception_handler
    async def bootstrap(self):
        """
        Bootstrap the strategy by fetching historical candles, calculating indicators, and processing the bootstrap loop to initialize the strategy state.
        The bootstrap process works till the second-last closed candle. This way, the first live loop will rise possible opportunities or entry signals.
        """
        self.info("Initializing the strategy bootstrap.")

        async with self.execution_lock:
            timeframe = self.trading_config.get_timeframe()
            symbol = self.trading_config.get_symbol()
            trading_direction = self.trading_config.get_trading_direction()

            self.debug(f"Configuration - Symbol: {symbol}, Timeframe: {timeframe}, Direction: {trading_direction}")

            try:
                bootstrap_candles_logger = CandlesLogger(self.config, symbol, timeframe, trading_direction,
                                                         custom_name='bootstrap')
                self.info(f"Fetching {self.tot_candles_count} candles...")
                candles = await self.broker().get_last_candles(
                    symbol=self.trading_config.get_symbol(),
                    timeframe=self.trading_config.get_timeframe(),
                    count=self.tot_candles_count,
                    position=1
                )
                self.info(f"Retrieved {len(candles)} candles.")

                if candles.empty:
                    self.error("Failed to retrieve candles during bootstrap. Aborting bootstrap process.")
                    self.initialized = False
                    self.bootstrap_completed_event.set()
                    return

                self.info("Calculating indicators on historical candles.")
                await self.calculate_indicators(candles)
                self.info("Indicators calculation completed.")

                first_index = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
                last_index = len(candles)

                self.info(f"Processing bootstrap candles from index {first_index} to {last_index - 1}")
                for i in range(first_index, last_index):
                    bootstrap_candles_logger.add_candle(candles.iloc[i])
                    self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                        rates=candles,
                        i=i,
                        trading_direction=trading_direction,
                        state=self.cur_state,
                        cur_condition_candle=self.cur_condition_candle,
                        log=False
                    )

                self.info(f"Bootstrap complete - Final state: {self.cur_state}")
                if not self.config.is_silent_start():
                    await self.send_generator_update("🚀 Bootstrapping complete - <b>Bot ready for trading.</b>")
                await self.notify_state_change(candles, last_index - 1)
                self.initialized = True
                self._last_processed_candle_close_time = candles.iloc[-1]['time_close']
                self.debug(f"Last bootstrap candle close time updated to: {self._last_processed_candle_close_time}")
                self.bootstrap_completed_event.set()

            except Exception as e:
                self.error("Error during strategy bootstrap", exec_info=e)
                self.initialized = False
                self.bootstrap_completed_event.set()

    @exception_handler  # Assuming @exception_handler decorator exists and works
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float,
                                      initializing: bool):
        """
        Handle changes in market status by setting or clearing the market open event and calculating
        the market closed duration.
        """
        async with self.execution_lock:
            self.debug(f"Acquired execution lock for market status change: symbol={symbol}, is_open={is_open}, initializing={initializing}")  # DEBUG log inside lock
            if is_open:
                self.market_open_event.set()
                if not initializing and self.market_close_timestamp is not None:
                    try:
                        opening_dt = unix_to_datetime(opening_time)
                        closed_duration = (opening_dt - self.market_close_timestamp).total_seconds()
                        self.market_closed_duration = max(0.0, closed_duration)
                        self.info(f"Market ({symbol}) was closed for {self.market_closed_duration:.2f} seconds.")
                        self.market_close_timestamp = None
                    except Exception as e:
                        self.error(f"Error calculating market closed duration for {symbol}", exec_info=e)
                        self.market_closed_duration = 0.0
                        self.market_close_timestamp = None
                else:
                    # Reset duration if initializing or if it was already open
                    self.market_closed_duration = 0.0
                    if initializing:
                        self.debug(f"Market ({symbol}) is open (initializing). Duration reset.")
                    elif self.market_close_timestamp is None:
                        self.debug(f"Market ({symbol}) is open (was already open or first status). Duration reset.")


            else:  # Market is closed
                self.market_open_event.clear()
                if not initializing:
                    self.info(f"Market ({symbol}) closing detected; recording closing timestamp.")
                    try:
                        self.market_close_timestamp = unix_to_datetime(closing_time)
                        self.debug(f"Market ({symbol}) close timestamp recorded: {self.market_close_timestamp}")  # DEBUG log for timestamp recording
                    except Exception as e:
                        self.error(f"Error converting closing_time to datetime for {symbol}", exec_info=e)
                        self.market_close_timestamp = None
                else:
                    # If initializing and market is closed, still try to record the timestamp if provided
                    if closing_time > 0:
                        try:
                            self.market_close_timestamp = unix_to_datetime(closing_time)
                            self.debug(f"Market ({symbol}) is closed (initializing). Close timestamp recorded: {self.market_close_timestamp}")
                        except Exception as e:
                            self.error(f"Error converting closing_time during initialization for {symbol}", exec_info=e)
                            self.market_close_timestamp = None
                    else:
                        self.debug(f"Market ({symbol}) is closed (initializing), no valid closing_time provided.")
                        self.market_close_timestamp = None

            # Persist state regardless of open/closed status change if the timestamp value changed
            if self.state_manager.update_market_close_timestamp(self.market_close_timestamp):
                if await self.state_manager.save_state():
                    self.debug(f"Attempting to persist market close timestamp state change (new value: {self.market_close_timestamp}).")  # DEBUG log before save
                    self.info(f"Persisted market close timestamp state change for {symbol} (new value: {self.market_close_timestamp}).")
                else:
                    self.error(f"Failed to save market status state for {symbol}.")
            self.debug(f"Releasing execution lock for market status change: symbol={symbol}")  # DEBUG log before releasing lock

    @exception_handler
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        """
        Process incoming ticks: checks gap using persisted timestamps,
        recalculates indicators, evaluates signals, logs candle data, and
        persists state changes for active_signal_id or last_processed_candle_timestamp.
        Saves state if active_signal_id changes or after successfully processing the tick.
        """
        # Wait for bootstrap to complete if it hasn't already
        if not self.bootstrap_completed_event.is_set():
            self.debug("Waiting for bootstrap completion before processing tick...")
            await self.bootstrap_completed_event.wait()

        # Use async lock to prevent concurrent processing for this agent instance
        async with self.execution_lock:
            if not self.initialized:
                self.info("Strategy not initialized; skipping tick processing.")
                return

            symbol = self.trading_config.get_symbol()
            timeframe = self.trading_config.get_timeframe()
            trading_direction = self.trading_config.get_trading_direction()

            # Candle Fetching
            candles: Optional[pd.DataFrame] = None
            try:
                main_loop = asyncio.get_running_loop()

                def run_get_last_candles():
                    # Ensure broker() method exists and returns a valid broker interface
                    # Ensure get_last_candles is called correctly
                    future = asyncio.run_coroutine_threadsafe(
                        self.broker().get_last_candles(symbol, timeframe, self.tot_candles_count),
                        main_loop
                    )
                    return future.result()

                candles = await asyncio.to_thread(run_get_last_candles)
                # Validate received candles
                if candles is None or candles.empty or len(candles) < self.get_minimum_frames_count():
                    self.error(f"Insufficient candles ({len(candles) if candles is not None else 0}) retrieved for {symbol} {timeframe.name}. Skipping tick.")
                    return
                self.debug(f"Retrieved {len(candles)} candles for {symbol} {timeframe.name}.")
            except Exception as e:
                self.error(f"Exception during candle retrieval for {symbol} {timeframe.name}", exec_info=e)
                return

            if candles is None:
                self.error("Candles DataFrame is None after fetch attempt. Skipping tick.")
                return

            last_candle: pd.Series = candles.iloc[-1]
            last_candle_close_time: datetime = last_candle['time_close']

            # Gap Check using loaded/internal state
            if self._last_processed_candle_close_time is not None:
                # Ensure timestamps are valid datetime objects before comparison
                if isinstance(last_candle_close_time, datetime) and isinstance(self._last_processed_candle_close_time, datetime):
                    time_diff = last_candle_close_time - self._last_processed_candle_close_time
                    gap_seconds = time_diff.total_seconds()
                    candle_interval = timeframe.to_seconds()
                    expected_max_gap = self.market_closed_duration + candle_interval + self.gap_tolerance_seconds

                    self.debug(
                        f"Gap Check: Time diff: {gap_seconds:.2f}s since last proc @ {self._last_processed_candle_close_time.isoformat()}. "
                        f"Market closed duration: {self.market_closed_duration:.2f}s. Expected max gap: ~{expected_max_gap:.2f}s."
                    )

                    if gap_seconds > expected_max_gap:
                        time_over_expected = gap_seconds - expected_max_gap + self.gap_tolerance_seconds
                        num_missed_estimate = round(time_over_expected / candle_interval) if candle_interval > 0 else 0
                        ex_msg = (f"Unexpected gap detected: {gap_seconds:.2f}s > ~{expected_max_gap:.2f}s. "
                                  f"Possible {max(1, num_missed_estimate)} missed tick(s). Skipping.")
                        self.error(ex_msg, exec_info=False)

                    if gap_seconds < -self.gap_tolerance_seconds:
                        self.warning(f"Negative gap detected ({gap_seconds:.2f}s). Check clock sync? Skipping.")
                        return

                    if abs(gap_seconds) < 1 and last_candle_close_time == self._last_processed_candle_close_time:
                        self.debug(f"Candle {last_candle_close_time.isoformat()} already processed. Skipping.")
                        return

                    if self.market_closed_duration > 0:
                        self.market_closed_duration = 0.0
                        self.debug("Reset internal market_closed_duration after successful gap check.")
                else:
                    self.warning("Skipping gap check due to invalid internal timestamp types.")
            else:
                self.info("First tick after (re)start or no previous state, skipping gap check.")
            # End Gap Check

            # Process Candle
            state_changed_this_tick = False  # Flag to save state only once at the end if needed
            try:
                self.debug(f"Processing candle: {describe_candle(last_candle)}")

                # Calculate Indicators
                self.debug("Calculating indicators...")

                def run_indicators():  # Run sync code in thread
                    # Ensure self.calculate_indicators exists and is callable
                    future = asyncio.run_coroutine_threadsafe(self.calculate_indicators(candles), main_loop)
                    return future.result()

                await asyncio.to_thread(run_indicators)
                self.debug("Indicators calculated.")

                last_candle_with_indicators = candles.iloc[-1]
                self.info(f"Candle post-indicators: {describe_candle(last_candle_with_indicators)}")

                # Evaluate trading signals
                self.debug("Evaluating trading signals...")
                (self.should_enter, self.prev_state, self.cur_state,
                 self.prev_condition_candle, self.cur_condition_candle) = self.check_signals(
                    rates=candles, i=len(candles) - 1,  # Pass DataFrame
                    trading_direction=self.trading_config.get_trading_direction(),
                    state=self.cur_state, cur_condition_candle=self.cur_condition_candle, log=True
                )
                self.debug(
                    f"Signal check result: should_enter={self.should_enter}, prev_state={self.prev_state}, cur_state={self.cur_state}.")

                self.debug("Notifying state change (via callback/event)...")
                await self.notify_state_change(candles, len(candles) - 1)

                topic = f"{symbol}.{timeframe.name}.{self.trading_config.get_trading_direction().name}"

                if self.prev_state == 3 and self.cur_state == 4:
                    self.debug("State transition 3->4 detected (Opportunity).")

                    signal = Signal(
                        bot_name=self.config.get_bot_name(),
                        instance_name=self.config.get_instance_name(),
                        signal_id=new_id(length=20),
                        symbol=symbol,
                        timeframe=timeframe,
                        direction=self.trading_config.get_trading_direction(),
                        cur_candle=to_serializable(self.cur_condition_candle) if isinstance(self.cur_condition_candle, pd.Series) else None,
                        prev_candle=None,
                        routine_id=self.id,
                        creation_tms=dt_to_unix(now_utc()),
                        agent=self.agent,
                        confirmed=None,
                        update_tms=None,
                        user=None
                    )
                    self.info(f"Generated signal opportunity: {signal.signal_id}. Saving DTO...")

                    save_dto_ok = await self.persistence_manager.save_signal(signal)
                    if save_dto_ok:
                        if self.active_signal_id != signal.signal_id:
                            self.active_signal_id = signal.signal_id
                        routing_key = f"event.signal.opportunity.{topic}"
                        self.info(f"Sending opportunity signal {signal.signal_id} to Middleware via RK: {routing_key}")
                        await self.send_queue_message(
                            exchange=RabbitExchange.jupiter_events,
                            payload={"signal_id": signal.signal_id},
                            routing_key=routing_key
                        )
                    else:
                        self.error(f"Failed to save signal DTO {signal.signal_id} to persistence!")

                if self.should_enter:
                    if self.active_signal_id:
                        signal_id_to_enter = self.active_signal_id
                        self.info(f"Entry condition met for active signal {signal_id_to_enter}. Sending enter signal.")
                        if self.persistence_manager:
                            try:
                                signal_dto: Optional[Signal] = await self.persistence_manager.get_signal(signal_id_to_enter)
                                if signal_dto:
                                    signal_dto.prev_candle = to_serializable(self.prev_condition_candle) if isinstance(self.prev_condition_candle, pd.Series) else signal_dto.cur_candle
                                    signal_dto.cur_candle = to_serializable(self.cur_condition_candle) if isinstance(self.cur_condition_candle, pd.Series) else None
                                    signal_dto.update_tms = dt_to_unix(now_utc())
                                    update_dto_ok = await self.persistence_manager.update_signal_status(signal_dto)
                                    if not update_dto_ok: self.error(f"Failed to update Signal DTO {signal_id_to_enter} status.")
                                else:
                                    self.error(f"Could not retrieve signal DTO {signal_id_to_enter} to update before entry.")
                            except Exception as dto_e:
                                self.error(f"Error updating signal DTO {signal_id_to_enter} before entry.", exec_info=dto_e)
                        else:
                            self.error("SignalPersistenceManager not available to update signal DTO.")

                        payload = {"signal_id": signal_id_to_enter}
                        routing_key = f"event.signal.enter.{topic}"

                        await self.send_queue_message(exchange=RabbitExchange.jupiter_events, payload=payload, routing_key=routing_key)
                        self.debug(f"Enter signal ID {signal_id_to_enter} sent to Executor.")

                        self.active_signal_id = None
                        self.info(f"Cleared active signal {signal_id_to_enter} state after entry.")
                    else:
                        self.warning("Entry condition met, but no active_signal_id was found to send.")

                self._last_processed_candle_close_time = last_candle_close_time  # Update internal tracker

                # Log Candle Data
                try:
                    self.live_candles_logger.add_candle(last_candle_with_indicators)
                except Exception as log_e:
                    self.error("Error logging live candle data", exec_info=log_e)

                if self.state_manager.update_active_signal_id(self.active_signal_id):
                    state_changed_this_tick = True

            except ValueError as ve:  # Catch specific errors like gap check failure
                self.error(f"Value error during processing for candle {last_candle_close_time}: {ve}", exec_info=False)
                return
            except Exception as process_e:
                self.error(f"Unexpected error during processing candle {last_candle_close_time}", exec_info=process_e)
                return

            finally:
                if state_changed_this_tick:
                    self.debug("State changed during tick processing, saving state...")
                    if await self.state_manager.save_state():
                        self.debug("State saved successfully after tick.")
                    else:
                        self.error("Failed to save state after tick processing.")

    @exception_handler
    async def calculate_indicators(self, rates):
        """
        Convert candles to Heikin Ashi and calculate all required indicators.
        """
        await self.heikin_ashi_values(rates)
        supertrend(super_trend_fast_period, super_trend_fast_multiplier, rates)
        supertrend(super_trend_slow_period, super_trend_slow_multiplier, rates)
        stochastic(stoch_k_period, stoch_d_period, stoch_smooth_k, rates)
        average_true_range(5, rates)
        average_true_range(2, rates)
        return rates

    @exception_handler
    async def heikin_ashi_values(self, df):
        """
        Compute and add Heikin-Ashi values to the DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain the required columns.
        """
        if not isinstance(df, pd.DataFrame) or not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("Input must be a DataFrame with 'open', 'high', 'low', and 'close' columns.")
        symbol_info: SymbolInfo = await self.broker().get_market_info(self.trading_config.get_symbol())
        df['HA_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [(df['open'][0] + df['close'][0]) / 2]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i - 1] + df['HA_close'].iloc[i - 1]) / 2)
        df['HA_open'] = pd.Series(ha_open, index=df.index)
        df['HA_high'] = df[['HA_open', 'HA_close', 'high']].max(axis=1)
        df['HA_low'] = df[['HA_open', 'HA_close', 'low']].min(axis=1)
        df['HA_open'] = round_to_point(df['HA_open'], symbol_info.point)
        df['HA_close'] = round_to_point(df['HA_close'], symbol_info.point)
        df['HA_high'] = round_to_point(df['HA_high'], symbol_info.point)
        df['HA_low'] = round_to_point(df['HA_low'], symbol_info.point)
        return df

    def check_signals(
            self,
            rates: pd.DataFrame,
            i: int,
            trading_direction: TradingDirection,
            state=None,
            cur_condition_candle=None,
            log: bool = True
    ) -> (bool, int, int, pd.DataFrame):
        """
        Analyze market conditions and update the internal state to determine if trade entry conditions are met.

        Parameters:
            rates (dict): Market data rates including time, close, and indicator values.
            i (int): Current index in the rates to evaluate.
            trading_direction (TradingDirection): The trading direction (LONG or SHORT).
            state (int, optional): The current state; defaults to 0.
            cur_condition_candle (DataFrame, optional): The last candle that met a condition.
            log (bool, optional): Enable detailed logging; defaults to True.

        Returns:
            should_enter (bool): True if trade entry conditions are satisfied.
            prev_state (int): The previous state value.
            cur_state (int): The updated state value.
            prev_condition_candle (Series): The previously matched condition candle.
            cur_condition_candle (Series): The latest matched condition candle.
        """
        cur_state = state if state is not None else 0
        prev_state = cur_state
        prev_condition_candle = cur_condition_candle
        should_enter = False
        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']
        supert_fast_prev, supert_slow_prev = rates[supertrend_fast_key][i - 1], rates[supertrend_slow_key][i - 1]
        supert_fast_cur = rates[supertrend_fast_key][i]
        stoch_k_cur, stoch_d_cur = rates[stoch_k_key][i], rates[stoch_d_key][i]
        is_long, is_short = trading_direction == TradingDirection.LONG, trading_direction == TradingDirection.SHORT
        int_time_open = lambda candle: -1 if candle is None else int(candle['time_open'].timestamp())
        int_time_close = lambda candle: -1 if candle is None else int(candle['time_close'].timestamp())

        # Condition 1
        can_check_1 = cur_state >= 0 and int_time_open(cur_candle) >= int_time_open(cur_condition_candle)
        if log: self.debug(f"Can check condition 1: {can_check_1}")
        if can_check_1:
            if log: self.debug(
                f"Before condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond1 = (is_long and close >= supert_slow_prev) or (is_short and close < supert_slow_prev)
            if cond1:
                if cur_state == 0:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                           prev_condition_candle,
                                                                                                           cur_condition_candle,
                                                                                                           1, cur_state)
            elif cur_state >= 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                       prev_condition_candle,
                                                                                                       cur_condition_candle,
                                                                                                       0, cur_state)
            if log: self.debug(
                f"After condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 2
        can_check_2 = cur_state >= 1 and int_time_open(cur_candle) > int_time_open(cur_condition_candle)
        if log: self.debug(f"Can check condition 2: {can_check_2}")
        if can_check_2:
            if log: self.debug(
                f"Before condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond2 = (is_long and close <= supert_fast_cur) or (is_short and close > supert_fast_cur)
            if cond2 and cur_state == 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                       prev_condition_candle,
                                                                                                       cur_condition_candle,
                                                                                                       2, cur_state)
            if log: self.debug(
                f"After condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 3
        can_check_3 = cur_state >= 2 and int_time_open(cur_candle) >= int_time_open(cur_condition_candle)
        if log: self.debug(f"Can check condition 3: {can_check_3}")
        if can_check_3:
            if log: self.debug(
                f"Before condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond3 = (is_long and close >= supert_fast_prev) or (is_short and close < supert_fast_prev)
            if cond3:
                if cur_state == 2:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                           prev_condition_candle,
                                                                                                           cur_condition_candle,
                                                                                                           3, cur_state)
            elif cur_state >= 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                       prev_condition_candle,
                                                                                                       cur_condition_candle,
                                                                                                       2, cur_state)
            if log: self.debug(
                f"After condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 4 (Stochastic)
        can_check_4 = cur_state >= 3
        if log: self.debug(f"Can check condition 4: {can_check_4}")
        if can_check_4:
            if log: self.debug(
                f"Before condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond4 = (is_long and stoch_k_cur > stoch_d_cur and stoch_d_cur < 50) or (
                    is_short and stoch_k_cur < stoch_d_cur and stoch_d_cur > 50)
            if cond4 and cur_state == 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                       prev_condition_candle,
                                                                                                       cur_condition_candle,
                                                                                                       4, cur_state)
            if log: self.debug(
                f"After condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 5 (Final condition for entry)
        time_tolerance = 30
        can_check_5 = cur_state == 4 and int(cur_candle['time_open'].timestamp()) > int(
            cur_condition_candle['time_open'].timestamp())
        if log: self.debug(f"Can check condition 5: {can_check_5}")
        if can_check_5:
            lower, upper = int_time_close(cur_condition_candle), int_time_close(cur_condition_candle) + time_tolerance
            cond5 = lower <= int_time_open(cur_candle) <= upper
            if log: self.debug(f"Condition 5 bounds: Lower={lower}, Upper={upper}, Current={int_time_open(cur_candle)}")
            if cond5:
                if log: self.debug(
                    f"Before condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle,
                                                                                                       prev_condition_candle,
                                                                                                       cur_condition_candle,
                                                                                                       5, cur_state)
                should_enter = True
            if log: self.debug(
                f"After condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        if log: self.debug(
            f"Returning: should_enter={should_enter}, prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
        return should_enter, prev_state, cur_state, prev_condition_candle, cur_condition_candle

    def update_state(
            self,
            cur_candle: Series,
            prev_condition_candle: Optional[Series],
            cur_condition_candle: Optional[Series],
            cur_state: int,
            prev_state: int
    ) -> Tuple[int, int, Optional[Series], Optional[Series]]:
        """
        Update the internal state and record the last candle that met the condition.

        Args:
            cur_candle (Series): The current candle.
            prev_condition_candle (Optional[Series]): The previously matched condition candle.
            cur_condition_candle (Optional[Series]): The last candle that met the condition.
            cur_state (int): The new state value.
            prev_state (int): The old state value.

        Raises:
            ValueError: If the current candle's time is earlier than the last condition candle's time.

        Returns:
            Tuple[int, int, Optional[Series], Optional[Series]]: (previous state, new state, previous condition candle, updated condition candle)
        """
        ret_state = cur_state if cur_state != prev_state else prev_state
        self.info(f"State change from {prev_state} to {cur_state}")
        cur_time_unix = dt_to_unix(cur_candle['time_open'])
        cur_condition_time_unix = dt_to_unix(
            cur_condition_candle['time_open']) if cur_condition_candle is not None else None
        if cur_condition_time_unix and cur_time_unix < cur_condition_time_unix:
            raise ValueError(
                f"Current candle time {cur_candle['time_open']} cannot be earlier than the last condition candle time {cur_condition_candle['time_open']}."
            )
        if cur_state != prev_state:
            if cur_state == 0:
                self.info("State changed to 0. Resetting condition candle.")
                updated_candle = None
            else:
                prev_time = cur_condition_candle['time_open'] if cur_condition_candle is not None else None
                self.info(f"Candle time updated from {prev_time} to {cur_candle['time_open']}.")
                updated_candle = cur_candle
            prev_condition_candle = cur_condition_candle
        else:
            self.info(
                f"No state change detected. State remains {cur_state}. Current candle time: {cur_candle['time_open']}. Previous state: {prev_state}.")
            updated_candle = cur_condition_candle
        return prev_state, ret_state, prev_condition_candle, updated_candle

    @exception_handler
    async def shutdown(self):
        """
        Shutdown the strategy agent.
        """
        self.info("Shutting down the strategy.")

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: Dict,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):
        """
        Publish a message to a RabbitMQ exchange with standard metadata.
        """
        recipient = recipient if recipient is not None else "middleware"
        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        meta_inf = MessageMetaInf(
            routine_id=self.id,
            agent_name=self.agent,
            symbol=self.trading_config.get_symbol(),
            timeframe=self.trading_config.get_timeframe(),
            direction=self.trading_config.get_trading_direction(),
            bot_name=self.trading_config.get_bot_name()
        )
        q_message = QueueMessage(sender=self.agent, payload=payload, recipient=recipient, meta_inf=meta_inf)
        self.info(f"Sending message to exchange {exchange_name} with routing key {routing_key} and message {q_message}")
        await RabbitMQService.publish_message(exchange_name=exchange_name,
                                              message=q_message,
                                              routing_key=routing_key,
                                              exchange_type=exchange_type)

    @exception_handler
    async def send_generator_update(self, message: str):
        """
        Send a generator update message via RabbitMQ.
        """
        self.info(f"Publishing update message: {message} for agent {self.id}")
        await self.send_queue_message(exchange=RabbitExchange.jupiter_notifications, payload={"message": message},
                                      routing_key=f"notification.user.{self.id}")
