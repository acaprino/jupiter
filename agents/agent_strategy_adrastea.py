# strategies/my_strategy.py
import asyncio
import uuid
from datetime import datetime
from typing import Tuple, Optional, Dict

import pandas as pd
from pandas import Series

from agents.agent_registration_aware import RegistrationAwareAgent
from csv_loggers.logger_candles import CandlesLogger
from csv_loggers.logger_strategy_events import StrategyEventsLogger
from dto.EconomicEvent import get_symbol_countries_of_interest, EconomicEvent
from dto.QueueMessage import QueueMessage
from dto.Signal import Signal
from dto.SymbolInfo import SymbolInfo
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Indicators, Timeframe, TradingDirection, RabbitExchange
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import describe_candle, dt_to_unix, unix_to_datetime, round_to_point, to_serializable, extract_properties, now_utc
from notifiers.notifier_economic_events import NotifierEconomicEvents
from notifiers.notifier_tick_updates import NotifierTickUpdates
from services.service_rabbitmq import RabbitMQService
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
    Implementazione concreta della strategia di trading.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration):
        RegistrationAwareAgent.__init__(self, config, trading_config)
        # Internal state
        self.initialized = False
        self.prev_condition_candle = None
        self.cur_condition_candle = None
        self.prev_state = 0  # Initialize state to 0
        self.cur_state = 0  # Initialize state to 0
        self.should_enter = False
        self.heikin_ashi_candles_buffer = int(1000 * trading_config.get_timeframe().to_hours())
        self.allow_last_tick = False
        self.market_open_event = asyncio.Event()
        self.bootstrap_completed_event = asyncio.Event()
        self.live_candles_logger = CandlesLogger(config, trading_config.get_symbol(), trading_config.get_timeframe(), trading_config.get_trading_direction())
        self.countries_of_interest = []

        bootstrap_rates_count = int(500 * (1 / trading_config.get_timeframe().to_hours()))
        self.tot_candles_count = self.heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()
        self.debug(f"Calculated {self.tot_candles_count} candles length to work on strategy")

        # Renamed for clarity, stores the close time of the last *successfully processed* candle
        self._last_processed_candle_close_time: Optional[datetime] = None
        # self.gap_checked = False # Removed, check is now always active
        self.market_closed_duration = 0.0  # Cumulative duration (in seconds) that the market remains closed
        self.market_close_timestamp = None  # Timestamp marking when the market was closed
        # self.first_run = True # Removed, gap check handles the first run implicitly

        # Tolerance for gap check in seconds
        self.gap_tolerance_seconds = 5.0

    @exception_handler
    async def start(self):
        self.info("Starting the strategy.")
        self.countries_of_interest = await get_symbol_countries_of_interest(self.trading_config.get_symbol())
        e_events_notif = await NotifierEconomicEvents.get_instance(self.config)
        await e_events_notif.register_observer(
            self.countries_of_interest,
            self.on_economic_event,
            self.id
        )
        tick_notif = await NotifierTickUpdates.get_instance(self.config)
        await tick_notif.register_observer(
            self.trading_config.timeframe,
            self.on_new_tick,
            self.id
        )

        asyncio.create_task(self.bootstrap())

    @exception_handler
    async def stop(self):
        self.info(f"Stopping the strategy.")

        await self.shutdown()

        e_events_notif = await NotifierEconomicEvents.get_instance(self.config)
        await e_events_notif.unregister_observer(
            self.countries_of_interest,
            3,
            self.id
        )
        tick_notif = await NotifierTickUpdates.get_instance(self.config)
        await tick_notif.unregister_observer(
            self.trading_config.timeframe,
            self.id
        )

    def get_minimum_frames_count(self):
        return max(super_trend_fast_period,
                   super_trend_slow_period,
                   stoch_k_period,
                   stoch_d_period,
                   stoch_smooth_k) + 1

    async def notify_state_change(self, rates, i):
        symbol, timeframe, trading_direction = (
            self.trading_config.get_symbol(), self.trading_config.get_timeframe(), self.trading_config.get_trading_direction()
        )

        events_logger = StrategyEventsLogger(self.config, symbol, timeframe, trading_direction)
        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']

        # Extract required indicator values from the candles
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

        # Handle state transitions and trigger notifications
        if self.cur_state == 1 and self.prev_state == 0:
            if is_long:
                await  _notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is above the slow Supertrend level {supert_slow_prev}, validating long position.")
            elif is_short:
                await  _notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is below the slow Supertrend level {supert_slow_prev}, validating short position.")
        elif self.cur_state == 0 and self.prev_state == 1:
            if is_long:
                await _notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now below the slow Supertrend level {supert_slow_prev}, invalidating the long position.")
            elif is_short:
                await _notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now above the slow Supertrend level {supert_slow_prev}, invalidating the short position.")

        elif self.cur_state == 2 and self.prev_state == 1:
            if is_long:
                await _notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is below the fast Supertrend level {supert_fast_cur}, valid for long trade.")
            elif is_short:
                await _notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is above the fast Supertrend level {supert_fast_cur}, valid for short trade.")
        elif self.cur_state == 1 and self.prev_state == 2:
            if is_long:
                await _notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain below the fast Supertrend level {supert_fast_cur}.")
            elif is_short:
                await _notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain above the fast Supertrend level {supert_fast_cur}.")

        elif self.cur_state == 3 and self.prev_state == 2:
            if is_long:
                await _notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains above the fast Supertrend level {supert_fast_prev}, confirming long trade.")
            elif is_short:
                await _notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains below the fast Supertrend level {supert_fast_prev}, confirming short trade.")
        elif self.cur_state == 2 and self.prev_state == 3:
            if is_long:
                await _notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain above the fast Supertrend level {supert_fast_prev}, invalidating the long trade.")
            elif is_short:
                await _notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain below the fast Supertrend level {supert_fast_prev}, invalidating the short trade.")

        elif self.cur_state == 4 and self.prev_state == 3:
            if is_long:
                await _notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed above D ({stoch_d_cur}) and D is below 50, confirming bullish momentum.")
            elif is_short:
                await _notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed below D ({stoch_d_cur}) and D is above 50, confirming bearish momentum.")
        elif self.cur_state == 3 and self.prev_state == 4:
            if is_long:
                await _notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer above D ({stoch_d_cur}).")
            elif is_short:
                await _notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer below D ({stoch_d_cur}).")

        if self.should_enter:
            dir_str = "long" if is_long else "short"
            cur_candle_time = f"{cur_candle['time_open'].strftime('%H:%M')} - {cur_candle['time_close'].strftime('%H:%M')}"
            last_condition_candle_time = f"{self.cur_condition_candle['time_open'].strftime('%H:%M')} - {self.cur_condition_candle['time_close'].strftime('%H:%M')}"
            await _notify_event(
                f"5️⃣ ✅ <b>Condition 5 matched</b>: Final signal generated for {dir_str} trade. The current candle time {cur_candle_time} is from the candle following the last condition candle: {last_condition_candle_time}")

    @exception_handler
    async def bootstrap(self):
        self.info("Initializing the strategy.")

        market_is_open = await self.broker().is_market_open(self.trading_config.get_symbol())
        async with self.execution_lock:
            if not market_is_open:
                self.info("Market is closed, waiting for it to open.")

        # await self.market_open_event.wait() # Keep commented if not strictly needed yet
        self.info("Market is open, proceeding with strategy bootstrap.")

        async with self.execution_lock:
            timeframe = self.trading_config.get_timeframe()
            symbol = self.trading_config.get_symbol()
            trading_direction = self.trading_config.get_trading_direction()

            self.debug(f"Config - Symbol: {symbol}, Timeframe: {timeframe}, Direction: {trading_direction}")

            try:
                bootstrap_candles_logger = CandlesLogger(self.config, symbol, timeframe, trading_direction, custom_name='bootstrap')

                # --- MODIFICATION START ---
                # Directly await the async broker call
                self.info(f"Fetching {self.tot_candles_count} candles...")
                candles = await self.broker().get_last_candles(
                    self.trading_config.get_symbol(),
                    self.trading_config.get_timeframe(),
                    self.tot_candles_count
                )
                self.info(f"Retrieved {len(candles)} candles.")

                if candles.empty:
                    self.error("Failed to retrieve candles during bootstrap. Aborting.")
                    self.initialized = False
                    self.bootstrap_completed_event.set()  # Allow other parts to proceed if needed, but mark as failed init
                    return  # Exit bootstrap

                self.info("Calculating indicators on historical candles.")
                # Directly await the async calculate_indicators call
                await self.calculate_indicators(candles)
                self.info("Indicators calculated.")
                # --- MODIFICATION END ---

                first_index = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
                last_index = len(candles)  # Use actual length after potential filtering in get_last_candles

                # --- MODIFICATION START ---
                # Process the bootstrap loop directly in the async context
                self.info(f"Processing bootstrap candles from index {first_index} to {last_index - 1}")
                for i in range(first_index, last_index):
                    # Log the candle data if needed
                    self.debug(f"Bootstrap frame {i}, Candle data: {describe_candle(candles.iloc[i])}")

                    # Add the current candle to the bootstrap logger
                    bootstrap_candles_logger.add_candle(candles.iloc[i])

                    # Update state by checking signals for the current candle
                    # check_signals is synchronous, call it directly
                    self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                        rates=candles,
                        i=i,
                        trading_direction=trading_direction,
                        state=self.cur_state,
                        cur_condition_candle=self.cur_condition_candle,
                        log=False  # Keep log=False for bootstrap performance unless needed
                    )
                    # If state changed, notify (notify_state_change is async)
                    if self.prev_state != self.cur_state or self.should_enter:
                        await self.notify_state_change(candles, i)  # Await the async notification

                self.info(f"Bootstrap complete - Final State: {self.cur_state}")
                # --- MODIFICATION END ---

                # NB If silent bootstrap is enabled, no enter signals will be sent to the bot's Telegram channel
                if not self.config.is_silent_start():
                    await self.send_generator_update("🚀 Bootstrapping complete - <b>Bot ready for trading.</b>")

                # Notify final state after loop (if needed, might be redundant if already notified in loop)
                # await self.notify_state_change(candles, last_index - 1) # Consider if this is still needed

                self.initialized = True
                self._last_processed_candle_close_time = candles.iloc[-1]['time_close']
                self.bootstrap_completed_event.set()

            except Exception as e:
                self.error(f"Error in strategy bootstrap", exec_info=e)
                self.initialized = False
                self.bootstrap_completed_event.set()  # Ensure event is set even on error

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        """
        Handles market status changes. Sets the market open event, calculates closed duration,
        and manages flags for tick processing. Avoids fetching candles here as on_new_tick handles it.
        """
        async with self.execution_lock:
            if is_open:
                self.market_open_event.set()
                # self.gap_checked = False # Removed

                # Only calculate market closed duration if reopening after a known close
                if not initializing and self.market_close_timestamp is not None:
                    try:
                        opening_dt = unix_to_datetime(opening_time)
                        closed_duration = (opening_dt - self.market_close_timestamp).total_seconds()
                        # Ensure duration is non-negative
                        self.market_closed_duration = max(0.0, closed_duration)
                        self.info(f"Market was closed for {self.market_closed_duration:.2f} seconds.")
                        self.market_close_timestamp = None
                    except Exception as e:
                        self.error("Error calculating market closed duration", exec_info=e)
                        self.market_closed_duration = 0.0  # Reset on error
                        self.market_close_timestamp = None
                else:
                    # Ensure duration is 0 if market opens without prior known close (e.g. initial state)
                    self.market_closed_duration = 0.0

            else:  # Market is closing
                self.market_open_event.clear()
                if not initializing:
                    self.info("Market closing detected: marking close time.")
                    self.allow_last_tick = True
                    try:
                        self.market_close_timestamp = unix_to_datetime(closing_time)
                    except Exception as e:
                        self.error("Error converting closing_time to datetime", exec_info=e)
                        self.market_close_timestamp = None

    @exception_handler
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        """
        Handles incoming ticks, performs gap checks, calculates indicators,
        checks for signals, and logs data. Now includes continuous gap checking.
        """
        if not self.bootstrap_completed_event.is_set():
            self.debug("Waiting for bootstrap to complete before processing tick...")
            await self.bootstrap_completed_event.wait()
            self.debug("Bootstrap complete. Proceeding with tick.")

        async with self.execution_lock:
            self.debug(f"Tick received for {timeframe.name} at {timestamp}. Acquiring lock.")

            # --- Prerequisiti ---
            symbol = self.trading_config.get_symbol()
            try:
                market_is_open = await self.broker().is_market_open(symbol)
            except Exception as e:
                self.error(f"Failed to check market status for {symbol}", exec_info=e)
                return  # Cannot proceed without market status

            self.debug(f"Market open status for {symbol}: {market_is_open}.")

            if not market_is_open and not self.allow_last_tick:
                self.info("Market is closed and last tick not allowed, skipping processing.")
                return

            if not self.initialized:
                self.info("Strategy not initialized, skipping tick processing.")
                return

            # --- Recupero Candele ---
            main_loop = asyncio.get_running_loop()

            # Offload retrieval of candles using run_coroutine_threadsafe.
            def run_get_last_candles():
                future = asyncio.run_coroutine_threadsafe(
                    self.broker().get_last_candles(self.trading_config.get_symbol(), self.trading_config.get_timeframe(), self.tot_candles_count),
                    main_loop
                )
                return future.result()

            try:
                self.debug(f"Fetching {self.tot_candles_count} candles for {symbol} {timeframe.name}")
                candles = await asyncio.to_thread(run_get_last_candles)  # Use to_thread for potentially blocking I/O
                if candles is None or candles.empty:
                    self.error(f"Failed to retrieve candles for {symbol} {timeframe.name}. Skipping tick.")
                    return
                self.debug(f"Retrieved {len(candles)} candles.")
            except Exception as e:
                self.error(f"Exception during candle retrieval for {symbol} {timeframe.name}", exec_info=e)
                return  # Exit if candles cannot be retrieved

            last_candle = candles.iloc[-1]
            self.info(f"Processing Candle: {describe_candle(last_candle)}")

            # --- Controllo Gap (Sempre Eseguito dopo il primo tick) ---
            if self._last_processed_candle_close_time is not None:
                candle_interval = self.trading_config.get_timeframe().to_seconds()
                time_diff = last_candle['time_close'] - self._last_processed_candle_close_time
                gap_seconds = time_diff.total_seconds()

                # expected_max_gap accounts for normal interval + known closure time + tolerance
                expected_max_gap = candle_interval + self.market_closed_duration + self.gap_tolerance_seconds

                self.debug(
                    f"Gap Check: Time diff: {gap_seconds:.2f}s since last processed candle at "
                    f"{self._last_processed_candle_close_time}. "
                    f"Expected Interval: {candle_interval}s. "
                    f"Market Closed Duration: {self.market_closed_duration:.2f}s. "
                    f"Expected Max Gap (incl. tolerance): {expected_max_gap:.2f}s."
                )

                if gap_seconds > expected_max_gap:
                    ex_msg = (f"Unexpected gap detected: {gap_seconds:.2f}s passed "
                              f"since last processed candle closing at {self._last_processed_candle_close_time}. "
                              f"Expected max gap was {expected_max_gap:.2f}s. Possible missed ticks.")
                    # Decide how to handle: log error or raise exception
                    self.error(ex_msg, exec_info=False)
                    # Option 1: Log and continue (might use stale state)
                    # Option 2: Reset state and continue (safer but might miss signals)
                    # self.prev_state = 0
                    # self.cur_state = 0
                    # self.cur_condition_candle = None
                    # self.info("Resetting strategy state due to unexpected gap.")
                    # Option 3: Raise exception (stops the agent for this config)
                    # raise Exception(ex_msg)

                # Reset market_closed_duration *after* using it in the check
                if self.market_closed_duration > 0:
                    self.market_closed_duration = 0.0
                    self.debug("Reset market_closed_duration after gap check.")

            # --- Aggiorna il timestamp dell'ultima candela *processata con successo* ---
            # Questo viene fatto alla FINE del try block per assicurarsi che l'elaborazione sia andata a buon fine
            # self._last_processed_candle_close_time = last_candle['time_close'] # Moved to end of try block

            # --- Calcolo Indicatori e Logica Segnali ---
            try:
                self.debug("Calculating indicators...")

                # Ensure that calculate_indicators is thread safe if it accesses shared state.
                def run_indicators():
                    future = asyncio.run_coroutine_threadsafe(self.calculate_indicators(candles), main_loop)
                    return future.result()

                await asyncio.to_thread(run_indicators)

                self.debug("Indicators calculated.")

                # Ricampiona l'ultima candela perché gli indicatori potrebbero averla modificata
                last_candle = candles.iloc[-1]
                self.info(f"Candle after indicators: {describe_candle(last_candle)}")

                # Controlla segnali
                self.debug("Checking for trading signals...")
                (self.should_enter, self.prev_state, self.cur_state,
                 self.prev_condition_candle, self.cur_condition_candle) = self.check_signals(
                    rates=candles,
                    i=len(candles) - 1,
                    trading_direction=self.trading_config.get_trading_direction(),
                    state=self.cur_state,
                    cur_condition_candle=self.cur_condition_candle,
                    log=True  # Abilita log dettagliato in check_signals
                )
                self.debug(
                    f"Signal check results: should_enter={self.should_enter}, "
                    f"prev_state={self.prev_state}, cur_state={self.cur_state}."
                )

                # Notifica cambio stato
                self.debug("Notifying state change.")
                await self.notify_state_change(candles, len(candles) - 1)
                self.debug("State change notification completed.")

                # --- Invio Segnali (se stato 3 -> 4) ---
                if self.prev_state == 3 and self.cur_state == 4:
                    self.debug("State transition 3->4 detected. Preparing Signal DTO.")
                    signal = Signal(
                        bot_name=self.config.get_bot_name(),
                        signal_id=str(uuid.uuid4()),
                        symbol=symbol,
                        timeframe=self.trading_config.get_timeframe(),
                        direction=self.trading_config.get_trading_direction(),
                        candle=to_serializable(self.cur_condition_candle),  # Assicurati sia serializzabile
                        routine_id=self.id,
                        creation_tms=dt_to_unix(now_utc()),
                        agent=self.agent,
                        confirmed=False,  # Deve essere confermato dal Middleware/Utente
                        update_tms=None,
                        user=None
                    )
                    self.info(f"Generated signal: {signal.signal_id}. Sending to Middleware.")
                    await self.send_queue_message(exchange=RabbitExchange.SIGNALS, payload=signal.to_json(), routing_key=self.id)
                    self.debug("Signal sent to Middleware.")

                # --- Invio Segnale di Ingresso (se should_enter è True) ---
                if self.should_enter:
                    self.info("Condition 5 met (should_enter=True). Sending enter signal for execution.")
                    payload = {
                        # Usa to_serializable per assicurarti che i tipi siano JSON-compatibili
                        'candle': to_serializable(self.cur_condition_candle),
                        'prev_candle': to_serializable(self.prev_condition_candle)
                    }
                    await self.send_queue_message(exchange=RabbitExchange.ENTER_SIGNAL, payload=payload, routing_key=self.topic)
                    self.debug("Enter signal sent to Executor topic.")
                else:
                    self.debug("No final entry condition met (should_enter=False).")

                # --- Aggiorna il timestamp dell'ultima candela processata con successo ---
                self._last_processed_candle_close_time = last_candle['time_close']
                self.debug(f"Successfully processed tick. Updated last processed candle time to: {self._last_processed_candle_close_time}")

                # Logga la candela processata
                try:
                    self.live_candles_logger.add_candle(last_candle)
                    self.debug("Live candle data logged.")
                except Exception as log_e:
                    self.error("Error logging live candle data", exec_info=log_e)

            except Exception as process_e:
                # Se c'è un errore durante il calcolo indicatori o check segnali, *non* aggiornare
                # _last_processed_candle_close_time, così il prossimo tick rifarà il check del gap
                # rispetto all'ultima candela *processata correttamente*.
                self.error(f"Error during indicator calculation or signal checking for candle closing at {last_candle['time_close']}", exec_info=process_e)
                # Considera se resettare lo stato qui in caso di errore grave

            finally:
                # Reset allow_last_tick dopo l'elaborazione (o il tentativo)
                if self.allow_last_tick:
                    self.debug("Resetting allow_last_tick flag.")
                    self.allow_last_tick = False

            self.debug("Tick processing finished. Releasing lock.")

    @exception_handler
    async def on_economic_event(self, event: EconomicEvent):
        async with self.execution_lock:
            self.info(f"Economic event occurred: {event.to_json()}")
            await self.send_queue_message(exchange=RabbitExchange.ECONOMIC_EVENTS, payload=to_serializable(event), routing_key=self.topic)

    @exception_handler
    async def calculate_indicators(self, rates):
        # Convert candlestick to Heikin Ashi
        await self.heikin_ashi_values(rates)

        # Calculate indicators
        supertrend(super_trend_fast_period, super_trend_fast_multiplier, rates)
        supertrend(super_trend_slow_period, super_trend_slow_multiplier, rates)
        stochastic(stoch_k_period, stoch_d_period, stoch_smooth_k, rates)
        average_true_range(5, rates)
        average_true_range(2, rates)

        return rates

    @exception_handler
    async def heikin_ashi_values(self, df):
        # Ensure df is a DataFrame with the necessary columns
        if not isinstance(df, pd.DataFrame) or not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("Input must be a DataFrame with 'open', 'high', 'low', and 'close' columns.")

        # Get the symbol's point precision (e.g., 0.01, 0.0001)
        symbol_info: SymbolInfo = await self.broker().get_market_info(self.trading_config.get_symbol())

        # Calculate HA_close without rounding
        df['HA_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        # Initialize the first HA_open as the average of the first open and close
        ha_open = [(df['open'][0] + df['close'][0]) / 2]

        # Calculate subsequent HA_open values without rounding
        for i in range(1, len(df)):
            ha_open.append((ha_open[i - 1] + df['HA_close'].iloc[i - 1]) / 2)

        # Add the calculated HA_open values to the DataFrame (without rounding)
        df['HA_open'] = pd.Series(ha_open, index=df.index)

        # Calculate HA_high and HA_low without rounding
        df['HA_high'] = df[['HA_open', 'HA_close', 'high']].max(axis=1)
        df['HA_low'] = df[['HA_open', 'HA_close', 'low']].min(axis=1)

        # Now, round all final Heikin-Ashi values to the appropriate precision
        df['HA_open'] = round_to_point(df['HA_open'], symbol_info.point)
        df['HA_close'] = round_to_point(df['HA_close'], symbol_info.point)
        df['HA_high'] = round_to_point(df['HA_high'], symbol_info.point)
        df['HA_low'] = round_to_point(df['HA_low'], symbol_info.point)

        return df

    def check_signals(
            self,
            rates: Series,
            i: int,
            trading_direction: TradingDirection,
            state=None,
            cur_condition_candle=None,
            log: bool = True
    ) -> (bool, int, int, Series):
        """
        Analyzes market conditions to determine the appropriateness of entering a trade based on a set of predefined rules.

        Parameters:
        - rates (dict): A dictionary containing market data rates, with keys for time, close, and other indicator values.
        - i (int): The current index in the rates dictionary to check signals for. In live mode is always the last candle index.
        - params (dict): A dictionary containing parameters such as symbol, timeframe, and trading direction.
        - state (int, optional): The current state of the trading conditions, used for tracking across multiple calls. Defaults to None.
        - cur_condition_candle (Dataframe, optional): The last candle where a trading condition was met. Defaults to None.
        - notifications (bool, optional): Indicates whether to send notifications when conditions are met. Defaults to False.

        Returns:
        - should_enter (bool): Indicates whether the conditions suggest entering a trade.
        - prev_state (int): The previous state before the current check.
        - cur_state (int): The updated state after checking the current conditions.
        - prev_condition_candle (Series): The candle where a trading condition was previously met.
        - cur_condition_candle (Series): The candle where the latest trading condition was met.

        The function evaluates a series of trading conditions based on market direction (long or short), price movements, and indicators like Supertrend and Stochastic. It progresses through states as conditions are met, logging each step, and ultimately determines whether the strategy's criteria for entering a trade are satisfied.
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
            if log: self.debug(f"Before evaluating condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond1 = (is_long and close >= supert_slow_prev) or (is_short and close < supert_slow_prev)
            if cond1:
                if cur_state == 0:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 1, cur_state)
            elif cur_state >= 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 0, cur_state)
            if log: self.debug(f"After evaluating condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 2
        can_check_2 = cur_state >= 1 and int_time_open(cur_candle) > int_time_open(cur_condition_candle)
        if log: self.debug(f"Can check condition 2: {can_check_2}")
        if can_check_2:
            if log: self.debug(f"Before evaluating condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond2 = (is_long and close <= supert_fast_cur) or (is_short and close > supert_fast_cur)
            if cond2 and cur_state == 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 2, cur_state)
            if log: self.debug(f"After evaluating condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 3
        can_check_3 = cur_state >= 2 and int_time_open(cur_candle) >= int_time_open(cur_condition_candle)
        if log: self.debug(f"Can check condition 3: {can_check_3}")
        if can_check_3:
            if log: self.debug(f"Before evaluating condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond3 = (is_long and close >= supert_fast_prev) or (is_short and close < supert_fast_prev)
            if cond3:
                if cur_state == 2:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 3, cur_state)
            elif cur_state >= 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 2, cur_state)
            if log: self.debug(f"After evaluating condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 4 (Stochastic)
        can_check_4 = cur_state >= 3
        if log: self.debug(f"Can check condition 4: {can_check_4}")
        if can_check_4:
            if log: self.debug(f"Before evaluating condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond4 = (is_long and stoch_k_cur > stoch_d_cur and stoch_d_cur < 50) or (is_short and stoch_k_cur < stoch_d_cur and stoch_d_cur > 50)
            if cond4 and cur_state == 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 4, cur_state)
            if log: self.debug(f"After evaluating condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 5 (Final condition for entry)
        time_tolerance = 30
        can_check_5 = cur_state == 4 and int(cur_candle['time_open'].timestamp()) > int(cur_condition_candle['time_open'].timestamp())
        if log: self.debug(f"Can check condition 5: {can_check_5}")
        if can_check_5:
            lower, upper = int_time_close(cur_condition_candle), int_time_close(cur_condition_candle) + time_tolerance
            cond5 = lower <= int_time_open(cur_candle) <= upper
            if log: self.debug(f"Lower Bound: {lower}, Upper Bound: {upper}, Current Candle Time: {int_time_open(cur_candle)}")
            # condition_5_met = to_int(cur_candle_time) >= lower_bound  # Uncomment for testing
            if cond5:
                if log: self.debug(f"Before evaluating condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 5, cur_state)
                should_enter = True
            if log: self.debug(f"After evaluating condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        if log: self.debug(f"Returning: should_enter={should_enter}, prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
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
        Updates the state and the last candle that met the condition.

        Args:
            cur_candle (Series): The current candle.
            prev_condition_candle (Optional[Series]): The previous condition-matching candle.
            cur_condition_candle (Optional[Series]): The last candle that meets the condition.
            cur_state (int): The current new state.
            prev_state (int): The previous old state.

        Raises:
            ValueError: If the current candle time is earlier than the last condition-matching candle time.

        Returns:
            Tuple[int, int, Optional[Series], Optional[Series]]: (previous state, new state, previous condition candle, updated condition candle)
        """

        ret_state = cur_state if cur_state != prev_state else prev_state

        self.info(f"Changing state from {prev_state} to {cur_state}")

        cur_time_unix = dt_to_unix(cur_candle['time_open'])
        cur_condition_time_unix = dt_to_unix(cur_condition_candle['time_open']) if cur_condition_candle is not None else None

        if cur_condition_time_unix and cur_time_unix < cur_condition_time_unix:
            raise ValueError(
                f"Strategy current candle time {cur_candle['time_open']} cannot be prior to last condition-matching "
                f"candle time {cur_condition_candle['time_open']}."
            )

        if cur_state != prev_state:
            if cur_state == 0:
                self.info("State changed to 0. Resetting cur_condition_candle.")
                updated_candle = None
            else:
                prev_time = cur_condition_candle['time_open'] if cur_condition_candle is not None else None
                self.info(f"Strategy candle time change from {prev_time} -> {cur_candle['time_open']}")
                updated_candle = cur_candle
            prev_condition_candle = cur_condition_candle
        else:
            self.info(
                f"update_state called but no state change detected. Current state remains {cur_state}. "
                f"Called with candle time {cur_candle['time_open']}. Previous state was {prev_state}."
            )
            updated_candle = cur_condition_candle

        return prev_state, ret_state, prev_condition_candle, updated_candle

    @exception_handler
    async def shutdown(self):
        self.info("Shutting down the bot.")

    @exception_handler
    async def send_queue_message(self, exchange: RabbitExchange,
                                 payload: Dict,
                                 routing_key: Optional[str] = None,
                                 recipient: Optional[str] = None):

        recipient = recipient if recipient is not None else "middleware"

        tc = extract_properties(self.trading_config, ["symbol", "timeframe", "trading_direction", "bot_name"])
        exchange_name, exchange_type = exchange.name, exchange.exchange_type
        q_message = QueueMessage(sender=self.agent, payload=payload, recipient=recipient, trading_configuration=tc)

        self.info(f"Sending message to exchange {exchange_name} with routing key {routing_key} and message {q_message}")
        await RabbitMQService.publish_message(exchange_name=exchange_name,
                                              message=q_message,
                                              routing_key=routing_key,
                                              exchange_type=exchange_type)

    @exception_handler
    async def send_generator_update(self, message: str):
        self.info(f"Publishing event message: {message} for agent with id {self.id}")
        await self.send_queue_message(exchange=RabbitExchange.NOTIFICATIONS, payload={"message": message}, routing_key=self.id)
