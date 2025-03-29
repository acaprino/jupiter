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
    Implementazione concreta della strategia di trading.
    """

    def __init__(self, config: ConfigReader, trading_config: TradingConfiguration, *args, **kwargs):
        RegistrationAwareAgent.__init__(self, config, trading_config)
        # Internal state
        self.initialized = False
        self.prev_condition_candle = None
        self.cur_condition_candle = None
        self.prev_state = None
        self.cur_state = None
        self.should_enter = False
        self.heikin_ashi_candles_buffer = int(1000 * trading_config.get_timeframe().to_hours())
        self.allow_last_tick = False
        self.market_open_event = asyncio.Event()
        self.bootstrap_completed_event = asyncio.Event()
        self.live_candles_logger = CandlesLogger(config, trading_config.get_symbol(), trading_config.get_timeframe(), trading_config.get_trading_direction())
        self.countries_of_interest = []

        bootstrap_rates_count = int(500 * (1 / trading_config.get_timeframe().to_hours()))
        self.tot_candles_count = self.heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()
        self.debug(f"Calculated {self.tot_candles_count} candles lenght to work on strategy")
        self.bootstrap_last_close = None

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

        # await self.market_open_event.wait()
        self.info("Market is open, proceeding with strategy bootstrap.")

        async with self.execution_lock:
            timeframe = self.trading_config.get_timeframe()
            symbol = self.trading_config.get_symbol()
            trading_direction = self.trading_config.get_trading_direction()

            self.debug(f"Config - Symbol: {symbol}, Timeframe: {timeframe}, Direction: {trading_direction}")

            try:
                main_loop = asyncio.get_running_loop()

                bootstrap_candles_logger = CandlesLogger(self.config, symbol, timeframe, trading_direction, custom_name='bootstrap')

                # Offload retrieval of candles using run_coroutine_threadsafe.
                def run_get_last_candles():
                    future = asyncio.run_coroutine_threadsafe(
                        self.broker().get_last_candles(self.trading_config.get_symbol(), self.trading_config.get_timeframe(), self.tot_candles_count),
                        main_loop
                    )
                    return future.result()

                candles = await asyncio.to_thread(run_get_last_candles)

                self.info("Calculating indicators on historical candles.")

                # Ensure that calculate_indicators is thread safe if it accesses shared state.
                def run_indicators():
                    future = asyncio.run_coroutine_threadsafe(self.calculate_indicators(candles), main_loop)
                    return future.result()

                await asyncio.to_thread(run_indicators)

                first_index = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
                last_index = self.tot_candles_count

                # Function to process the bootstrap loop in a separate thread
                def process_bootstrap_loop():
                    for i in range(first_index, last_index):
                        # Log the candle data if needed (debug line commented out)
                        self.debug(f"Bootstrap frame {i + 1}, Candle data: {describe_candle(candles.iloc[i])}")
                        is_current_candle = (i == last_index - 1)

                        # Add the current candle to the bootstrap logger
                        bootstrap_candles_logger.add_candle(candles.iloc[i])
                        # Update state by checking signals for the current candle
                        self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                            rates=candles,
                            i=i,
                            trading_direction=trading_direction,
                            state=self.cur_state,
                            cur_condition_candle=self.cur_condition_candle,
                            log=False
                        )

                # Run the heavy bootstrap loop in a separate thread to avoid blocking the asyncio event loop
                await asyncio.to_thread(process_bootstrap_loop)

                self.info(f"Bootstrap complete - Initial State: {self.cur_state}")

                # NB If silent bootstrap is enabled, no enter signals will be sent to the bot's Telegram channel
                if not self.config.is_silent_start():
                    await self.send_generator_update("🚀 Bootstrapping complete - <b>Bot ready for trading.</b>")
                await self.notify_state_change(candles, last_index - 1)
                self.initialized = True

                self.bootstrap_last_close = candles.iloc[-1]['time_close']
                self.bootstrap_completed_event.set()

            except Exception as e:
                self.error(f"Error in strategy bootstrap", exec_info=e)
                self.initialized = False

    @exception_handler
    async def on_market_status_change(self, symbol: str, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        async with self.execution_lock:
            symbol = self.trading_config.get_symbol()
            time_ref = opening_time if is_open else closing_time
            self.info(f"Market for {symbol} has {'opened' if is_open else 'closed'} at {unix_to_datetime(time_ref)}.")
            if is_open:
                self.market_open_event.set()
            else:
                self.market_open_event.clear()
                if not initializing:
                    self.info("Allowing the last tick to be processed before fully closing the market.")
                    self.allow_last_tick = True

    @exception_handler
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        await self.bootstrap_completed_event.wait()
        self.debug("New tick activated.")
        async with self.execution_lock:

            market_is_open = await self.broker().is_market_open(self.trading_config.get_symbol())
            if not market_is_open and not self.allow_last_tick:
                self.info("Market is closed, skipping tick processing.")
                return

            if not self.initialized:
                self.info("Strategy not initialized, skipping tick processing.")
                return

            main_loop = asyncio.get_running_loop()

            # Offload retrieval of candles using run_coroutine_threadsafe.
            def run_get_last_candles():
                future = asyncio.run_coroutine_threadsafe(
                    self.broker().get_last_candles(self.trading_config.get_symbol(), self.trading_config.get_timeframe(), self.tot_candles_count),
                    main_loop
                )
                return future.result()

            candles = await asyncio.to_thread(run_get_last_candles)

            # Get the last closed candle.
            last_candle = candles.iloc[-1]
            self.info(f"Candle: {describe_candle(last_candle)}")

            # Immediately check if the last candle's close timestamp matches that of the bootstrap.
            # If it does, it means no new candle has closed since bootstrap, so we skip processing.
            if last_candle['time_close'] == self.bootstrap_last_close:
                self.debug("Skipping tick processing: last candle is the same as the one processed during bootstrap.")
                return

            # Assume that self.trading_config.get_timeframe().to_seconds() returns the candle interval in seconds
            candle_interval = self.trading_config.get_timeframe().to_seconds()
            time_diff = last_candle['time_close'] - self.bootstrap_last_close

            if time_diff > candle_interval:
                # More than one candle has closed since the bootstrap finished—this is unexpected!
                raise Exception(f"Unexpected gap: {time_diff} seconds passed since bootstrap. Expected a gap of at most {candle_interval} seconds.")

            self.info("Calculating indicators on historical candles.")
            # Ensure that calculate_indicators is thread safe if it accesses shared state.
            def run_indicators():
                future = asyncio.run_coroutine_threadsafe(self.calculate_indicators(candles), main_loop)
                return future.result()

            await asyncio.to_thread(run_indicators)

            last_candle = candles.iloc[-1]
            self.info(f"Candle: {describe_candle(last_candle)}")

            self.debug("Checking for trading signals.")
            self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                rates=candles, i=len(candles) - 1, trading_direction=self.trading_config.get_trading_direction(),
                state=self.cur_state, cur_condition_candle=self.cur_condition_candle
            )

            await self.notify_state_change(candles, len(candles) - 1)

            if self.prev_state == 3 and self.cur_state == 4:
                signal = Signal(
                    bot_name=self.config.get_bot_name(),
                    signal_id=str(uuid.uuid4()),
                    symbol=self.trading_config.get_symbol(),
                    timeframe=self.trading_config.get_timeframe(),
                    direction=self.trading_config.get_trading_direction(),
                    candle=self.cur_condition_candle,
                    routine_id=self.id,
                    creation_tms=dt_to_unix(now_utc()),
                    agent=self.agent,
                    confirmed=False,
                    update_tms=None,
                    user=None
                )
                await self.send_queue_message(exchange=RabbitExchange.SIGNALS, payload=to_serializable(signal), routing_key=self.id)

            if self.should_enter:
                # Notify all listeners about the signal
                # candle_props = ['time_close', 'time_open', 'open', 'high', 'low', 'close', 'HA_open', 'HA_high', 'HA_low', 'HA_close']
                payload = {
                    'candle': self.cur_condition_candle,
                    'prev_candle': self.prev_condition_candle
                }
                await self.send_queue_message(exchange=RabbitExchange.ENTER_SIGNAL, payload=payload, routing_key=self.topic)
            else:
                self.info(f"No condition satisfied for candle {describe_candle(last_candle)}")

            if self.allow_last_tick:
                self.allow_last_tick = False

            try:
                self.live_candles_logger.add_candle(last_candle)
            except Exception as e:
                self.error(f"Error while logging candle", exec_info=e)

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
