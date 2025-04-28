import asyncio
import json
import os
import zmq
import MetaTrader5 as mt5
import pandas as pd

from datetime import datetime
from datetime import timedelta
from typing import Any, Optional, Tuple, List, Dict
from brokers.broker_interface import BrokerAPI
from dto.BrokerOrder import BrokerOrder
from dto.Deal import Deal
from dto.EconomicEvent import EconomicEvent, map_from_metatrader
from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.RequestResult import RequestResult
from dto.SymbolInfo import SymbolInfo
from dto.SymbolPrice import SymbolPrice
from misc_utils.config import ConfigReader
from misc_utils.enums import Timeframe, FillingType, OpType, DealType, OrderSource, PositionType, OrderType, Action
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import now_utc, dt_to_unix, unix_to_datetime, round_to_point, new_id

# https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties
# https://www.mql5.com/en/articles/40
# https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py
# https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py

ORDER_TYPE_MAPPING = {
    0: OrderType.BUY,  # ORDER_TYPE_BUY
    1: OrderType.SELL,  # ORDER_TYPE_SELL
    2: OrderType.BUY_LIMIT,  # ORDER_TYPE_BUY_LIMIT
    3: OrderType.SELL_LIMIT,  # ORDER_TYPE_SELL_LIMIT
    4: OrderType.BUY_STOP,  # ORDER_TYPE_BUY_STOP
    5: OrderType.SELL_STOP  # ORDER_TYPE_SELL_STOP
    # Other types are classified as 'OTHER'
}

DEAL_TYPE_MAPPING = {
    0: DealType.ENTER,  # DEAL_ENTRY_IN
    1: DealType.EXIT,  # DEAL_ENTRY_OUT
    # Other types are classified as 'OTHER'
}

POSITION_TYPE_MAPPING = {
    0: PositionType.LONG,  # DEAL_TYPE_BUY
    1: PositionType.SHORT,  # DEAL_TYPE_SELL
    # Other types are classified as 'OTHER'
}

REASON_MAPPING = {
    4: OrderSource.STOP_LOSS,  # DEAL_REASON_SL
    5: OrderSource.TAKE_PROFIT,  # DEAL_REASON_TP
    0: OrderSource.MANUAL,  # DEAL_REASON_CLIENT
    1: OrderSource.MANUAL,  # DEAL_REASON_MOBILE
    2: OrderSource.MANUAL,  # DEAL_REASON_WEB
    3: OrderSource.BOT,  # DEAL_REASON_EXPERT
    6: OrderSource.MANUAL,  # DEAL_REASON_SO (Stop Out)
    7: OrderSource.MANUAL,  # DEAL_REASON_ROLLOVER
    8: OrderSource.MANUAL,  # DEAL_REASON_VMARGIN
    9: OrderSource.MANUAL,  # DEAL_REASON_SPLIT
    # Other reasons are classified as 'OTHER'
}


class MT5Broker(BrokerAPI, LoggingMixin):

    def __init__(self, config: ConfigReader, configuration: Dict, *args, **kwargs):
        super().__init__(config=config, *args, **kwargs)
        self.agent = "MT5Broker"
        self.account = configuration['account']
        self.password = configuration['password']
        self.server = configuration['server']
        self.path = configuration['path']
        self._running = False
        # --- Heartbeat and Connection State ---
        self._is_connected = False
        self._connection_status_event = asyncio.Event()  # Event to signal connection status
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_interval: float = 30.0  # Check every 30 seconds
        self._reconnect_attempts: int = 5
        self._reconnect_delay: float = 10.0  # Wait 10 seconds between attempts
        self._lock = asyncio.Lock()  # Lock for reconnection logic

    @exception_handler
    async def startup(self) -> bool:
        """
        Initializes the MT5 connection and starts the heartbeat monitor.
        """
        async with self._lock:  # Ensure startup logic is atomic
            if self._running:
                self.warning("MT5Broker startup called but already running.")
                return True

            if await self._attempt_connect():
                self._running = True
                self._is_connected = True
                self._connection_status_event.set()  # Signal connection is up
                await self.start_heartbeat()  # Start background monitoring
                self.info("MT5Broker started and connection established.")
                return True
            else:
                self.critical("MT5Broker failed to start: initial connection failed.")
                # Ensure cleanup even if initial connection fails
                mt5.shutdown()
                return False

    @exception_handler
    async def shutdown(self):
        """
        Stops the heartbeat monitor and shuts down the MT5 connection.
        """
        async with self._lock:  # Ensure shutdown logic is atomic
            if not self._running:
                self.warning("MT5Broker shutdown called but not running.")
                return

            self._running = False  # Signal loops to stop
            await self.stop_heartbeat()  # Stop background monitoring first

            if self._is_connected:
                try:
                    mt5.shutdown()
                    self.info("MT5 connection shutdown successfully.")
                except Exception as e:
                    self.error("Error during mt5.shutdown()", exc_info=e)

            self._is_connected = False
            self._connection_status_event.clear()  # Signal connection is down
            self.info("MT5Broker shutdown complete.")

    # Conversion Methods
    def filling_type_to_mt5(self, filling_type: FillingType) -> int:
        conversion_dict = {
            FillingType.FOK: mt5.ORDER_FILLING_FOK,
            FillingType.IOC: mt5.ORDER_FILLING_IOC,
            FillingType.RETURN: mt5.ORDER_FILLING_RETURN
        }
        return conversion_dict[filling_type]

    def mt5_to_filling_type(self, mt5_filling_type: int) -> FillingType:
        """
        Convert an MT5 filling type constant to the corresponding FillingType enum.
        """
        reverse_conversion_dict = {
            mt5.ORDER_FILLING_FOK: FillingType.FOK,
            mt5.ORDER_FILLING_IOC: FillingType.IOC,
            mt5.ORDER_FILLING_RETURN: FillingType.RETURN,
        }
        if mt5_filling_type not in reverse_conversion_dict:
            raise ValueError(f"Unknown MT5 filling type: {mt5_filling_type}")
        return reverse_conversion_dict[mt5_filling_type]

    def timeframe_to_mt5(self, timeframe: Timeframe) -> int:
        conversion_dict = {
            Timeframe.M1: mt5.TIMEFRAME_M1,
            Timeframe.M5: mt5.TIMEFRAME_M5,
            Timeframe.M15: mt5.TIMEFRAME_M15,
            Timeframe.M30: mt5.TIMEFRAME_M30,
            Timeframe.H1: mt5.TIMEFRAME_H1,
            Timeframe.H4: mt5.TIMEFRAME_H4,
            Timeframe.D1: mt5.TIMEFRAME_D1
        }
        return conversion_dict[timeframe]

    def order_type_to_mt5(self, order_type: OpType) -> int:
        conversion_dict = {
            OpType.BUY: mt5.ORDER_TYPE_BUY,
            OpType.SELL: mt5.ORDER_TYPE_SELL,
            OpType.BUY_LIMIT: mt5.ORDER_TYPE_BUY_LIMIT,
            OpType.SELL_LIMIT: mt5.ORDER_TYPE_SELL_LIMIT,
            OpType.BUY_STOP: mt5.ORDER_TYPE_BUY_STOP,
            OpType.SELL_STOP: mt5.ORDER_TYPE_SELL_STOP
        }
        return conversion_dict[order_type]

    def mt5_to_order_type(self, mt5_order_type: int) -> OpType:
        conversion_dict = {
            mt5.ORDER_TYPE_BUY_STOP: OpType.BUY,
            mt5.ORDER_TYPE_SELL_STOP: OpType.SELL
        }
        return conversion_dict[mt5_order_type]

    def action_to_mt5(self, action: Action, op_type: OpType) -> int:
        if action == Action.PLACE_ORDER:
            if op_type in [OpType.BUY, OpType.SELL]:
                return mt5.TRADE_ACTION_DEAL
            else:
                return mt5.TRADE_ACTION_PENDING
        elif action == Action.MODIFY_ORDER:
            return mt5.TRADE_ACTION_MODIFY
        elif action == Action.REMOVE_ORDER:
            return mt5.TRADE_ACTION_REMOVE
        else:
            raise ValueError("Unsupported action")

    def get_last_error(self):
        error_code, error_message = mt5.last_error()
        return MT5Error(error_code, error_message)

    # Utility and Market Data Methods
    @exception_handler
    async def get_broker_name(self) -> str:
        return mt5.account_info().company

    @exception_handler
    async def is_market_open(self, symbol: str, utc_dt: Optional[datetime] = None) -> bool:
        """
        Check if the market is open for the given symbol, including session validation.

        If a utc_dt parameter is provided, the check for SYMBOL_TRADE_MODE_DISABLED is skipped
        because real-time data is assumed.
        """
        # Validate the symbol and retrieve its information
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            self.error(f"{symbol} not found, cannot retrieve symbol info.", exc_info=self.get_last_error())
            return False

        # If no utc_dt is provided, check that the symbol is not disabled for trading
        if utc_dt is None and symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            self.info(f"{symbol} is in trade mode disabled.")
            return False

        # Use the provided utc_dt or get the current UTC time
        current_time = utc_dt if utc_dt is not None else now_utc()

        # Verify if the symbol is in an active trading session
        if not await self.is_active_session(symbol, current_time):
            self.info(f"{symbol} is not in an active trading session.")
            return False

        # The market is open and within an active session
        return True

    @exception_handler
    async def is_active_session(self, symbol: str, utc_timestamp: datetime):
        """
        Checks if the specified session is active based on the broker's local time (UTC + offset)
        and the configured time ranges for that day of the week.

        NOTE: This version handles sessions crossing midnight and full-day sessions.
        In case of errors (except for missing session), it throws an exception.
        Extensive debug logging is provided to trace internal state.
        """
        # 1. Calculate the broker's local time by applying the timezone offset
        broker_offset_hours = await self.get_broker_timezone_offset()
        self.debug(f"Broker offset (hours): {broker_offset_hours}")
        if broker_offset_hours is None:
            raise ValueError("Broker timezone offset is None")

        broker_timestamp = utc_timestamp + timedelta(hours=broker_offset_hours)
        self.debug(f"UTC timestamp: {utc_timestamp} | Broker timestamp: {broker_timestamp}")

        # 2. Determine the day of the week from the broker's timestamp
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        broker_day_index = broker_timestamp.weekday()  # Monday=0, Sunday=6
        broker_day_name = day_names[broker_day_index]
        self.debug(f"Broker day index: {broker_day_index} | Broker day name: {broker_day_name}")

        # 3. Retrieve the market hours data (simulated via a ZMQ request)
        market_hours = await self.do_zmq_request(5556, symbol)
        self.debug(f"Retrieved market hours: {market_hours}")
        if not market_hours or 'sessions' not in market_hours:
            raise ValueError(f"Invalid market hours data received: {market_hours}")

        sessions = market_hours.get('sessions', [])
        if not isinstance(sessions, list):
            raise ValueError("Sessions data is not a list")

        # 4. Find the session corresponding to the broker's day.
        # If not found (e.g., Saturday or Sunday), consider the market closed.
        session = next((s for s in sessions if s['day'] == broker_day_name), None)
        if not session:
            self.debug(f"No session defined for {broker_day_name}. Market is closed.")
            return False
        self.debug(f"Session found for {broker_day_name}: {session}")

        # 5. Parse the start_time and end_time from the session configuration
        try:
            start_time = datetime.strptime(session['start_time'], '%H:%M').time()
            end_time = datetime.strptime(session['end_time'], '%H:%M').time()
        except ValueError as ve:
            raise ValueError(f"Invalid time format in session data: {session}") from ve

        self.debug(f"Parsed start_time: {start_time} | end_time: {end_time}")

        broker_time = broker_timestamp.time()
        self.debug(f"Broker current time: {broker_time}")

        # 6. Convert times to minutes for easier comparison
        broker_minutes = broker_time.hour * 60 + broker_time.minute
        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = end_time.hour * 60 + end_time.minute

        self.debug(f"Broker minutes: {broker_minutes} | Start minutes: {start_minutes} | End minutes: {end_minutes}")

        # 7. Check for a full-day session (e.g., both start_time and end_time are "00:00")
        if start_minutes == end_minutes:
            self.debug("Detected full-day session (start_time equals end_time)")
            # We interpret this as a 24-hour session (market open all day)
            is_active = True
        else:
            if start_minutes > end_minutes:
                self.debug("Session spans midnight (wrapping case)")
                # Wrapping case: the session spans midnight.
                # If end_time is "00:00", treat it as 1440 minutes (i.e., 24:00)
                if session['end_time'] == "00:00":
                    self.debug("end_time is '00:00', treating as 1440 minutes")
                    end_minutes = 1440
                # If broker time is less than start_minutes, it likely belongs to the next day,
                # so adjust broker_minutes by adding 1440 minutes.
                if broker_minutes < start_minutes:
                    self.debug("Broker minutes less than start_minutes; adjusting broker_minutes by adding 1440 minutes")
                    broker_minutes += 1440
                self.debug(f"After adjustment, Broker minutes: {broker_minutes} | End minutes: {end_minutes}")
                is_active = start_minutes <= broker_minutes <= end_minutes
            else:
                self.debug("Session does not span midnight")
                is_active = start_minutes <= broker_minutes <= end_minutes

        self.debug(f"Final session active status: {is_active}")
        return is_active

    @exception_handler
    async def get_economic_calendar(self, country: str, from_datetime_utc: datetime, to_datetime_utc: datetime) -> List[EconomicEvent]:
        """
        Retrieves economic events from the broker's economic calendar for the specified country and time range.
        """
        # Request the IDs
        broker_offset_hours = await self.get_broker_timezone_offset()
        from_datetime = from_datetime_utc + timedelta(hours=broker_offset_hours)
        to_datetime = from_datetime_utc + timedelta(hours=broker_offset_hours)
        events_ids_request = f"LIST_IDS:{country}:{dt_to_unix(from_datetime)}:{dt_to_unix(to_datetime)}"
        events_ids = await self.do_zmq_request(5557, events_ids_request)
        self.debug(f"Events ids: {events_ids} ")
        events = []
        for event_id in events_ids:
            # Request the individual event
            event_details_request = f"GET_EVENT:{country}:{dt_to_unix(from_datetime)}:{dt_to_unix(to_datetime)}:{event_id}"
            event = await self.do_zmq_request(5557, event_details_request)
            self.debug(f"Event details: {event}")
            events.append(map_from_metatrader(event, broker_offset_hours))

        return events

    @exception_handler
    async def get_symbol_price(self, symbol: str) -> Optional[SymbolPrice]:
        symbol_tick = mt5.symbol_info_tick(symbol)
        if symbol_tick is None:
            self.warning(f"{symbol} not found.")
            return None
        return SymbolPrice(symbol_tick.ask, symbol_tick.bid)

    @exception_handler
    async def get_broker_timezone_offset(self) -> Optional[int]:
        offset_hours = await self.do_zmq_request(5555, "GetBrokerTimezoneOffset")
        self.debug(f"Offset hours: {offset_hours} ")
        return offset_hours.get("time_difference")

    @exception_handler
    async def get_market_info(self, symbol: str) -> Optional[SymbolInfo]:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            self.warning(f"{symbol} not found.")
            return None
        return SymbolInfo(
            symbol=symbol,
            base=symbol_info.currency_base,
            quote=symbol_info.currency_profit,
            volume_min=symbol_info.volume_min,
            volume_max=symbol_info.volume_max,
            point=symbol_info.point,
            trade_mode=symbol_info.trade_mode,
            trade_contract_size=symbol_info.trade_contract_size,
            volume_step=symbol_info.volume_step,
            default_filling_mode=self.mt5_to_filling_type(symbol_info.filling_mode)
        )

    @exception_handler
    async def get_last_candles(self, symbol: str, timeframe: Timeframe, count: int = 1, position: int = 0) -> pd.DataFrame:
        """
        Retrieve the most recent closed candlestick data for a symbol.
        Args:
            symbol (str): The trading symbol (e.g., "EURUSD").
            timeframe: The timeframe for the candlesticks (e.g., M1, H1).
            count (int): Number of closed candles to retrieve. Default is 1.
            position (int): The starting position for retrieval (0-based index from the most recent closed).
                            0: Starts from the first completed (most recent closed) bar. # MODIFIED DOCSTRING
                            1: Starts from the second most recent closed bar. # MODIFIED DOCSTRING
                            n: Starts from the n+1 most recent closed bar (counting backwards). # MODIFIED DOCSTRING

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested candlestick data.
            Returns empty DataFrame on failure.
        """
        timezone_offset = await self.get_broker_timezone_offset()

        # Adjust position for MT5: 0 maps to MT5's 1 (most recent closed), 1 maps to MT5's 2, etc.
        start_position_for_mt5 = position + 1
        # Fetch exactly 'count' candles starting from the adjusted position
        bars_to_fetch = count

        rates = mt5.copy_rates_from_pos(symbol, self.timeframe_to_mt5(timeframe), start_position_for_mt5, bars_to_fetch)  # Use adjusted position
        if rates is None or len(rates) == 0:
            self.warning(f"No rates returned from mt5.copy_rates_from_pos for {symbol}, position={start_position_for_mt5}, count={bars_to_fetch}")
            return pd.DataFrame()  # Return empty DataFrame

        df = pd.DataFrame(rates)

        # Convert timestamp and calculate time_close
        df['time_open'] = pd.to_datetime(df['time'], unit='s')
        df.drop(columns=['time'], inplace=True)
        timeframe_duration = timeframe.to_seconds()
        df['time_close'] = df['time_open'] + pd.to_timedelta(timeframe_duration, unit='s')
        df['time_open_broker'] = df['time_open']  # Keep the original broker time
        df['time_close_broker'] = df['time_close']  # Keep the original broker time

        # Convert to UTC
        self.debug(f"Timezone offset: {timezone_offset} hours")
        if timezone_offset is not None:  # Added check for safety
            df['time_open'] -= pd.to_timedelta(timezone_offset, unit='h')
            df['time_close'] -= pd.to_timedelta(timezone_offset, unit='h')

        # Reorder columns
        columns_order = ['time_open', 'time_close', 'time_open_broker', 'time_close_broker']
        df = df[columns_order + [col for col in df.columns if col not in columns_order]]
        if not df.empty:
            self.debug(f"Last fetched candle - Close time (UTC): {df.iloc[-1]['time_close'].strftime('%d/%m/%Y %H:%M:%S')}")
        else:
            self.warning("Empty DataFrame after initial fetch.")
            return df  # Return empty if no data

        # --- Ensure Correct Number of Candles ---
        # Since we fetch exactly 'count' bars starting from a closed position,
        # the result should already have the correct number.
        final_df = df.reset_index(drop=True)  # Use the entire fetched df
        if len(final_df) != count:
            # This might happen if there isn't enough historical data for the requested position
            self.warning(f"Expected {count} candles but received {len(final_df)} fetching from position {start_position_for_mt5}. Check MT5 data availability.")
            # Return what was received
            pass  # Keep final_df as is

        if not final_df.empty:
            self.debug(f"Returning {len(final_df)} candles. Final last candle close time (UTC): {final_df.iloc[-1]['time_close'].strftime('%d/%m/%Y %H:%M:%S')}")
        else:
            self.debug("Returning empty DataFrame.")
        return final_df

    @exception_handler
    async def get_working_directory(self):
        terminal_info = mt5.terminal_info()
        return terminal_info.data_path + "\\MQL5\\Files"

    @exception_handler
    async def get_account_currency(self) -> str:
        account_info = mt5.account_info()
        if account_info is None:
            raise Exception("Failed to retrieve account information")
        self.info(f"Account currency: {account_info.currency}")
        return account_info.currency

    @exception_handler
    async def get_account_balance(self) -> float:
        account_info = mt5.account_info()
        if account_info is None:
            raise Exception("Failed to retrieve account information")
        self.info(f"Account balance: {account_info.balance}")
        return account_info.balance

    @exception_handler
    async def get_account_leverage(self) -> int:
        account_info = mt5.account_info()
        if account_info is None:
            raise Exception("Failed to retrieve account information")
        self.info(f"Account leverage: {account_info.leverage}")
        return account_info.leverage

    # Order Placement Methods

    async def get_filling_mode(self, symbol: str) -> FillingType:
        """
        Determines the supported filling mode for the specified symbol based on the requested action.

        Parameters:
          - symbol: The symbol to operate on (e.g., "EURUSD").

        Returns:
          - A FillingType value representing the supported filling mode (e.g., FillingType.RETURN).

        Note:
          - For MODIFY_ORDER and REMOVE_ORDER, filling mode checking is not applicable.
        """
        # Retrieve market information and the current price (functions assumed to be defined elsewhere)
        market_info: SymbolInfo = await self.get_market_info(symbol)
        symbol_price = await self.get_symbol_price(symbol)

        order_type = mt5.ORDER_TYPE_BUY
        price = round_to_point(symbol_price.ask, market_info.point)

        result = None
        # Iterate over possible type_filling values (e.g., 0, 1, 2, 3)
        for i in range(4):
            request = {
                "action": self.action_to_mt5(Action.PLACE_ORDER, OpType.BUY),
                "symbol": symbol,
                "volume": market_info.volume_min,
                "type": order_type,
                "price": price,
                "type_filling": i,
                "type_time": mt5.ORDER_TIME_GTC
            }
            result = mt5.order_check(request)
            # If the check returns "Done", then the filling mode is supported
            # and not result.comment == "Unsupported filling mode"
            if result and result.comment == "Done":
                return self.mt5_to_filling_type(i)

        additional_log = f" Response details: '{result.comment}'." if result is not None else ""
        raise ValueError(f"No valid filling mode found for symbol {symbol}.{additional_log}")

    @exception_handler
    async def place_order(self, request: OrderRequest) -> RequestResult:
        symbol_info = await self.get_market_info(request.symbol)
        if symbol_info is None:
            raise Exception(f"Symbol {request.symbol} not found")

        if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            raise Exception(f"Market is closed for symbol {request.symbol}, cannot place order.")

        try:
            op_type = self.order_type_to_mt5(request.order_type)
        except KeyError:
            raise ValueError(f"Invalid MT5 order type for order type: {request.order_type}")

        try:
            filling_type = self.filling_type_to_mt5(request.filling_mode)
        except KeyError:
            raise ValueError(f"Invalid MT5 filling type for filling mode: {request.filling_mode}")

        mt5_request = {
            "action": self.action_to_mt5(Action.PLACE_ORDER, request.order_type),
            "symbol": request.symbol,
            "volume": request.volume,
            "type": op_type,
            "price": request.order_price,
            # "stoplimit": request.order_price,
            "sl": request.sl,
            "tp": request.tp,
            "magic": request.magic_number if request.magic_number is not None else 0,
            "comment": request.comment if request.comment is not None else "order-placed-by-bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }

        self.debug(f"Send_order_request payload: {mt5_request}")
        result = mt5.order_send(mt5_request)
        response = RequestResult(request, result)

        if not response.success:
            ex = MT5Error(response.server_response_code, response.comment)
            self.error(f"Order failed, retcode={response.server_response_code}, description={response.comment}", exc_info=ex)

        return response

    @exception_handler
    async def close_position(self, position: Position, comment: Optional[str] = None, magic_number: Optional[int] = None) -> RequestResult:
        # Prepare request for closing the position
        filling_mode = await self.get_filling_mode(position.symbol)
        symbol_price = await self.get_symbol_price(position.symbol)

        if position.position_type == PositionType.LONG:
            price = symbol_price.bid
            order_type = mt5.ORDER_TYPE_SELL
        else:
            price = symbol_price.ask
            order_type = mt5.ORDER_TYPE_BUY

        close_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": position.symbol,
            "volume": position.volume,
            "type": order_type,
            "position": position.ticket,
            "price": price,
            "magic": magic_number if magic_number is not None else 0,
            "comment": comment if comment is not None else "position-closed-by-bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": self.filling_type_to_mt5(filling_mode),
        }

        result = mt5.order_send(close_request)
        req_result = RequestResult(close_request, result)
        if req_result.success:
            self.info(f"Position {position.ticket} successfully closed.")
        else:
            ex = self.get_last_error()
            self.error(f"Error closing position {position.ticket}, error code = {result.retcode}, message = {result.comment}", exc_info=ex)
            raise ex

        return req_result

    # Orders, Deals and Positions

    @exception_handler
    async def get_orders_by_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        orders_list = []
        timezone_offset = await self.get_broker_timezone_offset()

        for order_ticket in orders_ticket:
            try:
                orders = mt5.history_orders_get(ticket=order_ticket)
                mapped_orders = [self.map_order(order, timezone_offset) for order in orders]
                filtered_orders = list(filter(lambda order: order.magic_number == magic_number if magic_number else True, mapped_orders))

                if not filtered_orders:
                    self.warning(f"No order found with ticket {order_ticket}")
                    continue

                orders_list.append(filtered_orders[0])
            except Exception as e:
                self.error(f"Error retrieving orders", exc_info=e)

        return orders_list

    @exception_handler
    async def get_orders_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        timezone_offset = await self.get_broker_timezone_offset()

        from_tms_broker = from_tms_utc + timedelta(hours=timezone_offset)
        to_tms_broker = to_tms_utc + timedelta(hours=timezone_offset)

        from_unix = dt_to_unix(from_tms_broker)
        to_unix = dt_to_unix(to_tms_broker)

        orders = mt5.history_orders_get(date_from=from_unix, date_to=to_unix, group=f"*{symbol}*")

        if orders is None:
            return []

        mapped_orders = [self.map_order(order, timezone_offset) for order in orders]
        filtered_orders = list(filter(lambda order: order.magic_number == magic_number if magic_number else True, mapped_orders))
        sorted_orders = sorted(filtered_orders, key=lambda x: (x.symbol, x.time))

        return sorted_orders

    @exception_handler
    async def get_deals_by_orders_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> List[Deal]:
        timezone_offset = await self.get_broker_timezone_offset()

        deal_list = []

        for order_ticket in orders_ticket:
            try:
                deals = mt5.history_deals_get(ticket=order_ticket)
                mapped_deals: List[Deal] = [self.map_deal(deal, timezone_offset) for deal in deals]
                filtered_deals = list(filter(lambda deal: deal.magic_number == magic_number if magic_number else True, mapped_deals))

                if not filtered_deals:
                    self.warning(f"No deal found with ticket {order_ticket}")
                    continue

                if include_orders:
                    orders = await self.get_orders_by_ticket([order_ticket], symbol)
                    for deal in filtered_deals:
                        deal.order = orders[0]

                deal_list.append(filtered_deals[0])

            except Exception as e:
                self.error(f"Error retrieving orders", exc_info=e)

        return deal_list

    @exception_handler
    async def get_deals_by_position(self, positions_id: List[int], symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> dict[int, List[Deal]]:
        timezone_offset = await self.get_broker_timezone_offset()

        deal_list: dict[int, List[Deal]] = {}

        for position_id in positions_id:
            try:
                deals = mt5.history_deals_get(position=position_id)
                mapped_deals: List[Deal] = [self.map_deal(deal, timezone_offset) for deal in deals]
                filtered_deals = list(filter(lambda deal: deal.magic_number == magic_number if magic_number else True, mapped_deals))

                if not filtered_deals:
                    self.warning(f"No deal found with ticket {position_id}")
                    continue

                if include_orders:
                    order_tickets = list(deal.order_id for deal in filtered_deals)
                    position_orders = await self.get_orders_by_ticket(order_tickets, symbol, magic_number)
                    order_dict = {order.ticket: order for order in position_orders}

                    for deal in filtered_deals:
                        deal.order = order_dict.get(deal.order_id)

                ordered_deals = sorted(filtered_deals, key=lambda x: (x.symbol, x.time))
                deal_list[position_id] = ordered_deals
            except Exception as e:
                self.error(f"Error retrieving orders", exc_info=e)

        return deal_list

    @exception_handler
    async def get_deals_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> List[Deal]:
        timezone_offset = await self.get_broker_timezone_offset()

        from_tms_broker = from_tms_utc + timedelta(hours=timezone_offset)
        to_tms_broker = to_tms_utc + timedelta(hours=timezone_offset)

        from_unix = dt_to_unix(from_tms_broker)
        to_unix = dt_to_unix(to_tms_broker)

        deals = mt5.history_deals_get(from_unix, to_unix, group=f"*{symbol}*")

        if not deals:
            return []

        mapped_deals: List[Deal] = [self.map_deal(deal, timezone_offset) for deal in deals]
        filtered_deals = list(filter(lambda deal: deal.magic_number == magic_number if magic_number else True, mapped_deals))
        sorted_deals = sorted(filtered_deals, key=lambda x: (x.symbol, x.time))

        if include_orders:
            order_tickets = list(deal.order_id for deal in filtered_deals)
            deals_orders = await self.get_orders_by_ticket(order_tickets, symbol, magic_number)

            if not deals_orders:
                self.warning(f"No orders found for deals in range {from_tms_utc} to {to_tms_utc}")
                return sorted_deals

            for deal in sorted_deals:
                deal.order = deals_orders[0]

        return sorted_deals

    @exception_handler
    async def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        open_positions = mt5.positions_get(symbol=symbol)

        if not open_positions:
            return []

        timezone_offset = await self.get_broker_timezone_offset()
        mapped_positions: List[Position] = [self.map_open_position(pos, timezone_offset) for pos in open_positions]

        oldest_time = min(mapped_positions, key=lambda pos: pos.time).time

        deals = await self.get_deals_in_range(oldest_time, now_utc(), symbol, magic_number, include_orders=True)

        for position in mapped_positions:
            position.deals = list(filter(lambda deal: deal.position_id == position.position_id, deals))

        if magic_number is not None:
            mapped_positions = [position for position in mapped_positions if position.deals]

        return mapped_positions

    @exception_handler
    async def get_closed_positions(self, open_from_tms_utc: datetime, open_to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        deals = await self.get_deals_in_range(open_from_tms_utc, open_to_tms_utc, symbol, magic_number, include_orders=False)

        if deals is None:
            self.warning(f"No deals found for symbol {symbol} in the specified range.")

        position_ids = list(set(deal.position_id for deal in deals))
        positions = []

        for position_id in position_ids:
            try:
                position_deals_dict = await self.get_deals_by_position([position_id], symbol, magic_number, include_orders=True)
                position_deals = position_deals_dict.get(position_id, [])

                # Check if an exit deal exists among the deals for the position.
                exit_exists = any(deal.deal_type == DealType.EXIT for deal in position_deals)
                if not exit_exists:
                    self.debug(f"Position {position_id} does not have an exit deal; skipping.")
                    continue

                total_profit = sum(deal.profit for deal in position_deals)

                position = Position(
                    position_id=position_id,
                    symbol=position_deals[0].symbol,
                    open=False,
                    deals=position_deals,
                    profit=total_profit
                )
                positions.append(position)
            except Exception as e:
                self.error(f"Error while processing position {position_id}", exc_info=e)
                continue

        return positions

    # --- Heartbeat and Connection Management ---

    async def start_heartbeat(self):
        """Starts the background heartbeat task if not already running."""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self.info("Starting MT5 connection heartbeat task...")
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        else:
            self.warning("Heartbeat task already running.")

    async def stop_heartbeat(self):
        """Stops the background heartbeat task."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self.info("Stopping MT5 connection heartbeat task...")
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                self.info("Heartbeat task cancelled successfully.")
            except Exception as e:
                self.error("Error waiting for heartbeat task to cancel", exc_info=e)
            self._heartbeat_task = None
        else:
            self.info("Heartbeat task not running or already stopped.")

    def is_connected(self) -> bool:
        return self._is_connected

    async def _check_connection(self) -> bool:
        """
        Checks if the MT5 connection is active.
        """
        if not self._running:  # Don't check if broker is shutting down
            return False
        try:
            # Use a lightweight check like terminal_info or account_info
            info = mt5.terminal_info()
            if info is None:
                # Try account_info as a fallback check
                acc_info = mt5.account_info()
                if acc_info is None:
                    err = self.get_last_error()
                    self.warning(f"MT5 connection check failed (terminal_info and account_info are None). Last error: {err}")
                    return False
                # If account_info worked but terminal_info didn't, still consider connected
                return True

            # If terminal_info worked, we are connected
            return True
        except Exception as e:
            # Catch potential runtime errors if MT5 is in a bad state
            self.error(f"Exception during MT5 connection check", exc_info=e)
            return False

    async def _attempt_connect(self) -> bool:
        """
        Attempts to initialize and log in to MT5.
        """
        try:
            # Ensure previous instance is shut down if attempting re-initialization
            # This might be necessary if initialize fails partially
            try:
                mt5.shutdown()
                self.debug("Called mt5.shutdown() before attempting initialization.")
            except Exception as e2:
                self.error("Unexpected exception during MT5 shutdown attempt", exc_info=e2)
                pass  # Ignore errors during preemptive shutdown

            if not mt5.initialize(path=self.path):
                err = self.get_last_error()
                self.error(f"MT5 initialization failed. Error: {err}")
                mt5.shutdown()  # Attempt cleanup
                return False
            self.info("MT5 initialized successfully.")

            if not mt5.login(self.account, password=self.password, server=self.server):
                err = self.get_last_error()
                self.error(f"MT5 login failed for account #{self.account}. Error: {err}")
                mt5.shutdown()  # Attempt cleanup
                return False
            self.info(f"MT5 login successful for account #{self.account}.")
            self.info(f"Account Info: {mt5.account_info()}")
            self.info(f"Terminal Info: {mt5.terminal_info()}")
            return True
        except Exception as e:
            self.error("Unexpected exception during MT5 connect attempt", exc_info=e)
            try:
                mt5.shutdown()  # Ensure cleanup on unexpected error
            except Exception as e2:
                self.error("Unexpected exception during MT5 connect attempt", exc_info=e2)
            return False

    async def _heartbeat_loop(self):
        """Periodically checks the connection and attempts reconnection if needed."""
        self.info("Heartbeat loop started.")
        while self._running:
            await asyncio.sleep(self._heartbeat_interval)  # Wait for the interval

            if not self._running:  # Check again after sleep
                break

            if not await self._check_connection():
                self.warning("MT5 connection lost. Attempting to reconnect...")
                self._is_connected = False
                self._connection_status_event.clear()  # Signal connection down

                reconnected = False
                for attempt in range(self._reconnect_attempts):
                    if not self._running:  # Check if shutdown was initiated during retry
                        break
                    self.info(f"Reconnect attempt {attempt + 1}/{self._reconnect_attempts}...")
                    if await self._attempt_connect():
                        self.info("MT5 reconnection successful.")
                        self._is_connected = True
                        self._connection_status_event.set()  # Signal connection up
                        reconnected = True
                        break  # Exit retry loop on success
                    else:
                        self.warning(f"Reconnect attempt {attempt + 1} failed.")
                        if self._running:  # Only sleep if not shutting down
                            await asyncio.sleep(self._reconnect_delay)

                if not reconnected and self._running:
                    self.error(f"Failed to reconnect to MT5 after {self._reconnect_attempts} attempts. Heartbeat will continue checking.")
                    # Optionally: Implement further logic like notifying an admin

            else:
                # Connection is okay, ensure event is set (might be redundant but safe)
                if not self._is_connected:
                    self.info("Connection check successful (was previously down).")
                    self._is_connected = True
                    self._connection_status_event.set()
                else:
                    self.debug("Heartbeat: MT5 connection check successful.")

        self.info("Heartbeat loop finished.")

        # --- Helper to wait for connection ---

    async def _wait_for_connection(self, timeout: float = 60.0):
        """
        Waits for the connection event to be set, with a timeout.
        """
        try:
            await asyncio.wait_for(self._connection_status_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise ConnectionError(f"MT5 Broker connection not established within {timeout} seconds.")
        if not self._is_connected:
            raise ConnectionError("MT5 Broker is not connected.")

    # Classification and Mapping Methods
    def classify_order(self, order_obj: Any) -> Tuple[OrderType, Optional[OrderSource]]:
        order_type = ORDER_TYPE_MAPPING.get(order_obj.type, PositionType.OTHER)
        order_source = REASON_MAPPING.get(order_obj.reason, OrderSource.OTHER)
        return order_type, order_source

    def classify_position(self, pos_obj: Any) -> Tuple[PositionType, Optional[OrderSource]]:
        pos_type = POSITION_TYPE_MAPPING.get(pos_obj.type, PositionType.OTHER)
        order_source = REASON_MAPPING.get(pos_obj.reason, OrderSource.OTHER)
        return pos_type, order_source

    def classify_deal(self, deal_obj: Any) -> Tuple[DealType, Optional[OrderSource]]:
        deal_type = DEAL_TYPE_MAPPING.get(deal_obj.entry, DealType.OTHER)
        order_source = REASON_MAPPING.get(deal_obj.reason, OrderSource.OTHER)
        return deal_type, order_source

    def map_open_position(self, pos_obj: Any, timezone_offset: int) -> Position:
        pos_type, order_source = self.classify_position(pos_obj)
        return Position(
            position_id=pos_obj.identifier,
            ticket=pos_obj.ticket,
            volume=pos_obj.volume,
            symbol=pos_obj.symbol,
            time=unix_to_datetime(pos_obj.time) - timedelta(hours=timezone_offset) if pos_obj.time else None,
            last_update_timestamp=unix_to_datetime(pos_obj.time_update) - timedelta(hours=timezone_offset) if pos_obj.time_update else None,
            price_open=pos_obj.price_open,
            price_current=pos_obj.price_current,
            swap=pos_obj.swap,
            profit=pos_obj.profit,
            sl=pos_obj.sl,
            tp=pos_obj.tp,
            position_type=pos_type,
            order_source=order_source,
            comment=pos_obj.comment,
            open=True
        )

    def map_deal(self, deal_obj: Any, timezone_offset: int) -> Deal:
        time = unix_to_datetime(deal_obj.time) - timedelta(hours=timezone_offset) if deal_obj.time else None
        deal_type, order_source = self.classify_deal(deal_obj)

        return Deal(
            ticket=deal_obj.ticket,  # is equal to order id
            order_id=deal_obj.order,
            time=time,
            magic_number=deal_obj.magic,
            position_id=deal_obj.position_id,
            volume=deal_obj.volume,
            execution_price=deal_obj.price,
            commission=deal_obj.commission,
            swap=deal_obj.swap,
            profit=deal_obj.profit,
            fee=deal_obj.fee,
            symbol=deal_obj.symbol,
            comment=deal_obj.comment,
            external_id=str(deal_obj.ticket),
            deal_type=deal_type,
            order_source=order_source
        )

    def map_order(self, order: any, timezone_offset: int) -> BrokerOrder:
        time_setup = datetime.fromtimestamp(order.time_setup) - timedelta(hours=timezone_offset)
        time_done = datetime.fromtimestamp(order.time_done) - timedelta(hours=timezone_offset)

        order_type, order_source = self.classify_order(order)
        filling_mode = self.mt5_to_filling_type(order.type_filling)

        return BrokerOrder(
            ticket=order.ticket,
            order_type=order_type,
            symbol=order.symbol,
            order_price=order.price_open,
            price_current=order.price_current,
            volume=order.volume_initial,
            sl=order.sl if hasattr(order, 'sl') else None,
            tp=order.tp if hasattr(order, 'tp') else None,
            comment=order.comment if hasattr(order, 'comment') else None,
            time_setup=time_setup,
            time_done=time_done,
            position_id=order.position_id,
            filling_mode=filling_mode,
            magic_number=order.magic if hasattr(order, 'magic') else None,
            order_source=order_source
        )

    async def do_zmq_request(self, port: int, request: str, timeout: int = 30 * 1000) -> Dict[str, any]:
        # Create a context and a DEALER socket
        context = zmq.Context()
        try:
            with context.socket(zmq.DEALER) as dealer:
                # Generate a unique identity for the socket
                identity = new_id()
                dealer.setsockopt_string(zmq.IDENTITY, identity)

                # Connect to the server
                dealer.connect(f"tcp://127.0.0.1:{port}")

                # Send the request
                dealer.send_string(request)

                # Use a poller to wait for a response
                poller = zmq.Poller()
                poller.register(dealer, zmq.POLLIN)

                socks = dict(poller.poll(timeout))
                if dealer in socks and socks[dealer] == zmq.POLLIN:
                    # Receive all frames of the message
                    messages = []
                    while True:
                        try:
                            part = dealer.recv_string()
                            messages.append(part)
                            # Check if there are additional frames
                            if not dealer.getsockopt(zmq.RCVMORE):
                                break
                        except zmq.Again:
                            break
                    # The response message is in the last frame
                    response = messages[-1]
                else:
                    raise TimeoutError(f"Request timed out after {timeout} ms.")
        finally:
            # Terminate the context to release resources
            context.term()
            return json.loads(response)


class ServerTimeReader:
    def __init__(self, sandbox_dir: str, broker: MT5Broker, semaphore_timeout: int = 30):
        self.broker = broker
        self.semaphore_timeout = semaphore_timeout
        self.sandbox_dir = sandbox_dir

    async def read_get_broker_timezone_offset(self) -> int:
        semaphore_file = "ServerTime_Service_lockfile.lock"
        timestamp_file = "server_timestamp.json"
        try:
            semaphore_file_path = os.path.join(self.sandbox_dir, semaphore_file)
            timestamp_file_path = os.path.join(self.sandbox_dir, timestamp_file)

            wait_time = 0
            while os.path.exists(semaphore_file_path):
                self.broker.logger.info("Semaphore file active. Waiting for file access...")
                await asyncio.sleep(1)
                wait_time += 1
                if wait_time >= self.semaphore_timeout:
                    raise TimeoutError("Timeout waiting for semaphore file release.")

            # Check if the timestamp file exists
            if not os.path.exists(timestamp_file_path):
                raise FileNotFoundError(f"Timestamp file '{timestamp_file_path}' not found.")

            # Read and parse the timestamp file
            with open(timestamp_file_path, 'r') as f:
                data = json.load(f)

            return int(data.get("time_difference"))

        except FileNotFoundError:
            self.broker.logger.error(f"Error: {timestamp_file} not found.")
            raise
        except json.JSONDecodeError as e:
            self.broker.logger.error(f"Error decoding JSON: {e}")
            raise
        except Exception as e:
            self.broker.logger.error(f"Unexpected error reading server timestamp: {e}")
            raise


class MT5Error(Exception):
    """Custom exception to wrap MT5 errors."""

    def __init__(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message
        super().__init__(f"MT5 Error {error_code}: {error_message}")
