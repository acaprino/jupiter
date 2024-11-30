import asyncio
import math
import threading
from datetime import timedelta, datetime
from typing import Any, Optional, Tuple, List

import MetaTrader5 as mt5
import pandas as pd

from brokers.broker_interface import BrokerAPI
from dto.BrokerOrder import BrokerOrder
from dto.Deal import Deal
from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.RequestResult import RequestResult
from dto.SymbolInfo import SymbolInfo
from dto.SymbolPrice import SymbolPrice
from misc_utils.bot_logger import BotLogger
from misc_utils.enums import Timeframe, FillingType, OpType, DealType, OrderSource, PositionType, OrderType
from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import now_utc, dt_to_unix, unix_to_datetime

# https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties
# https://www.mql5.com/en/articles/40
# https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py
# https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py

ORDER_TYPE_MAPPING = {
    0: OrderType.BUY,  # DEAL_TYPE_BUY
    1: OrderType.SELL,  # DEAL_TYPE_SELL
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


class MT5Broker(BrokerAPI):
    _instance: Optional['MT5Broker'] = None
    lock = threading.Lock()  # Lock per garantire il thread safety

    # Definizione delle proprietà con valori di default
    def __init__(self):
        # Definire tutte le proprietà con valori predefiniti per evitare warning dell'IDE
        self.agent: Optional[str] = None
        self.logger: Optional[BotLogger] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.lock: Optional[asyncio.Lock] = None
        self.account: Optional[int] = None
        self.password: Optional[str] = None
        self.server: Optional[str] = None
        self.path: Optional[str] = None
        self._running: bool = False
        self._initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls.lock:
                if cls._instance is None:
                    cls._instance = super(MT5Broker, cls).__new__(cls)
        return cls._instance

    @classmethod
    def initialize(cls, agent: str, account: int = 0, password: str = "", server: str = "", path: str = ""):
        with cls.lock:
            instance = cls()
            if not instance._initialized:
                instance.agent = agent
                instance.logger = BotLogger.get_logger(agent)
                instance.loop = asyncio.get_event_loop()
                instance.lock = asyncio.Lock()
                instance.account = account
                instance.password = password
                instance.server = server
                instance.path = path
                instance._running = False
                instance._initialized = True
            else:
                raise Exception("MT5Broker è già stato inizializzato.")

    def is_initialized(self):
        return self._initialized

    @exception_handler
    async def startup(self) -> bool:
        if not self.is_initialized():
            raise Exception("MT5Broker non è stato inizializzato. Chiama MT5Broker.initialize(...) prima di usarlo.")

        async with self.lock:
            if not mt5.initialize(path=self.path):
                self.logger.error(f"initialization failed, error code {mt5.last_error()}")
                mt5.shutdown()
                raise Exception("Failed to initialize MT5")
            self.logger.info("MT5 initialized successfully")

            # Set up connection with MT5 account
            # config = ConfigReader.get_config(self.bot_name)

            self.logger.info(f"Trying to connect with account {self.account} and provided server credentials.")

            if not mt5.login(self.account, password=self.password, server=self.server):
                self.logger.error(f"Failed to connect to account #{self.account}, error code: {mt5.last_error()}")
                raise Exception("Failed to initialize MT5")
            self.logger.info("Login success")
            self.logger.info(mt5.account_info())
            self._running = True
            return True

    # Conversion Methods
    def filling_type_to_mt5(self, filling_type: FillingType) -> int:
        conversion_dict = {
            FillingType.FOK: mt5.ORDER_FILLING_FOK,
            FillingType.IOC: mt5.ORDER_FILLING_IOC,
            FillingType.RETURN: mt5.ORDER_FILLING_RETURN
        }
        return conversion_dict[filling_type]

    def mt5_to_filling_type(self, mt5_filling_type: int) -> FillingType:
        """Convert an MT5 filling type constant to the corresponding FillingType enum."""
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
            OpType.SELL: mt5.ORDER_TYPE_SELL
        }
        return conversion_dict[order_type]

    def mt5_to_order_type(self, mt5_order_type: int) -> OpType:
        conversion_dict = {
            mt5.ORDER_TYPE_BUY: OpType.BUY,
            mt5.ORDER_TYPE_SELL: OpType.SELL
        }
        return conversion_dict[mt5_order_type]

    # Utility and Market Data Methods
    @exception_handler
    async def is_market_open(self, symbol: str) -> bool:
        async with self.lock:
            symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            self.logger.warning(f"{symbol} not found, cannot retrieve symbol info.")
            return False
        return not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED

    @exception_handler
    async def get_symbol_price(self, symbol: str) -> Optional[SymbolPrice]:
        async with self.lock:
            symbol_tick = mt5.symbol_info_tick(symbol)
        if symbol_tick is None:
            self.logger.warning(f"{symbol} not found.")
            return None
        return SymbolPrice(symbol_tick.ask, symbol_tick.bid)

    @exception_handler
    async def get_broker_timezone_offset(self, symbol) -> Optional[int]:
        async with self.lock:
            symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            self.logger.warning(f"{symbol} not found, cannot retrieve symbol info.")
            return None

        if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            self.logger.warning(f"Market closed for {symbol}. Cannot get the broker server timezone offset.")
            return None

        # Get the current broker time and UTC time to calculate offset
        broker_time = symbol_info.time
        utc_unix_timestamp = dt_to_unix(now_utc())
        time_diff_seconds = abs(broker_time - utc_unix_timestamp)
        offset_hours = math.ceil(time_diff_seconds / 3600)

        self.logger.debug(f"Broker timestamp: {broker_time}, UTC timestamp: {utc_unix_timestamp}, Offset: {offset_hours} hours")
        return offset_hours

    @exception_handler
    async def get_market_info(self, symbol: str) -> Optional[SymbolInfo]:
        async with self.lock:
            symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            self.logger.warning(f"{symbol} not found.")
            return None
        return SymbolInfo(
            symbol=symbol,
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
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        async with self.lock:
            # Fetch one more candle than requested to potentially exclude the open candle
            rates = mt5.copy_rates_from_pos(symbol, self.timeframe_to_mt5(timeframe), position, count + 1)
            df = pd.DataFrame(rates)

            # Rename 'time' to 'time_open' and convert to datetime
            df['time_open'] = pd.to_datetime(df['time'], unit='s')
            df.drop(columns=['time'], inplace=True)

            # Calculate 'time_close' and add original broker times
            timeframe_duration = timeframe.to_seconds()
            df['time_close'] = df['time_open'] + pd.to_timedelta(timeframe_duration, unit='s')
            df['time_open_broker'] = df['time_open']
            df['time_close_broker'] = df['time_close']

            # Convert from broker timezone to UTC
            self.logger.debug(f"Timezone offset: {timezone_offset} hours")
            df['time_open'] -= pd.to_timedelta(timezone_offset, unit='h')
            df['time_close'] -= pd.to_timedelta(timezone_offset, unit='h')

            # Arrange columns for clarity
            columns_order = ['time_open', 'time_close', 'time_open_broker', 'time_close_broker']
            df = df[columns_order + [col for col in df.columns if col not in columns_order]]

            # Check and exclude the last candle if it's still open
            current_time = now_utc()
            self.logger.debug(f"Current UTC time: {current_time.strftime('%d/%m/%Y %H:%M:%S')}")
            if current_time < df.iloc[-1]['time_close']:
                self.logger.debug(f"Excluding last open candle with close time: {df.iloc[-1]['time_close'].strftime('%d/%m/%Y %H:%M:%S')}")
                df = df.iloc[:-1]

            # Ensure DataFrame has exactly 'count' rows
            return df.iloc[-count:].reset_index(drop=True)

    @exception_handler
    async def shutdown(self):
        async with self.lock:
            mt5.shutdown()
            self.logger.info("MT5 shutdown successfully.")

    @exception_handler
    async def get_working_directory(self):
        async with self.lock:
            terminal_info = mt5.terminal_info()
            return terminal_info.data_path + "\\MQL5\\Files"

    @exception_handler
    async def get_account_balance(self) -> float:
        async with self.lock:
            account_info = mt5.account_info()
            if account_info is None:
                raise Exception("Failed to retrieve account information")
            self.logger.info(f"Account balance: {account_info.balance}")
            return account_info.balance

    @exception_handler
    async def get_account_leverage(self) -> int:
        async with self.lock:
            account_info = mt5.account_info()
            if account_info is None:
                raise Exception("Failed to retrieve account information")
            self.logger.info(f"Account leverage: {account_info.leverage}")
            return account_info.leverage

    # Order Placement Methods
    @exception_handler
    async def get_filling_mode(self, symbol) -> FillingType:
        market_info = await self.get_market_info(symbol)
        symbol_price = await self.get_symbol_price(symbol)

        for i in range(4):
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": market_info.volume_min,
                "type": mt5.ORDER_TYPE_BUY,
                "price": symbol_price.ask,
                "type_filling": i,
                "type_time": mt5.ORDER_TIME_GTC
            }
            async with self.lock:
                result = mt5.order_check(request)
            if result and not result.comment == "Unsupported filling mode" and result.comment == "Done":
                return self.mt5_to_filling_type(i)

        add_part_log = f" Check response details: {result.comment}" if result is not None else ""
        raise ValueError(f"No valid filling mode found for symbol {symbol}.{add_part_log}")

    @exception_handler
    async def place_order(self, request: OrderRequest) -> RequestResult:

        # Implement order placement logic similar to the previous _place_order_sync
        symbol_info = await self.get_market_info(request.symbol)
        if symbol_info is None:
            raise Exception(f"Symbol {request.symbol} not found")

        if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            raise Exception(f"Market is closed for symbol {request.symbol}, cannot place order.")

        op_type = self.order_type_to_mt5(request.order_type)
        filling_type = self.filling_type_to_mt5(request.filling_mode)

        mt5_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": request.symbol,
            "volume": request.volume,
            "type": op_type,
            "price": request.order_price,
            "sl": request.sl,
            "tp": request.tp,
            "magic": request.magic_number if request.magic_number is not None else 0,
            "comment": request.comment if request.comment is not None else "",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }

        self.logger.debug(f"Send_order_request payload: {mt5_request}")
        async with self.lock:
            result = mt5.order_send(mt5_request)
        response = RequestResult(request, result)

        if not response.success:
            self.logger.error(f"Order failed, retcode={response.server_response_code}, description={response.comment}")

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
            "comment": comment if comment is not None else "",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": self.filling_type_to_mt5(filling_mode),
        }
        async with self.lock:
            result = mt5.order_send(close_request)
        req_result = RequestResult(close_request, result)
        if req_result.success:
            self.logger.info(f"Position {position.ticket} successfully closed.")
        else:
            self.logger.error(f"Error closing position {position.ticket}, error code = {result.retcode}, message = {result.comment}")

        return req_result

    # Orders, Deals and Positions

    @exception_handler
    async def get_orders_by_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        orders_list = []
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        for order_ticket in orders_ticket:
            try:
                orders = mt5.history_orders_get(ticket=order_ticket)
                mapped_orders = [self.map_order(order, timezone_offset) for order in orders]
                filtered_orders = list(filter(lambda order: order.magic_number == magic_number if magic_number else True, mapped_orders))

                if not filtered_orders:
                    self.logger.warning(f"No order found with ticket {order_ticket}")
                    continue

                orders_list.append(filtered_orders[0])
            except Exception as e:
                self.logger.error(f"Error retrieving orders: {e}")

        return orders_list

    @exception_handler
    async def get_orders_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        from_tms_broker = from_tms_utc + timedelta(hours=timezone_offset)
        to_tms_broker = to_tms_utc + timedelta(hours=timezone_offset)

        from_unix = dt_to_unix(from_tms_broker)
        to_unix = dt_to_unix(to_tms_broker)

        async with self.lock:
            orders = mt5.history_orders_get(date_from=from_unix, date_to=to_unix, group=f"*{symbol}*")

        if orders is None:
            return []

        mapped_orders = [self.map_order(order, timezone_offset) for order in orders]
        filtered_orders = list(filter(lambda order: order.magic_number == magic_number if magic_number else True, mapped_orders))
        sorted_orders = sorted(filtered_orders, key=lambda x: (x.symbol, x.time))

        return sorted_orders

    async def get_deals_by_orders_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> List[Deal]:
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        deal_list = []

        for order_ticket in orders_ticket:
            try:
                deals = mt5.history_deals_get(ticket=order_ticket)
                mapped_deals: List[Deal] = [self.map_deal(deal, timezone_offset) for deal in deals]
                filtered_deals = list(filter(lambda deal: deal.magic_number == magic_number if magic_number else True, mapped_deals))

                if not filtered_deals:
                    self.logger.warning(f"No deal found with ticket {order_ticket}")
                    continue

                if include_orders:
                    orders = await self.get_orders_by_ticket([order_ticket], symbol)
                    for deal in filtered_deals: deal.order = orders[0]

                deal_list.append(filtered_deals[0])

            except Exception as e:
                self.logger.error(f"Error retrieving orders: {e}")

            return deal_list

    @exception_handler
    async def get_deals_by_position(self, positions_id: List[int], symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> dict[int, List[Deal]]:
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        deal_list = {}

        for position_id in positions_id:
            try:
                deals = mt5.history_deals_get(position=position_id)
                mapped_deals: List[Deal] = [self.map_deal(deal, timezone_offset) for deal in deals]
                filtered_deals = list(filter(lambda deal: deal.magic_number == magic_number if magic_number else True, mapped_deals))

                if not filtered_deals:
                    self.logger.warning(f"No deal found with ticket {position_id}")
                    continue

                if include_orders:
                    order_tickets = list(deal.order_id for deal in filtered_deals)
                    position_orders = await self.get_orders_by_ticket(order_tickets, symbol, magic_number)
                    order_dict = {order.ticket: order for order in position_orders}

                    for deal in filtered_deals: deal.order = order_dict.get(deal.order_id)

                ordered_deals = sorted(filtered_deals, key=lambda x: (x.symbol, x.time))
                deal_list[position_id] = ordered_deals
            except Exception as e:
                self.logger.error(f"Error retrieving orders: {e}")

            return deal_list

    @exception_handler
    async def get_deals_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> List[Deal]:
        timezone_offset = await self.get_broker_timezone_offset(symbol)

        from_tms_broker = from_tms_utc + timedelta(hours=timezone_offset)
        to_tms_broker = to_tms_utc + timedelta(hours=timezone_offset)

        from_unix = dt_to_unix(from_tms_broker)
        to_unix = dt_to_unix(to_tms_broker)

        async with self.lock:
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
                self.logger.warning(f"No orders found for deals in range {from_tms_utc} to {to_tms_utc}")
                return sorted_deals

            for deal in sorted_deals:
                deal.order = deals_orders[0]

        return sorted_deals

    @exception_handler
    async def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        async with self.lock:
            open_positions = mt5.positions_get(symbol=symbol)

        if not open_positions:
            return []

        timezone_offset = await self.get_broker_timezone_offset(symbol)
        mapped_positions = [self.map_open_position(pos, timezone_offset) for pos in open_positions]

        oldest_time = min(mapped_positions, key=lambda pos: pos.time).time

        deals = await self.get_deals_in_range(oldest_time, now_utc(), symbol, magic_number, include_orders=True)

        for position in mapped_positions:
            position.deals = list(filter(lambda deal: deal.position_id == position.position_id, deals))

        return mapped_positions

    @exception_handler
    async def get_historical_positions(self, open_from_tms_utc: datetime, open_to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        deals = await self.get_deals_in_range(open_from_tms_utc, open_to_tms_utc, symbol, magic_number, include_orders=False)

        if deals is None:
            self.logger.warning(f"No deals found for symbol {symbol} in the specified range.")

        position_ids = list(set(deal.position_id for deal in deals))
        positions = []

        for position_id in position_ids:
            try:
                position_deals = await self.get_deals_by_position([position_id], symbol, magic_number, include_orders=True)
                position_deals = position_deals.get(position_id)

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
                self.logger.error(f"Error while processing position {position_id}: {e}")
                continue

        return positions

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
