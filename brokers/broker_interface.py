"""
Abstract Base Class for interacting with a broker.

The `BrokerAPI` defines the essential methods that any broker implementation should 
support, such as retrieving market data, placing orders, managing positions, and 
fetching account information. Concrete implementations of this class are expected to 
provide functionality for specific broker APIs.

Methods:
    startup(): Initialize the broker connection.
    shutdown(): Terminate the broker connection.
    get_last_candles(): Retrieve the last set of candlestick data.
    get_symbol_price(): Fetch the latest bid and ask prices for a symbol.
    place_order(): Place a new order with the broker.
    get_market_info(): Retrieve detailed information about a financial instrument.
    get_filling_mode(): Get the default order filling mode for a symbol.
    is_market_open(): Check if the market is open for a given symbol.
    get_broker_timezone_offset(): Get the broker's timezone offset from UTC.
    get_working_directory(): Fetch the broker's working directory path.
    get_account_currency(): Retrieve the current account currency.
    get_account_balance(): Retrieve the current account balance.
    get_account_leverage(): Retrieve the leverage applied to the account.
    close_position(): Close an existing trading position.
    get_orders_by_ticket(): Retrieve orders based on their ticket numbers.
    get_orders_in_range(): Retrieve orders within a specified time range.
    get_deals_by_position(): Retrieve deals associated with a specific position.
    get_deals_in_range(): Retrieve deals within a specified time range.
    get_open_positions(): Fetch all currently open positions.
    get_closed_positions(): Retrieve historical positions based on their opening times.
    get_broker_name(): Retrieve the name of the broker.
    get_economic_calendar(): Fetch economic events from the broker's calendar.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict
from pandas import Series

from dto.BrokerOrder import BrokerOrder
from dto.Deal import Deal
from dto.EconomicEvent import EconomicEvent
from dto.Position import Position
from dto.RequestResult import RequestResult
from dto.SymbolInfo import SymbolInfo
from dto.SymbolPrice import SymbolPrice
from misc_utils.enums import FillingType, Action


# Detailed method docstrings

class BrokerAPI(ABC):
    """
    Abstract Base Class for implementing a broker API.
    """

    @abstractmethod
    async def startup(self) -> bool:
        """
        Initialize the broker connection.

        Returns:
            bool: True if the connection is successfully initialized, otherwise False.
        """
        pass

    async def test(self):
        pass

    @abstractmethod
    async def shutdown(self):
        """
        Terminate the broker connection and release any resources.
        """
        pass

    @abstractmethod
    async def get_last_candles(self, symbol: str, timeframe, count: int = 1, position: int = 0) -> Series:
        """
        Retrieve the most recent candlestick data for a symbol.

        Args:
            symbol (str): The trading symbol (e.g., "EURUSD").
            timeframe (Timeframe): The timeframe for the candlesticks (e.g., M1, H1).
            count (int): Number of candles to retrieve. Default is 1.
            position (int): The starting position for the candle retrieval, starting from the most recent one. Default is 0 (most recent candle).

        Returns:
            Series: A pandas Series containing the requested candlestick data.
        """
        pass

    @abstractmethod
    async def get_symbol_price(self, symbol: str) -> SymbolPrice:
        """
        Fetch the latest bid and ask prices for the specified symbol.

        Args:
            symbol (str): The trading symbol.

        Returns:
            SymbolPrice: An object containing bid and ask prices.
        """
        pass

    @abstractmethod
    async def place_order(self, request) -> RequestResult:
        """
        Place a new order with the broker.

        Args:
            request (OrderRequest): The order details, including symbol, volume, and price.

        Returns:
            RequestResult: The result of the order placement, including success or failure details.
        """
        pass

    @abstractmethod
    async def get_market_info(self, symbol: str) -> SymbolInfo:
        """
        Retrieve detailed information about a financial instrument.

        Args:
            symbol (str): The trading symbol.

        Returns:
            SymbolInfo: Detailed information about the instrument, including trading mode and volume limits.
        """
        pass

    @abstractmethod
    async def get_filling_mode(self, symbol: str) -> FillingType:
        """
        Get the default order filling mode for a symbol.

        Args:
            symbol (str): The trading symbol.

        Returns:
            FillingType: The filling mode used for placing orders.
        """
        pass

    @abstractmethod
    async def is_market_open(self, symbol: str, utc_dt: Optional[datetime] = None) -> bool:
        """
        Check if the market is open for a trading symbol.

        Args:
            symbol (str): The trading symbol.
            utc_dt (Optional[datetime]): UTC time for session validation (defaults to current UTC time).
                If provided, the trade mode disabled check is skipped.

        Returns:
            bool: True if the market is open, otherwise False.
        """
        pass

    @abstractmethod
    async def get_broker_timezone_offset(self) -> Optional[int]:
        """
        Get the broker's timezone offset from UTC.

        The offset is calculated as the broker's timestamp minus the UTC timestamp.
        It indicates the number of hours to subtract to convert broker time to UTC or
        to add to convert UTC to broker time.

        Returns:
            Optional[int]: The timezone offset in hours, or None if unavailable.
        """
        pass

    @abstractmethod
    async def get_working_directory(self) -> str:
        """
        Fetch the working directory path used by the broker terminal.

        This method is applicable only when the broker relies on locally installed software
        or a client terminal running on the machine.

        Returns:
            str: The path to the working directory.
        """
        pass

    @abstractmethod
    async def get_account_currency(self) -> str:
        """
                Retrieve the current account currency.

                Returns:
                    str: The account currency.
                """
        pass

    @abstractmethod
    async def get_account_balance(self) -> float:
        """
        Retrieve the current account balance.

        Returns:
            float: The account balance.
        """
        pass

    @abstractmethod
    async def get_account_leverage(self) -> float:
        """
        Retrieve the leverage applied to the account.

        Returns:
            float: The leverage value.
        """
        pass

    @abstractmethod
    async def close_position(self, position, comment: Optional[str] = None, magic_number: Optional[int] = None):
        """
        Close an existing trading position.

        Args:
            position (Position): The position to close.
            comment (Optional[str]): An optional comment for the closure.
            magic_number (Optional[int]): An optional identifier for the order.

        Returns:
            RequestResult: The result of the position closure.
        """
        pass

    @abstractmethod
    async def get_orders_by_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        """
        Retrieve orders based on their ticket numbers.

        Args:
            orders_ticket (List[int]): A list of order tickets.
            symbol (str): The trading symbol.
            magic_number (Optional[int]): An optional identifier for filtering orders.

        Returns:
            List[BrokerOrder]: A list of matching broker orders.
        """
        pass

    @abstractmethod
    async def get_orders_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        """
        Retrieve orders within a specified time range.

        Args:
            from_tms_utc (datetime): The start of the time range in UTC.
            to_tms_utc (datetime): The end of the time range in UTC.
            symbol (str): The trading symbol.
            magic_number (Optional[int]): An optional identifier for filtering orders.

        Returns:
            List[BrokerOrder]: A list of broker orders within the time range.
        """
        pass

    @abstractmethod
    async def get_deals_by_position(self, positions_id: List[int], symbol: str, magic_number: Optional[int] = None) -> Dict[int, List[Deal]]:
        """
        Retrieve deals associated with one or more specific positions.

        Args:
            positions_id (List[int]): List of position IDs to fetch deals for.
            symbol (str): The trading symbol.
            magic_number (Optional[int]): Optional identifier to filter deals by magic number.

        Returns:
            Dict[int, List[Deal]]: A dictionary where each position ID maps to its corresponding deals.
        """
        pass

    @abstractmethod
    async def get_deals_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None) -> List[Deal]:
        """
        Retrieve deals within a specified time range.

        Args:
            from_tms_utc (datetime): The start of the time range in UTC.
            to_tms_utc (datetime): The end of the time range in UTC.
            symbol (str): The trading symbol.
            magic_number (Optional[int]): An optional identifier for filtering deals.

        Returns:
            List[Deal]: A list of deals within the specified time range.
        """
        pass

    @abstractmethod
    async def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        """
        Fetch all currently open positions.

        Args:
            symbol (str): The trading symbol.
            magic_number (Optional[int]): An optional identifier for filtering positions.

        Returns:
            List[Position]: A list of currently open positions.
        """
        pass

    @abstractmethod
    async def get_closed_positions(self, open_from_tms_utc: datetime, open_to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        """
         Retrieve historical positions for a given symbol within a specified UTC time range.
         A position is considered closed if it contains at least one exit deal.

        Args:
            open_from_tms_utc (datetime): The start of the time range for position openings in UTC.
            open_to_tms_utc (datetime): The end of the time range for position openings in UTC.
            symbol (str): The trading symbol.
            magic_number (Optional[int]): An optional identifier for filtering positions.

        Returns:
            List[Position]: A list of historical positions within the specified time range.
        """
        pass

    @abstractmethod
    async def get_broker_name(self) -> str:
        """
        Retrieve the name of the broker.

        Returns:
            str: The broker's name.
        """
        pass

    @abstractmethod
    async def get_economic_calendar(self, country: str, from_datetime_utc: datetime, to_datetime_utc: datetime) -> List[EconomicEvent]:
        """
        Fetch economic events from the broker's calendar.

        Args:
            country (str): The country code for the events.
            from_datetime_utc (datetime): The start time for the events in UTC.
            to_datetime_utc (datetime): The end time for the events in UTC.

        Returns:
            List[EconomicEvent]: A list of economic events within the specified time range.
        """
        pass

    @abstractmethod
    async def start_heartbeat(self) -> None:
        """Starts the periodic monitoring of the connection."""
        pass

    @abstractmethod
    async def stop_heartbeat(self) -> None:
        """Stops the connection monitoring."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Returns True if the connection is active, False otherwise."""
        pass
