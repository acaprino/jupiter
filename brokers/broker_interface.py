from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict

from pandas import Series

from dto import SymbolInfo, SymbolPrice
from dto.BrokerOrder import BrokerOrder
from dto.Deal import Deal
from dto.EconomicEvent import EconomicEvent
from dto.OrderRequest import OrderRequest
from dto.Position import Position
from dto.RequestResult import RequestResult
from misc_utils.enums import Timeframe


class BrokerAPI(ABC):

    @abstractmethod
    async def startup(self) -> bool:
        pass

    @abstractmethod
    async def get_last_candles(self, symbol: str, timeframe: Timeframe, count: int = 1, position: int = 0) -> Series:
        pass

    @abstractmethod
    async def get_symbol_price(self, symbol: str) -> SymbolPrice:
        pass

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> RequestResult:
        pass

    @abstractmethod
    async def get_market_info(self, symbol: str) -> SymbolInfo:
        pass

    @abstractmethod
    async def get_filling_mode(self, symbol: str) -> SymbolInfo:
        pass

    @abstractmethod
    async def is_market_open(self, symbol: str) -> bool:
        pass

    @abstractmethod
    async def get_broker_timezone_offset(self) -> Optional[int]:
        pass

    @abstractmethod
    async def get_working_directory(self) -> str:
        pass

    @abstractmethod
    async def shutdown(self):
        pass

    @abstractmethod
    async def get_account_balance(self) -> float:
        pass

    @abstractmethod
    async def get_account_leverage(self) -> float:
        pass

    @abstractmethod
    async def close_position(self, position: Position, comment: Optional[str] = None, magic_number: Optional[int] = None) -> RequestResult:
        pass

    @abstractmethod
    async def get_orders_by_ticket(self, orders_ticket: List[int], symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        pass

    @abstractmethod
    async def get_orders_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int]) -> List[BrokerOrder]:
        pass

    @abstractmethod
    async def get_deals_by_position(self, positions_id: List[int], symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> dict[int, List[Deal]]:
        pass

    @abstractmethod
    async def get_deals_in_range(self, from_tms_utc: datetime, to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None, include_orders: bool = True) -> List[Deal]:
        pass

    @abstractmethod
    async def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        pass

    @abstractmethod
    async def get_historical_positions(self, open_from_tms_utc: datetime, open_to_tms_utc: datetime, symbol: str, magic_number: Optional[int] = None) -> List[Position]:
        pass

    @abstractmethod
    async def get_broker_name(self) -> str:
        pass

    @abstractmethod
    async def get_economic_calendar(self, country: str, from_datetime: datetime, to_datetime: datetime) -> List[EconomicEvent]:
        pass