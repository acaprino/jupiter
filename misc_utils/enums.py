from enum import Enum, auto

from aio_pika import ExchangeType


class OpType(Enum):
    BUY = "Buy"
    SELL = "Sell"

    @property
    def label(self):
        return self.value


class TradingDirection(Enum):
    SHORT = auto()
    LONG = auto()


class Indicators(Enum):
    STOCHASTIC_K = "Stochastic_K"
    STOCHASTIC_D = "Stochastic_D"
    SUPERTREND = "Supertrend"
    MOVING_AVERAGE = "Moving_Average"
    ATR = "ATR"


class Signals(Enum):
    SMA_CROSS_UP = "SMA_Cross_Up"
    SMA_CROSS_DOWN = "SMA_Cross_Down"
    STOCHASTIC_CROSS_UP = "Stoch_Cross_Up"
    STOCHASTIC_CROSS_DOWN = "Stoch_Cross_Down"
    SUPERTREND_CROSS_UP = "Supertrend_Cross_Up"
    SUPERTREND_CROSS_DOWN = "Supertrend_Cross_Down"


class TradingMode(Enum):
    LONG_ONLY = "SYMBOL_TRADE_MODE_LONGONLY"
    SHORT_ONLY = "SYMBOL_TRADE_MODE_SHORTONLY"
    CLOSE_ONLY = "SYMBOL_TRADE_MODE_CLOSEONLY"
    FULL = "SYMBOL_TRADE_MODE_FULL"
    DISABLED = "SYMBOL_TRADE_MODE_DISABLED"


class OrderClosure(Enum):
    TAKE_PROFIT = auto()
    STOP_LOSS = auto()
    KEEP = auto()


class Timeframe(Enum):
    M1 = 1
    M5 = 2
    M15 = 3
    M30 = 4
    H1 = 5
    H4 = 6
    D1 = 7

    def to_seconds(self):
        seconds_per_candle = {
            Timeframe.M1: 60,
            Timeframe.M5: 300,
            Timeframe.M15: 900,
            Timeframe.M30: 1800,
            Timeframe.H1: 3600,
            Timeframe.H4: 14400,
            Timeframe.D1: 86400
        }
        return seconds_per_candle[self]

    def to_minutes(self):
        return self.to_seconds() / 60

    def to_hours(self):
        return self.to_minutes() / 60


class NotificationLevel(Enum):
    DEBUG = 0
    DEFAULT = 1


class FillingType(Enum):
    FOK = "Fill-Or-Kill (FOK)"
    IOC = "Immediate-Or-Cancel (IOC)"
    RETURN = "Return (RETURN)"


class OrderType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    OTHER = "OTHER"


class DealType(Enum):
    EXIT = "Exit"
    ENTER = "Enter"
    OTHER = "Other"


class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    OTHER = "OTHER"


class OrderSource(Enum):
    STOP_LOSS = "Stop Loss"
    TAKE_PROFIT = "Take Profit"
    MANUAL = "Manual"
    BOT = "Bot"
    OTHER = "Other"


class Mode(Enum):
    STANDALONE = "STANDALONE"
    GENERATOR = "GENERATOR"
    SENTINEL = "SENTINEL"


class RabbitExchange(Enum):
    REGISTRATION = (1, ExchangeType.DIRECT, "registration.exchange")
    SIGNALS = (2, ExchangeType.DIRECT)
    SIGNALS_CONFIRMATIONS = (3, ExchangeType.TOPIC)
    ENTER_SIGNAL = (4, ExchangeType.TOPIC)
    NOTIFICATIONS = (5, ExchangeType.TOPIC)
    ECONOMIC_EVENTS = (6, ExchangeType.TOPIC)

    def __init__(self, value: int, exchange_type: ExchangeType, routing_key: str = None):
        self._value_ = value
        self.exchange_type = exchange_type
        self.routing_key = routing_key
