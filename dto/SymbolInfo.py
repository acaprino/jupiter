from misc_utils.enums import FillingType


class SymbolInfo:
    def __init__(self, symbol, volume_min, volume_max, point, trade_mode, trade_contract_size, volume_step, default_filling_mode: FillingType):
        self.symbol = symbol
        self.volume_min = volume_min
        self.volume_max = volume_max
        self.point = point
        self.trade_mode = trade_mode
        self.trade_contract_size = trade_contract_size
        self.volume_step = volume_step
        self.default_filling_mode = default_filling_mode

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, value):
        self._symbol = value

    @property
    def volume_min(self):
        return self._volume_min

    @volume_min.setter
    def volume_min(self, value):
        self._volume_min = value

    @property
    def volume_max(self):
        return self._volume_max

    @volume_max.setter
    def volume_max(self, value):
        self._volume_max = value

    @property
    def point(self):
        return self._point

    @point.setter
    def point(self, value):
        self._point = value

    @property
    def trade_mode(self):
        return self._trade_mode

    @trade_mode.setter
    def trade_mode(self, value):
        self._trade_mode = value

    @property
    def default_filling_mode(self) -> FillingType:
        return self._default_filling_mode

    @default_filling_mode.setter
    def default_filling_mode(self, value: FillingType):
        self._default_filling_mode = value

    def __repr__(self):
        return f"SymbolInfo(symbol='{self.symbol}', volume_min={self.volume_min}, point={self.point}, trade_mode={self.trade_mode})"

