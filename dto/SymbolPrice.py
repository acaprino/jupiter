class SymbolPrice:
    def __init__(self, ask, bid):
        self._ask = ask
        self._bid = bid

    @property
    def ask(self):
        return self._ask

    @ask.setter
    def ask(self, value):
        self._ask = value

    @property
    def bid(self):
        return self._bid

    @bid.setter
    def bid(self, value):
        self._bid = value

    def __str__(self):
        return f"SymbolInfoTick(ask='{self.ask}', bid={self.bid})"

    def __repr__(self):
        return self.__str__()
