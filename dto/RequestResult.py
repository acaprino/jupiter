class RequestResult:
    def __init__(self, request, response):
        # Initialize with essential attributes, using None as the default value
        self._server_response_code = getattr(response, 'retcode', None)
        self._deal = getattr(response, 'deal', None)
        self._order = getattr(response, 'order', None)
        self._volume = getattr(response, 'volume', None)
        self._price = getattr(response, 'price', None)
        self._comment = getattr(request, 'comment', None)
        self._server_response_message = getattr(response, 'comment', '')
        self._symbol = getattr(response, 'symbol', None)
        self._sl = getattr(response, 'sl', None)
        self._tp = getattr(response, 'tp', None)
        self._magic_number = getattr(response, 'magic_number', None)
        self._success = self.is_order_request_successful(getattr(response, 'retcode', None))

        # Properties with getter and setter for response code

    def is_order_request_successful(self, return_code: int):
        # Define the retcode values that indicate a successful trade operation
        # https://www.mql5.com/en/docs/constants/errorswarnings/enum_trade_return_codes
        success_codes = [
            10008,  # TRADE_RETCODE_PLACED: Order placed
            10009  # TRADE_RETCODE_DONE: Request completed
        ]

        return return_code in success_codes

    @property
    def server_response_code(self):
        return self._server_response_code

    @server_response_code.setter
    def server_response_code(self, value):
        self._server_response_code = value
        self._success = self.is_order_request_successful(value)  # Update success status

    @property
    def deal(self):
        return self._deal

    @deal.setter
    def deal(self, value):
        self._deal = value

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        self._order = value

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value):
        self._volume = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        self._price = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def server_response_message(self):
        return self._server_response_message

    @server_response_message.setter
    def server_response_message(self, value):
        self._server_response_message = value

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, value):
        self._symbol = value

    @property
    def sl(self):
        return self._sl

    @sl.setter
    def sl(self, value):
        self._sl = value

    @property
    def tp(self):
        return self._tp

    @tp.setter
    def tp(self, value):
        self._tp = value

    @property
    def magic_number(self):
        return self._magic_number

    @magic_number.setter
    def magic_number(self, value):
        self._magic_number = value

    @property
    def success(self):
        return self._success

    def __str__(self) -> str:
        success_status = "Success" if self._success else "Failure"
        return (f"Symbol: {self._symbol}\n"
                f"Volume: {self._volume}\n"
                f"Price: {self._price}\n"
                f"Stop Loss: {self._sl}\n"
                f"Take Profit: {self._tp}\n"
                f"Magic Number: {self._magic_number}\n"
                f"Request Comment: {self._comment}\n"
                f"Response Comment: {self._server_response_message}\n"
                f"Deal: {self._deal}\n"
                f"Order: {self._order}\n"
                f"Response Code: {self._server_response_code} ({success_status})")

    def __repr__(self):
        return self.__str__()
