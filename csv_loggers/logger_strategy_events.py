import re

from csv_loggers.logger_csv import CSVLogger
from misc_utils.utils_functions import now_utc


class StrategyEventsLogger(CSVLogger):

    def __init__(self, symbol, timeframe, trading_direction):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        output_path = None
        logger_name = f'strategy_signals_{symbol}_{timeframe}_{trading_direction}'
        super().__init__(logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_event(self, time_open, time_close, close_price, state_pre, state_cur, message, supert_fast_prev, supert_slow_prev, supert_fast_cur, supert_slow_cur, stoch_k_cur, stoch_d_cur):
        event = {
            'Candle open time': time_open,
            'Candle close time': time_close,
            'Timestamp': now_utc().strftime("%d/%m/%Y %H:%M:%S"),
            'Event': self.clean_text(message),
            'HA Close': close_price,
            'State prev.': state_pre,
            'State cur.': state_cur,
            'Supertrend Fast prev.': supert_fast_prev,
            'Supertrend Slow prev.': supert_slow_prev,
            'Supertrend Fast cur.': supert_fast_cur,
            'Supertrend Slow cur.': supert_slow_cur,
            'Stochastic K cur.': stoch_k_cur,
            'Stochastic D cur.': stoch_d_cur
        }
        self.record(event)

    def clean_text(self, text: str) -> str:
        """
        Removes emojis and trims whitespace from the beginning and end of the given string.

        Parameters:
        - text (str): The input string potentially containing emojis and extra whitespace.

        Returns:
        - str: The cleaned string with emojis and surrounding whitespace removed.
        """
        emoji_pattern = re.compile(
            r'[\U00010000-\U0010FFFF]',  # Match emojis in the supplementary planes
            flags=re.UNICODE
        )
        # Remove emojis and strip whitespace
        return emoji_pattern.sub(r'', text).strip()
