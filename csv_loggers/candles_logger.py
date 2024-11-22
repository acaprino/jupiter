from pandas import Series

from csv_loggers.csv_logger import CSVLogger


class CandlesLogger(CSVLogger):

    def __init__(self, symbol, timeframe, trading_direction, custom_name=''):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        output_path = None
        logger_name = f'candles_{symbol}_{timeframe}_{trading_direction}_{custom_name}'
        super().__init__(logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_candle(self, candle: Series):
        dic = candle.to_dict()
        self.record(dic)
