from pandas import Series

from csv_loggers.logger_csv import CSVLogger
from misc_utils.config import ConfigReader


class CandlesLogger(CSVLogger):

    def __init__(self, config: ConfigReader, symbol, timeframe, trading_direction, custom_name=None):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        custom_name = f"_{custom_name}" if custom_name is not None else ''
        output_path = None
        logger_name = f'candles{custom_name}_{bot_name}_{instance_name}_{symbol}_{timeframe}_{trading_direction}'
        super().__init__(config, logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_candle(self, candle: Series):
        dic = candle.to_dict()
        self.record(dic)
