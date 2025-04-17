from csv_loggers.logger_csv import CSVLogger
from misc_utils.config import ConfigReader


class RabbitMessages(CSVLogger):

    def __init__(self, config: ConfigReader, custom_name=None):
        bot_name = config.get_bot_name()
        instance_name = config.get_instance_name()
        custom_name = f"_{custom_name}" if custom_name is not None else ''
        output_path = None
        logger_name = f'messages{custom_name}_{bot_name}_{instance_name}'
        super().__init__(config, logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_message(self, exchange: str,
                    routing_key: str,
                    body: str,
                    message_id: str,
                    direction: str):
        record = {
            'direction': direction.upper(),
            'exchange': exchange,
            'routing_key': routing_key,
            'message_id': message_id,
            'body': body
        }
        self.record(record)
