import csv
import os
import io
import logging
import pandas as pd

from datetime import datetime
from logging.handlers import RotatingFileHandler

from misc_utils.config import ConfigReader
from misc_utils.logger_mixing import LoggingMixin
from misc_utils.utils_functions import create_directories


class CSVLogger(LoggingMixin):
    """
    A logger class that records events into a CSV file, with options for real-time logging, buffer-based logging,
    and rotation based on file size.
    """
    _instances = {}

    def __new__(cls, *args, **kwargs):
        """Implements singleton pattern per unique combination of arguments."""
        key = (cls, args, frozenset(kwargs.items()))
        if key not in cls._instances:
            cls._instances[key] = super(CSVLogger, cls).__new__(cls)
        return cls._instances[key]

    def __init__(self, config: ConfigReader, logger_name=None, output_path=None, real_time_logging=False, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0):
        if not hasattr(self, 'initialized'):  # Ensures initialization runs only once
            super().__init__(config)
            self.initialized = False
            self.logger = None
            self.real_time_logging = real_time_logging
            self.csv_file_path = None
            self.log_buffer = []
            self.memory_buffer_size = memory_buffer_size
            self._initialize_logger(logger_name, output_path, max_bytes, backup_count)

    def _initialize_logger(self, logger_name, output_path, max_bytes, backup_count):
        """Sets up the logging mechanism, directories, and file paths."""
        self.full_output_path = f"output/{output_path}" if output_path else "output"
        create_directories(self.full_output_path)

        # Define paths and file names
        self.relative_output_path = output_path
        #current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.file_name = f"{logger_name}.csv" if logger_name else None
        self.csv_file_path = os.path.join(self.full_output_path, self.file_name) if logger_name else None

        # Initialize logger with rotating file handler
        if self.csv_file_path:
            self.logger = logging.getLogger(logger_name)
            handler = RotatingFileHandler(self.csv_file_path, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(handler)

        self.initialized = True

    def record(self, event):
        """Records an event to the buffer or directly to the CSV file if real-time logging is enabled."""
        if self.real_time_logging:
            self._write_to_csv([event])  # Write directly to CSV
        self._add_to_buffer(event)

    def flush_to_csv(self):
        """Flushes the buffered log events to the CSV file."""
        if not self.real_time_logging and self.log_buffer:
            self._write_to_csv(self.log_buffer)
            self.reset_buffer()

    def _write_to_csv(self, rows):
        """Writes a list of rows to the CSV file."""
        if not rows:
            return

        df = pd.DataFrame(rows)
        buffer = io.StringIO()
        header = not os.path.exists(self.csv_file_path) or os.path.getsize(self.csv_file_path) == 0
        df.to_csv(buffer, sep=';', index=False, header=header, date_format='%Y-%m-%d %H:%M:%S', decimal='.', quoting=csv.QUOTE_NONNUMERIC, quotechar='"')

        buffer.seek(0)
        for line in buffer:
            self.logger.info(line.strip())

    def _add_to_buffer(self, event):
        """Adds an event to the log buffer with buffer size management."""
        self.log_buffer.append(event)
        if len(self.log_buffer) > self.memory_buffer_size:
            self.log_buffer.pop(0)

    def reset_buffer(self):
        """Clears the log buffer."""
        self.log_buffer = []

    def get_latest_log(self):
        """Returns the latest event in the buffer, or None if buffer is empty."""
        return self.log_buffer[-1] if self.log_buffer else None

    def get_file_name(self) -> str:
        """Returns the name of the CSV log file."""
        return self.file_name

    def get_relative_output_path(self) -> str:
        """Returns the relative path to the output directory."""
        return self.relative_output_path

    def get_full_file_path(self) -> str:
        """Returns the full path to the CSV log file."""
        return self.csv_file_path

    def get_full_output_path(self) -> str:
        """Returns the full path to the output directory."""
        return self.full_output_path
