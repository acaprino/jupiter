import argparse
import asyncio
import sys
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor

import psutil

from agents.agent_market_state_notifier import MarketStateNotifierAgent
from agents.agent_strategy_adrastea import AdrasteaSignalGeneratorAgent
from agents.middleware import MiddlewareService
from agents.sentinel_closed_deals_agent import ClosedDealsAgent
from agents.sentinel_event_manager import EconomicEventsManagerAgent
from agents.sentinel_filled_orders_agent import FilledOrdersAgent
from brokers.broker_proxy import Broker
# Custom module imports
from brokers.mt5_broker import MT5Broker
from csv_loggers.logger_rabbit_messages import RabbitMessages
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader
from misc_utils.enums import Mode
from notifiers.executor_agent_adrastea import ExecutorAgent
from notifiers.notifier_market_state import NotifierMarketState
from notifiers.notifier_tick_updates import NotifierTickUpdates
from services.service_rabbitmq import RabbitMQService

# Suppress specific warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Configure standard input and output encoding
sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')


def calculate_workers(num_configs, max_workers=500):
    """
    Calculates the optimal number of worker threads based on system resources and the number of configurations.
    """
    # Base calculation
    workers = num_configs * 10

    # System memory constraints
    mem = psutil.virtual_memory()
    total_memory_gb = mem.total / (1024 ** 3)
    reserved_memory_percentage = 0.25
    usable_memory_gb = total_memory_gb * (1 - reserved_memory_percentage)
    per_worker_memory_gb = 0.02  # Estimated memory per worker
    memory_limit = int(usable_memory_gb / per_worker_memory_gb)

    # Final worker count
    workers = min(workers, memory_limit, max_workers)
    workers = max(1, workers)
    print(f"Calculated workers: {workers}")
    return workers


class BotLauncher:
    """
    A class to encapsulate the bot launching process, handling different modes and their specific routines.
    """

    def __init__(self, config_file, start_silent):
        self.agent = "BotLauncher"
        self.config_file = config_file
        self.start_silent = start_silent
        self.config = None
        self.logger = None
        self.mode = None
        self.routines = []
        self.executor = None
        self.loop = asyncio.get_event_loop()

    def load_configuration(self):
        """
        Loads the bot configuration from the specified file.
        """
        print(f"Loading configuration from: {self.config_file}")
        self.config = ConfigReader.load_config(config_file_param=self.config_file)
        self.config.register_param("start_silent", self.start_silent)
        if not self.config.get_enabled():
            print("Bot configuration not enabled. Exiting...")
            sys.exit()
        self.mode = self.config.get_bot_mode()
        self.logger = BotLogger.get_logger(name=self.config.get_bot_name(), level=self.config.get_bot_logging_level())

    def initialize_routines(self):
        """
        Initializes routines based on the bot mode and trading configurations.
        """

        print(f"Initializing routines for mode: {self.mode}")

        if self.mode == Mode.MIDDLEWARE:
            self.routines.append(MiddlewareService(self.config))
        else:
            trading_configs = self.config.get_trading_configurations()
            for tc in trading_configs:
                if self.mode == Mode.SENTINEL:
                    self.routines.append(ExecutorAgent(self.config, tc))
                elif self.mode == Mode.GENERATOR:
                    self.routines.append(AdrasteaSignalGeneratorAgent(self.config, tc))
                else:
                    raise ValueError(f"Invalid bot mode specified: {self.mode}")

            self.routines.append(MarketStateNotifierAgent(self.config, trading_configs))
            if self.mode == Mode.SENTINEL:
                self.routines.append(EconomicEventsManagerAgent(self.config, trading_configs))
                self.routines.append(ClosedDealsAgent(self.config, trading_configs))
                self.routines.append(FilledOrdersAgent(self.config, trading_configs))

    def setup_executor(self):
        """
        Configures the ThreadPoolExecutor based on the number of routines and system resources.
        """
        if self.mode == Mode.MIDDLEWARE:
            max_workers = 50
        else:
            max_workers = calculate_workers(len(self.routines))
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.loop.set_default_executor(self.executor)

        print(f"Initialized executor with {max_workers} workers.")

    def log_rabbit_message(self,
                           exchange: str,
                           routing_key: str,
                           body: str,
                           message_id: str,
                           direction: str):
        RabbitMessages(self.config).add_message(
            exchange=exchange,
            routing_key=routing_key,
            body=f"{body}",
            message_id=message_id,
            direction=direction
        )

    async def start_services(self):
        """
        Initializes and starts necessary services like RabbitMQ and the Broker.
        """
        # Initialize RabbitMQService
        rabbitmq_service = await RabbitMQService.get_instance(
            config=self.config,
            user=self.config.get_rabbitmq_username(),
            password=self.config.get_rabbitmq_password(),
            rabbitmq_host=self.config.get_rabbitmq_host(),
            port=self.config.get_rabbitmq_port(),
            loop=self.loop
        )
        await rabbitmq_service.register_hook(self.log_rabbit_message)

        await rabbitmq_service.start()

        # Initialize Broker if not in middleware mode
        if self.mode == Mode.MIDDLEWARE:
            return

        await Broker().initialize(
            MT5Broker,
            config=self.config,
            connection={
                'account': self.config.get_broker_account(),
                'password': self.config.get_broker_password(),
                'server': self.config.get_broker_server(),
                'path': self.config.get_broker_mt5_path()
            }
        )
        await Broker().startup()

    async def stop_services(self):
        """
        Stops all services and routines gracefully.
        """
        await asyncio.gather(*(routine.routine_stop() for routine in reversed(self.routines)))
        t_notif = await NotifierTickUpdates.get_instance(self.config)
        m_sate_notif = await NotifierMarketState.get_instance(self.config)
        rabbitmq_s = await RabbitMQService.get_instance()

        await t_notif.shutdown()
        await m_sate_notif.shutdown()
        await rabbitmq_s.stop()

        if self.mode != Mode.MIDDLEWARE:
            await Broker().shutdown()

        self.executor.shutdown()
        print("All services have been stopped.")

    async def run(self):
        """
        Runs the bot by starting all services and routines, and keeps it running until interrupted.
        """
        try:
            self.load_configuration()
            self.initialize_routines()
            self.setup_executor()
            await self.start_services()

            # Start all routines
            await asyncio.gather(*(routine.routine_start() for routine in self.routines))

            # Keeps the program running
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            print("Keyboard interruption detected. Stopping the bot...")
        except Exception as e:
            print(f"Exception occurred: {e} "
                  f"Stopping the bot... ")
            traceback.print_exc()
        finally:
            await self.stop_services()
            print("Program terminated.")


async def main():
    """
    Main function that parses arguments and starts the bot launcher.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Bot launcher script.')
    parser.add_argument(
        'config_file',
        nargs='?',
        default='config.json',
        help='Path to the configuration file.'
    )
    parser.add_argument(
        'start_silent',
        nargs='?',
        default='False',
        help='Start the bot in silent mode without sending bootstrap notifications'
    )
    args = parser.parse_args()

    config_file = args.config_file
    start_silent = args.start_silent.lower() in ('start_silent', 'silent')

    # Initialize and run the bot launcher
    bot_launcher = BotLauncher(config_file, start_silent)
    await bot_launcher.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
