# main.py
import argparse
import asyncio
import math
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor

from brokers.broker_interface import BrokerAPI
from brokers.mt5_broker import MT5Broker
from misc_utils.async_executor import executor
from misc_utils.bot_logger import BotLogger
from misc_utils.config import ConfigReader, TradingConfiguration
from misc_utils.enums import Mode
from misc_utils.error_handler import exception_handler
from routines.generator import GeneratorRoutine
from routines.sentinel import SentinelRoutine

warnings.filterwarnings('ignore', category=FutureWarning)


def calculate_workers(num_configs, max_workers=500):
    """
    Calculates the number of workers with a continuous and balanced growth:
    - On average 5 workers per configuration for few tasks.
    - On average 2.5 workers per configuration for many tasks.

    :param num_configs: Number of configurations.
    :param max_workers: Maximum number of allowed workers.
    :return: Calculated number of workers.
    """
    if num_configs <= 1:
        return 5  # Minimum 5 workers for 1 configuration

    # Continuous formula: scaled combination
    workers = num_configs * (5 - min(2.5, 2.5 * math.log(num_configs, 15)))
    return min(max_workers, max(num_configs, int(workers)))


@exception_handler
async def main(config: ConfigReader, trading_config: TradingConfiguration, broker_api: BrokerAPI):
    """
    Main function that starts the asynchronous trading bot.
    """
    topic = f"{trading_config.get_symbol()}_{trading_config.get_timeframe().name}_{trading_config.get_trading_direction().name}"
    mode: Mode = config.get_bot_mode()
    worker_id = f"{mode.name}_{config.get_bot_name()}_{topic}"
    worker_logger = BotLogger.get_logger(worker_id, level=config.get_bot_logging_level().upper())

    routines = []
    if mode == Mode.GENERATOR:
        routines.append(GeneratorRoutine(worker_id, config, trading_config, broker_api))
    elif mode == Mode.SENTINEL:
        routines.append(SentinelRoutine(worker_id, config, trading_config, broker_api))
    elif mode == Mode.STANDALONE:
        routines.append(SentinelRoutine(worker_id, config, trading_config, broker_api))
        routines.append(GeneratorRoutine(worker_id, config, trading_config, broker_api))
    else:
        raise ValueError("Invalid bot mode specified.")

    await broker_api.startup()

    for routine in routines:
        await routine.start()

    try:
        # Keep the program running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        worker_logger.info("Keyboard interruption detected. Stopping the bot...")
    finally:
        # Stop the routines
        for routine in reversed(routines):
            await routine.stop()

        await broker_api.shutdown()

        worker_logger.info("Program terminated.")
        executor.shutdown()

if __name__ == "__main__":
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')

    # Read command-line parameters
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Bot launcher script.')
    parser.add_argument('config_file', nargs='?', default='config.json', help='Path to the configuration file.')
    parser.add_argument('start_silent', nargs='?', default=False, help='Start the bot in silent mode without sending bootstrap notifications')

    # Parse the command-line arguments
    args = parser.parse_args()

    config_file_param = args.config_file
    start_silent = bool(args.start_silent)

    print(f"Config file: {config_file_param}")

    global_config = ConfigReader.load_config(config_file_param=config_file_param)

    global_config.register_param("start_silent", start_silent)

    if not global_config.get_enabled():
        print("Bot configuration not enabled. Exiting...")
        exit(0)

    trading_configs = global_config.get_trading_configurations()

    executor = ThreadPoolExecutor(max_workers=calculate_workers(len(trading_configs)))
    loop = asyncio.new_event_loop()
    loop.set_default_executor(executor)
    asyncio.set_event_loop(loop)

    # Initialize the broker
    broker: BrokerAPI = MT5Broker(bot_name=global_config.get_bot_name() + "_broker",
                                  account=global_config.get_broker_account(),
                                  password=global_config.get_broker_password(),
                                  server=global_config.get_broker_server(),
                                  path=global_config.get_broker_mt5_path())

    broker_startup_success = loop.run_until_complete(broker.startup())
    if not broker_startup_success:
        print("Broker connection failed. Exiting...")
        exit(1)


    async def run_all_tasks():
        tasks = [
            main(global_config, trading_config, broker) for trading_config in trading_configs
        ]
        await asyncio.gather(*tasks)


    # Run all tasks asynchronously
    try:
        loop.run_until_complete(run_all_tasks())
    finally:
        loop.close()
