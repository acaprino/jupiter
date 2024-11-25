import argparse
import asyncio
import math
import sys
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor

from misc_utils.config import ConfigReader
from misc_utils.enums import Mode
from routines.generator_routine import GeneratorRoutine
from routines.middleware_routine import MiddlewareService
from routines.sentinel_routine import SentinelRoutine
from services.singleton_rabbitmq_service import RabbitMQService

# Ignore FutureWarnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Configure the encoding for standard input and output
sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')


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


async def main():
    """
    Main function that starts the asynchronous trading bot.
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
    start_silent = args.start_silent.lower() in ('true', '1', 't', 'y', 'yes')

    print(f"Config file: {config_file}")

    # Load the configuration
    config = ConfigReader.load_config(config_file_param=config_file)
    config.register_param("start_silent", start_silent)

    if not config.get_enabled():
        print("Bot configuration not enabled. Exiting...")
        return

    trading_configs = config.get_trading_configurations()

    # Set up the logger
    mode = config.get_bot_mode()
    logger_name = f"{mode.name}_{config.get_bot_name()}"

    # Create routines based on the bot mode
    routines = []
    for trading_config in trading_configs:
        topic = f"{trading_config.get_symbol()}.{trading_config.get_timeframe().name}.{trading_config.get_trading_direction().name}"
        routine_label = f"{logger_name}_{topic}"

        if mode == Mode.GENERATOR:
            routines.append(GeneratorRoutine(routine_label, config, trading_config))
        elif mode == Mode.SENTINEL:
            routines.append(SentinelRoutine(routine_label, config, trading_config))
        elif mode == Mode.STANDALONE:
            routines.append(SentinelRoutine(routine_label, config, trading_config))
            routines.append(GeneratorRoutine(routine_label, config, trading_config))
        else:
            print("Invalid bot mode specified.")
            raise ValueError("Invalid bot mode specified.")

    if mode == Mode.MIDDLEWARE:
        routines.append(MiddlewareService(f"{config.get_bot_name()}_middleware", config))

    # Configure the ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=calculate_workers(len(routines)))
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)

    try:
        # Initialize the service (Singleton instance)
        RabbitMQService(config.get_bot_name(), config.get_rabbitmq_username(), config.get_rabbitmq_password(),
                        config.get_rabbitmq_host(), config.get_rabbitmq_port(), loop=loop)

        # Start the RabbitMQ service
        await RabbitMQService.start()
        # Start all routines
        await asyncio.gather(*(routine.routine_start() for routine in routines))
        # Keeps the program running
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("Keyboard interruption detected. Stopping the bot...")
    finally:
        # Stop the RabbitMQ service
        await RabbitMQService.stop()
        # Stop routines in reverse order
        await asyncio.gather(*(routine.routine_stop() for routine in reversed(routines)))
        print("Program terminated.")
        executor.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())  # Use asyncio.run to start the main coroutine
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
