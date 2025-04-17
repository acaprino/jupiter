import argparse
import asyncio
import os
import signal
import sys
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import psutil

from agents.agent_market_state_notifier import MarketStateNotifierAgent
from agents.agent_registration_aware import RegistrationAwareAgent
from agents.agent_strategy_adrastea import AdrasteaSignalGeneratorAgent
from agents.generator_event_manager import EconomicEventsEmitterAgent
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


def calculate_workers(num_configs, config: ConfigReader, default_max_workers=500, default_mem_per_worker=0.02):
    """
    Calculates the optimal number of worker threads based on system resources,
    the number of configurations, and configuration settings.

    Args:
        num_configs (int): The number of configurations/routines being run.
        config (ConfigReader): The application configuration object.
        default_max_workers (int): Default maximum workers if not overridden.
        default_mem_per_worker (float): Default estimated memory per worker (GB) if not in config.

    Returns:
        int: The calculated number of workers.
    """
    # --- Read performance settings from config, using defaults if not present ---
    perf_config = config.config.get("performance", {})  # Get the 'performance' section safely
    estimated_mem_per_worker_gb = perf_config.get("estimated_memory_per_worker_gb", default_mem_per_worker)
    max_workers = perf_config.get("max_workers_override", default_max_workers)

    print(f"Using config: Estimated Mem/Worker: {estimated_mem_per_worker_gb} GB, Max Workers: {max_workers}")  # Log effective settings

    # --- Heuristic based on number of configs ---
    heuristic_workers = num_configs * 10  # Base calculation remains

    # --- System Memory Constraint ---
    mem = psutil.virtual_memory()
    total_memory_gb = mem.total / (1024 ** 3)
    reserved_memory_percentage = 0.25
    usable_memory_gb = total_memory_gb * (1 - reserved_memory_percentage)
    memory_limit = int(usable_memory_gb / estimated_mem_per_worker_gb) if estimated_mem_per_worker_gb > 0 else max_workers
    print(f"System Memory: Total={total_memory_gb:.2f} GB, Usable={usable_memory_gb:.2f} GB -> Memory Limit Workers={memory_limit}")

    # --- CPU Core Consideration (Optional Constraint) ---
    cpu_cores = os.cpu_count() or 1  # Get number of logical cores, default to 1 if unknown
    # Example: Cap workers at 4 times the number of cores as a soft limit before memory/max caps
    cpu_based_limit = cpu_cores * 4
    print(f"System CPU Cores: {cpu_cores} -> CPU-Based Worker Limit (e.g., x4)={cpu_based_limit}")

    # --- Apply Constraints ---
    # Start with the heuristic, then apply caps
    workers = min(heuristic_workers, memory_limit, cpu_based_limit, max_workers)

    # Ensure at least one worker
    workers = max(1, workers)

    print(f"Calculated Workers: Heuristic={heuristic_workers}, Final={workers} (Applied Limits: Memory={memory_limit}, CPU={cpu_based_limit}, Max={max_workers})")
    return workers


class BotLauncher:
    """
    A class to encapsulate the bot launching process, handling different modes and their specific routines.
    """

    def __init__(self, config_file: str, start_silent: bool):
        """
        Initializes the BotLauncher instance.

        Args:
            config_file (str): The path to the configuration file for this bot instance.
            start_silent (bool): A flag indicating whether the bot should start silently,
                                 suppressing initial bootstrap notifications.
        """
        self.agent: str = "BotLauncher"  # Identifier name for logging
        self.config_file: str = config_file  # Path to the configuration file
        self.start_silent: bool = start_silent  # Flag for silent startup

        # State variables that will be populated later
        self.config = None  # Instance of ConfigReader after loading
        self.logger = None  # Logger instance specific to this bot
        self.mode = None  # Operational mode (MIDDLEWARE, GENERATOR, SENTINEL)

        # Lists to separate the types of agents/routines (as discussed)
        # They are populated in the initialize_routines method
        self.registration_aware_agents: List = []  # Agents that require registration ACK
        self.other_agents: List = []  # Middleware, Notifiers, etc.

        # Execution management and event loop
        self.executor: Optional[ThreadPoolExecutor] = None  # Executor for blocking tasks
        self.loop = asyncio.get_event_loop()  # Get the current event loop

        self.shutdown_event = asyncio.Event()

        # Initial log/debug message (optional)
        print(f"BotLauncher initialized. Config file: '{self.config_file}', Silent start: {self.start_silent}")

    def load_configuration(self):
        """
        Loads the bot configuration from the specified file.
        """
        print(f"Loading configuration from: {self.config_file}")
        self.config = ConfigReader.load_config(config_file_param=self.config_file)

        # Register additional parameters
        self.config.register_param("start_silent", self.start_silent)

        # Check if the bot configuration is enabled
        if not self.config.get_enabled():
            print("Bot configuration not enabled. Exiting...")
            sys.exit()

        # Check for duplicate trading configurations
        self.check_duplicate_trading_configurations(self.config)

        self.mode = self.config.get_bot_mode()
        self.logger = BotLogger.get_logger(name=self.config.get_bot_name(), level=self.config.get_bot_logging_level())

    def initialize_routines(self):
        """
        Initializes routines based on the bot mode and trading configurations,
        separating registration-aware agents from others.
        """
        try:
            print(f"Initializing routines for mode: {self.mode}")

            self.registration_aware_agents: List[RegistrationAwareAgent] = []
            self.other_agents: List = []  # For Middleware, Notifiers, etc.

            if self.mode == Mode.MIDDLEWARE:
                # Middleware doesn't register in the same way; treat as 'other'
                self.other_agents.append(MiddlewareService(self.config))
            else:
                trading_configs = self.config.get_trading_configurations()
                for tc in trading_configs:
                    if self.mode == Mode.SENTINEL:
                        agent_instance = ExecutorAgent(self.config, tc)
                    elif self.mode == Mode.GENERATOR:
                        agent_instance = AdrasteaSignalGeneratorAgent(self.config, tc)
                    else:
                        raise ValueError(f"Invalid bot mode specified: {self.mode}")

                    if agent_instance:
                        self.registration_aware_agents.append(agent_instance)

                # Notifiers are started after main agents are registered
                self.other_agents.append(MarketStateNotifierAgent(self.config, trading_configs))
                if self.mode == Mode.SENTINEL:
                    self.other_agents.append(EconomicEventsManagerAgent(self.config, trading_configs))
                    self.other_agents.append(ClosedDealsAgent(self.config, trading_configs))
                    self.other_agents.append(FilledOrdersAgent(self.config, trading_configs))
                if self.mode == Mode.GENERATOR:
                    self.other_agents.append(EconomicEventsEmitterAgent(self.config, trading_configs))

            print(f"Found {len(self.registration_aware_agents)} registration-aware agents.")
            print(f"Found {len(self.other_agents)} other agents.")
        except Exception as e2:
            print(f"Error initializing routines: {e2}")

    def check_duplicate_trading_configurations(self, config: ConfigReader) -> None:
        """
        Checks that there are no duplicate trading configurations based on
        the combination of timeframe, symbol, and trading_direction.

        Parameters:
            config (List[TradingConfiguration]): A list of trading configurations.

        Raises:
            ValueError: If a duplicate configuration is found.
        """
        seen_keys = {}
        for tc in config.get_trading_configurations():
            # Construct the key using timeframe, symbol, and trading_direction.
            # Use the .name attribute of the enums to obtain a string representation.
            key = (tc.get_timeframe().name, tc.get_symbol(), tc.get_trading_direction().name)
            if key in seen_keys:
                raise ValueError(f"Duplicate configuration found for (timeframe, symbol, trading_direction): {key}")
            seen_keys[key] = config

    def setup_executor(self):
        """
        Configures the ThreadPoolExecutor based on the number of routines and system resources.
        """
        # Determine the total number of routines to estimate workload
        # Use the separated lists if available from initialize_routines
        total_routines = len(getattr(self, 'registration_aware_agents', [])) + \
                         len(getattr(self, 'other_agents', []))

        if self.mode == Mode.MIDDLEWARE:
            # Middleware might have different resource needs
            max_workers = 50  # Fixed or calculated differently for middleware
            print("Setting up executor for MIDDLEWARE mode.")  # Use print or self.logger if available
        else:
            # Calculate workers based on the number of agent routines
            print(f"Calculating workers for {total_routines} routines.")  # Use print or self.logger if available
            max_workers = calculate_workers(total_routines, self.config)

        # Create the ThreadPoolExecutor
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Set this executor as the default for the asyncio event loop
        self.loop.set_default_executor(self.executor)

        # Log the outcome
        log_msg = f"Initialized executor with {max_workers} workers."
        if self.logger:
            self.logger.info(log_msg, agent=self.agent)
        else:
            print(log_msg)  # Fallback to print if logger not ready

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
        #        port = int(self.config.get_rabbitmq_port().strip()) if self.config and self.config.get_rabbitmq_port().strip() != "" else None
        #        vhost = self.config.get_rabbitmq_vhost().strip() if self.config and self.config.get_rabbitmq_vhost().strip() != "" else None

        rabbitmq_service = await RabbitMQService.get_instance(
            config=self.config,
            user=self.config.get_rabbitmq_username(),
            password=self.config.get_rabbitmq_password(),
            rabbitmq_host=self.config.get_rabbitmq_host(),
            port=self.config.get_rabbitmq_port(),
            vhost=self.config.get_rabbitmq_vhost(),
            ssl=self.config.get_rabbitmq_is_ssl(),
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
        Stops all services and routines gracefully in reverse order of dependency.
        """
        log_prefix = "[Shutdown]"  # Prefix for clarity
        print(f"{log_prefix} Starting service shutdown sequence...")
        if self.logger: self.logger.info("Starting service shutdown sequence...", agent=self.agent)

        # 1. Stop all agent routines (both types)
        # Combine the lists for stopping
        all_agents = getattr(self, 'registration_aware_agents', []) + getattr(self, 'other_agents', [])
        if all_agents:
            print(f"{log_prefix} Stopping {len(all_agents)} agent routines...")
            if self.logger: self.logger.info(f"Stopping {len(all_agents)} agent routines...", agent=self.agent)
            # Stop routines, potentially in reverse order if dependencies exist
            # Using gather waits for all stop routines to complete
            await asyncio.gather(*(agent.routine_stop() for agent in reversed(all_agents)), return_exceptions=True)
            print(f"{log_prefix} All agent routines stopped.")
            if self.logger: self.logger.info("All agent routines stopped.", agent=self.agent)
        else:
            print(f"{log_prefix} No agent routines to stop.")
            if self.logger: self.logger.info("No agent routines to stop.", agent=self.agent)

        # 2. Stop specific notifier instances (if they need explicit shutdown)
        # Use get_instance safely, it might return None if never initialized
        try:
            print(f"{log_prefix} Attempting to get NotifierTickUpdates instance...")
            t_notif = await NotifierTickUpdates.get_instance(self.config)  # Pass config if needed
            if t_notif:
                print(f"{log_prefix} Shutting down NotifierTickUpdates...")
                if self.logger: self.logger.info("Shutting down NotifierTickUpdates...", agent=self.agent)
                await t_notif.shutdown()
                print(f"{log_prefix} NotifierTickUpdates shut down.")
                if self.logger: self.logger.info("NotifierTickUpdates shut down.", agent=self.agent)
        except Exception as e2:
            print(f"{log_prefix} Error shutting down NotifierTickUpdates: {e2}")
            if self.logger: self.logger.error(f"Error shutting down NotifierTickUpdates: {e2}", agent=self.agent, exec_info=e2)

        try:
            print(f"{log_prefix} Attempting to get NotifierMarketState instance...")
            m_state_notif = await NotifierMarketState.get_instance(self.config)  # Pass config if needed
            if m_state_notif:
                print(f"{log_prefix} Shutting down NotifierMarketState...")
                if self.logger: self.logger.info("Shutting down NotifierMarketState...", agent=self.agent)
                await m_state_notif.shutdown()
                print(f"{log_prefix} NotifierMarketState shut down.")
                if self.logger: self.logger.info("NotifierMarketState shut down.", agent=self.agent)
        except Exception as e3:
            print(f"{log_prefix} Error shutting down NotifierMarketState: {e3}")
            if self.logger: self.logger.error(f"Error shutting down NotifierMarketState: {e3}", agent=self.agent, exec_info=e3)

        # 3. Stop RabbitMQ service
        try:
            print(f"{log_prefix} Attempting to get RabbitMQService instance...")
            rabbitmq_s = await RabbitMQService.get_instance()  # No config needed if already initialized
            if rabbitmq_s:
                print(f"{log_prefix} Stopping RabbitMQ service...")
                if self.logger: self.logger.info("Stopping RabbitMQ service...", agent=self.agent)
                await rabbitmq_s.stop()
                print(f"{log_prefix} RabbitMQ service stopped.")
                if self.logger: self.logger.info("RabbitMQ service stopped.", agent=self.agent)
        except Exception as e4:
            print(f"{log_prefix} Error stopping RabbitMQ service: {e4}")
            if self.logger: self.logger.error(f"Error stopping RabbitMQ service: {e4}", agent=self.agent, exec_info=e4)

        # 4. Stop Broker (only if not in MIDDLEWARE mode)
        if self.mode != Mode.MIDDLEWARE:
            try:
                # Check if Broker was initialized before trying to shut down
                broker_instance = Broker()  # Get singleton
                if broker_instance.is_initialized:
                    print(f"{log_prefix} Shutting down Broker...")
                    if self.logger: self.logger.info("Shutting down Broker...", agent=self.agent)
                    await broker_instance.shutdown()
                    print(f"{log_prefix} Broker shut down.")
                    if self.logger: self.logger.info("Broker shut down.", agent=self.agent)
                else:
                    print(f"{log_prefix} Broker was not initialized, skipping shutdown.")
                    if self.logger: self.logger.info("Broker was not initialized, skipping shutdown.", agent=self.agent)
            except Exception as e4:
                print(f"{log_prefix} Error shutting down Broker: {e4}")
                if self.logger: self.logger.error(f"Error shutting down Broker: {e4}", agent=self.agent, exec_info=e4)
        else:
            print(f"{log_prefix} Skipping Broker shutdown in MIDDLEWARE mode.")
            if self.logger: self.logger.info("Skipping Broker shutdown in MIDDLEWARE mode.", agent=self.agent)

        # 5. Shutdown the ThreadPoolExecutor
        if self.executor:
            print(f"{log_prefix} Shutting down ThreadPoolExecutor...")
            if self.logger: self.logger.info("Shutting down ThreadPoolExecutor...", agent=self.agent)
            # Use wait=True to ensure threads finish gracefully if possible
            self.executor.shutdown(wait=True)
            print(f"{log_prefix} ThreadPoolExecutor shut down.")
            if self.logger: self.logger.info("ThreadPoolExecutor shut down.", agent=self.agent)
        else:
            print(f"{log_prefix} Executor not initialized, skipping shutdown.")
            if self.logger: self.logger.info("Executor not initialized, skipping shutdown.", agent=self.agent)

        print(f"{log_prefix} All services have been processed for shutdown.")
        if self.logger: self.logger.info("All services have been processed for shutdown.", agent=self.agent)

    def _register_signal_handlers(self):
        if sys.platform == "win32":
            # add_signal_handler is not supported on Windows.
            # Rely on KeyboardInterrupt for Ctrl+C.
            # SIGTERM equivalent usually requires other mechanisms (e.g., service control).
            print("Skipping signal handler registration on Windows.")
            return

        # On non-Windows platforms, register the handlers
        print("Registering signal handlers for SIGINT and SIGTERM.")
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                # Use call_soon_threadsafe if the handler might need to interact
                # with the loop from a different thread context (safer approach)
                self.loop.add_signal_handler(
                    sig, lambda s=sig: self.loop.call_soon_threadsafe(self._signal_callback, s)
                )
                # Or if the callback is simple and only sets an asyncio Event:
                # self.loop.add_signal_handler(sig, self.shutdown_event.set)
            except (ValueError, OSError, NotImplementedError) as e:
                print(f"Warning: Could not register signal handler for {sig}: {e}")

    def _signal_callback(self, sig):
        print(f"Received signal {sig}, setting shutdown event.")
        self.shutdown_event.set()

    async def run(self):
        """
        Runs the bot by starting all services and routines in the correct order,
        ensuring registration-aware agents complete registration before others start fully.
        """
        try:
            self._register_signal_handlers()

            self.load_configuration()
            self.initialize_routines()  # Separates agents
            self.setup_executor()
            await self.start_services()  # Start RabbitMQ, Broker

            # --- Start and Wait for Registration-Aware Agents ---
            if self.registration_aware_agents:
                print("Starting registration-aware agents and waiting for registration completion...")
                registration_tasks = [
                    asyncio.create_task(agent.routine_start())
                    for agent in self.registration_aware_agents
                ]
                # Wait for all routine_start() calls to complete.
                # Since routine_start waits for the ACK, this ensures registration is done.
                await asyncio.gather(*registration_tasks)
                print("All registration-aware agents have completed registration.")
            else:
                print("No registration-aware agents to start.")

            # --- Start Other Agents ---
            if self.other_agents:
                print("Starting other agents (Middleware, Notifiers, etc.)...")
                other_tasks = [
                    asyncio.create_task(agent.routine_start())
                    for agent in self.other_agents
                ]
                # We don't necessarily need to wait for these to fully "start" unless they
                # also have critical async setup. We primarily needed to wait for the *registration* part.
                # If their routine_start is quick, gather isn't strictly needed before the final wait.
                # However, using gather ensures they are launched.
                await asyncio.gather(*other_tasks)  # Launch them
                print("Other agents have been started.")
            else:
                print("No other agents to start.")

            # Keeps the program running indefinitely
            print("Bot started. Awaiting shutdown signal...")
            await self.shutdown_event.wait()

        except KeyboardInterrupt:
            print("\nKeyboardInterrupt detected. Initiating shutdown...")
            if hasattr(self, 'shutdown_event') and self.shutdown_event:
                self.shutdown_event.set()
        except asyncio.CancelledError:
            print("\nCancelledError detected. Initiating shutdown...")
            if hasattr(self, 'shutdown_event') and self.shutdown_event:
                self.shutdown_event.set()
        except Exception as e2:
            print(f"Exception occurred during bot execution: {e2}. Stopping the bot...")
            traceback.print_exc()
            if hasattr(self, 'shutdown_event') and self.shutdown_event:
                self.shutdown_event.set()
        finally:
            print("Shutdown signal received, stopping services...")
            await self.stop_services()
            print("Shutdown complete.")


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
