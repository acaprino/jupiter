import json
import os
import sys
import glob
import shutil
import subprocess
import logging


def setup_logging(log_file):
    # Set up logging configuration to write debug messages to both the log file and the console
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.debug(f"Logging is set up. Log file: {log_file}")
    logging.debug("Starting startup script.")


def delete_directories(dirs):
    # Delete the specified directories if they exist
    for d in dirs:
        logging.debug(f"Checking if directory exists for deletion: {d}")
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                logging.debug(f"Deleted directory: {d}")
            except Exception as e:
                logging.error(f"Error deleting directory {d}: {e}")
        else:
            logging.debug(f"Directory not found, so not deleted: {d}")

def launch_instances(config_files, config_type, silent_param, pid_dir, log_file):
    """
    Launches application instances based on configuration files, wrapping them
    with the New Relic agent admin script.

    Args:
        config_files (list): A list of paths to configuration files.
        config_type (str): The type of instance (e.g., 'middleware', 'generator').
        silent_param (str): An optional parameter to pass to the script (e.g., 'start_silent').
        pid_dir (str): The directory to store PID files.
        log_file (str): The path to the main log file (used for logging within this function).
    """
    logging.debug(f"Found {len(config_files)} {config_type} configuration file(s).")
    for config_file in config_files:
        logging.debug(f"Preparing to start {config_type} instance with configuration: {config_file}")

        python_exe = os.path.abspath(r"..\venv\Scripts\python.exe")
        main_script = os.path.abspath(r"..\main.py")
        config_file_abs = os.path.abspath(config_file)

        app_command_list = [python_exe, main_script, config_file_abs]
        if silent_param:
            app_command_list.append(silent_param)
            logging.debug(f"Silent parameter added: {silent_param}")

        logging.debug(f"Base application command list: {app_command_list}")

        new_relic_config_path = os.path.abspath("newrelic.ini")
        new_relic_admin_cmd = "newrelic-admin"

        if not os.path.exists(new_relic_config_path):
            logging.error(f"New Relic config file not found at: {new_relic_config_path}. Skipping instance {config_file}.")
            continue

        current_env = os.environ.copy()
        current_env["NEW_RELIC_CONFIG_FILE"] = new_relic_config_path
        logging.debug(f"Setting environment variable NEW_RELIC_CONFIG_FILE={new_relic_config_path}")

        new_relic_wrapped_cmd_list = [new_relic_admin_cmd, "run-program"] + app_command_list
        logging.debug("Final command list (with New Relic wrapper): " + subprocess.list2cmdline(new_relic_wrapped_cmd_list))

        try:
            process = subprocess.Popen(
                new_relic_wrapped_cmd_list,
                env=current_env,
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            pid = process.pid
            logging.info(f"Started {config_type} process via New Relic with PID {pid} for configuration: {config_file}") # Changed to info level

            if not os.path.exists(pid_dir):
                try:
                    os.makedirs(pid_dir)
                    logging.debug(f"Created PID directory: {pid_dir}")
                except OSError as e:
                    logging.error(f"Failed to create PID directory {pid_dir}: {e}")
                    continue

            pid_file_name = os.path.join(pid_dir, f"{os.path.basename(config_file)}.pid")
            try:
                with open(pid_file_name, 'w') as pid_file:
                    pid_file.write(f"{pid};{config_type}")
                logging.debug(f"Wrote PID file: {pid_file_name}")
            except IOError as e:
                 logging.error(f"Failed to write PID file {pid_file_name}: {e}")

        except FileNotFoundError:
            logging.error(f"Failed to start {config_type} instance for {config_file}. "
                          f"Error: '{new_relic_admin_cmd}' command not found. Is it in the system PATH or virtual environment?")
        except Exception as e:
            logging.error(f"Failed to start {config_type} instance for {config_file} via New Relic: {e}", exc_info=True) # Add exc_info for traceback

def main():
    # Set up the main log file as startup.log
    log_file = "startup.log"
    setup_logging(log_file)

    logging.debug("Starting main function.")

    # Define directories for configuration, PID, logs, and output
    config_dir = "configs"
    pid_dir = "pid_instances"
    logs_dir = "logs"
    output_dir = "output"
    logging.debug(f"Configuration directory: {config_dir}")
    logging.debug(f"PID directory: {pid_dir}")
    logging.debug(f"Logs directory: {logs_dir}")
    logging.debug(f"Output directory: {output_dir}")

    # Check command line arguments for the --delete-logs parameter
    delete_logs_param = "--delete-logs" in sys.argv[1:]
    logging.debug(f"Command line arguments: {sys.argv[1:]}")
    if delete_logs_param:
        logging.debug("--delete-logs parameter detected: proceeding with deletion")
        delete_directories([logs_dir, output_dir])
    else:
        logging.debug("--delete-logs parameter not provided")

    # Check for the existence of silent.sem to set an extra parameter
    if os.path.exists("silent.sem"):
        silent_param = "start_silent"
        logging.debug("silent.sem found: adding parameter start_silent")
    else:
        silent_param = ""
        logging.debug("silent.sem not found: no extra parameter")

    # Retrieve all configuration files from the configs directory
    config_files_all = glob.glob(os.path.join(config_dir, "*"))
    logging.debug(f"Total configuration files found in {config_dir}: {len(config_files_all)}")
    if not config_files_all:
        logging.debug("No configuration files found in configs directory. Exiting main.")
        return

    # Separate configuration files by type: middleware, generator, executor
    middleware_configs = []
    generator_configs = []
    executor_configs = []

    for config_file in config_files_all:
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
        except Exception as e:
            print(f"Errore nel caricare {config_file}: {e}")
            continue

        # Leggi il valore del campo "mode" e convertilo in minuscolo
        mode = config_data.get("mode", "").lower()

        if mode == "middleware":
            middleware_configs.append(config_file)
        elif mode == "generator":
            generator_configs.append(config_file)
        elif mode == "sentinel":
            executor_configs.append(config_file)

    logging.debug(f"Middleware configs found: {len(middleware_configs)}")
    logging.debug(f"Generator configs found: {len(generator_configs)}")
    logging.debug(f"Executor configs found: {len(executor_configs)}")

    if not middleware_configs:
        logging.debug("No middleware configuration files found.")
    if not generator_configs:
        logging.debug("No generator configuration files found.")
    if not executor_configs:
        logging.debug("No executor configuration files found.")

    # Launch instances in the required order: middleware, then generator, then executor
    if middleware_configs:
        logging.debug("Starting middleware instances...")
        launch_instances(middleware_configs, "middleware", silent_param, pid_dir, log_file)
    if generator_configs:
        logging.debug("Starting generator instances...")
        launch_instances(generator_configs, "generator", silent_param, pid_dir, log_file)
    if executor_configs:
        logging.debug("Starting executor instances...")
        launch_instances(executor_configs, "executor", silent_param, pid_dir, log_file)

    logging.debug("All instances processed. Exiting main.")


if __name__ == "__main__":
    main()
