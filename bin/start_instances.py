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
        format='[DEBUG] %(message)s',
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
    # Log the number of configuration files found for the specified type
    logging.debug(f"Found {len(config_files)} {config_type} configuration file(s).")
    # Launch instances for each configuration file and write PID files in the format "PID;type"
    for config_file in config_files:
        logging.debug(f"Preparing to start {config_type} instance with configuration: {config_file}")
        # Build the command to execute: use the Python from the virtual environment and the main.py script
        cmd = [r"..\venv\Scripts\python.exe", r"..\main.py", config_file]
        if silent_param:
            cmd.append(silent_param)
            logging.debug(f"Silent parameter added: {silent_param}")
        logging.debug(f"Command to run: {' '.join(cmd)}")
        try:
            # Open the log file in append mode to redirect output (stdout and stderr)
            with open(log_file, 'a') as logfile:
                process = subprocess.Popen(cmd, stdout=logfile, stderr=subprocess.STDOUT)
            pid = process.pid
            logging.debug(f"Started process with PID {pid} for configuration: {config_file}")
            # Ensure that the PID directory exists, create it if necessary
            if not os.path.exists(pid_dir):
                os.makedirs(pid_dir)
                logging.debug(f"Created PID directory: {pid_dir}")
            # Define the PID file name including the configuration type, configuration file name, and PID
            pid_file_name = os.path.join(pid_dir, f"{config_type}_{os.path.basename(config_file)}_{pid}.pid")
            with open(pid_file_name, 'w') as pid_file:
                pid_file.write(f"{pid};{config_type}")
            logging.debug(f"Wrote PID file: {pid_file_name}")
        except Exception as e:
            logging.error(f"Failed to start {config_type} instance for {config_file}: {e}")


def main():
    # Set up the main log file as startup.log
    log_file = "startup.log"
    setup_logging(log_file)

    logging.debug("Starting main function.")

    # Define directories for configuration, PID, logs, and output
    config_dir = "configs"
    pid_dir = "pid"
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
