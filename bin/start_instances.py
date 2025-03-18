import os
import sys
import glob
import shutil
import subprocess
import logging

def setup_logging(log_file):
    # Set up logging configuration to write to both a file and the console
    logging.basicConfig(
        level=logging.DEBUG,
        format='[DEBUG] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.debug("Starting start instances script")

def delete_directories(dirs):
    # Delete the specified directories if they exist
    for d in dirs:
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                logging.debug(f"Deleted directory: {d}")
            except Exception as e:
                logging.error(f"Error deleting directory {d}: {e}")

def launch_instances(config_files, config_type, silent_param, pid_dir):
    # Launch instances for each configuration file and write PID files in the format "PID;type"
    for config_file in config_files:
        logging.debug(f"Starting {config_type} instance with configuration: {config_file}")
        # Build the command to execute; replace "run_instance.py" with the actual script/program if needed
        cmd = ["python", "run_instance.py", config_file]
        if silent_param:
            cmd.append(silent_param)
        try:
            process = subprocess.Popen(cmd)
            pid = process.pid
            logging.debug(f"Started process with PID {pid} for configuration {config_file}")
            # Ensure the PID directory exists
            if not os.path.exists(pid_dir):
                os.makedirs(pid_dir)
            # Define the PID file name that includes the configuration type, configuration filename, and PID
            pid_file_name = os.path.join(pid_dir, f"{config_type}_{os.path.basename(config_file)}_{pid}.pid")
            with open(pid_file_name, 'w') as pid_file:
                pid_file.write(f"{pid};{config_type}")
            logging.debug(f"Wrote PID file: {pid_file_name}")
        except Exception as e:
            logging.error(f"Failed to start {config_type} instance for {config_file}: {e}")

def main():
    # Set up the main log file to startup.log
    log_file = "startup.log"
    setup_logging(log_file)

    # Define directories for configuration, PID, logs, and output
    config_dir = "configs"
    pid_dir = "pid"
    logs_dir = "logs"
    output_dir = "output"

    # Check for the --delete-logs parameter in the command line arguments
    delete_logs_param = "--delete-logs" in sys.argv[1:]
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
    if not config_files_all:
        logging.debug("No configuration files found in configs directory.")
        return

    # Separate configuration files by type: middleware, generator, executor
    middleware_configs = [f for f in config_files_all if "middleware" in os.path.basename(f).lower()]
    generator_configs  = [f for f in config_files_all if "generator" in os.path.basename(f).lower()]
    executor_configs   = [f for f in config_files_all if "executor" in os.path.basename(f).lower()]

    if not middleware_configs:
        logging.debug("No middleware configuration files found.")
    if not generator_configs:
        logging.debug("No generator configuration files found.")
    if not executor_configs:
        logging.debug("No executor configuration files found.")

    # Launch instances in the required order: middleware, then generator, then executor
    if middleware_configs:
        logging.debug("Starting middleware instances...")
        launch_instances(middleware_configs, "middleware", silent_param, pid_dir)
    if generator_configs:
        logging.debug("Starting generator instances...")
        launch_instances(generator_configs, "generator", silent_param, pid_dir)
    if executor_configs:
        logging.debug("Starting executor instances...")
        launch_instances(executor_configs, "executor", silent_param, pid_dir)

if __name__ == "__main__":
    main()
