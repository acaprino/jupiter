import os
import sys
import glob
import logging
import subprocess
import signal

def setup_logging(log_file):
    # Set up the logging configuration
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.debug("Starting kill instances script")

def kill_process(pid):
    try:
        pid = int(pid)
    except ValueError:
        logging.error(f"Invalid PID: {pid}")
        return

    if os.name == 'nt':
        # On Windows, use taskkill command to forcefully terminate the process
        try:
            result = subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True)
            if result.returncode == 0:
                logging.debug(f"Successfully killed process {pid}")
            else:
                logging.error(f"Failed to kill process {pid}: {result.stderr.strip()}")
        except Exception as e:
            logging.error(f"Exception while killing process {pid}: {e}")
    else:
        # On Unix-like systems, send SIGTERM
        try:
            os.kill(pid, signal.SIGTERM)
            logging.debug(f"Sent SIGTERM to process {pid}")
        except ProcessLookupError:
            logging.error(f"Process {pid} not found")
        except Exception as e:
            logging.error(f"Error killing process {pid}: {e}")

def kill_instances():
    pid_dir = "pid"
    if not os.path.exists(pid_dir):
        logging.debug("PID directory does not exist. No instances to kill.")
        return

    # Retrieve all PID files from the PID directory
    pid_files = glob.glob(os.path.join(pid_dir, "*.pid"))
    if not pid_files:
        logging.debug("No PID files found in PID directory.")
        return

    # Dictionary to group processes by type (extracted from the file content)
    processes_by_type = {}

    # Expected file content format: "PID;type"
    for pid_file in pid_files:
        try:
            with open(pid_file, 'r') as f:
                content = f.read().strip()
            parts = content.split(";")
            if len(parts) != 2:
                logging.error(f"PID file {pid_file} does not have expected format 'PID;type'. Content: {content}")
                continue
            pid_str, proc_type = parts[0].strip(), parts[1].strip().lower()
            if not pid_str.isdigit():
                logging.error(f"Invalid PID '{pid_str}' in file {pid_file}")
                continue
            # Group processes by type
            if proc_type not in processes_by_type:
                processes_by_type[proc_type] = []
            processes_by_type[proc_type].append((pid_str, pid_file))
        except Exception as e:
            logging.error(f"Error reading PID file {pid_file}: {e}")

    # Define kill order: executor first, then generator, then middleware
    kill_order = ["executor", "generator", "middleware"]

    for proc_type in kill_order:
        if proc_type in processes_by_type:
            logging.debug(f"Killing {proc_type} instances...")
            for pid_str, pid_file in processes_by_type[proc_type]:
                logging.debug(f"Attempting to kill process from file {pid_file} with PID {pid_str}")
                kill_process(pid_str)
                try:
                    os.remove(pid_file)
                    logging.debug(f"Removed PID file: {pid_file}")
                except Exception as e:
                    logging.error(f"Error removing PID file {pid_file}: {e}")

def main():
    # Set up the overall log file
    log_file = "startup.log"
    setup_logging(log_file)
    kill_instances()

if __name__ == "__main__":
    main()
