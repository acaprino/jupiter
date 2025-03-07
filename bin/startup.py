import subprocess
import time
import os
import logging

# Configure logging to write to startup.log in English
logging.basicConfig(
    filename="startup.log",
    filemode="a",  # Append mode to preserve previous logs
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def launch_batch_file(file_path):
    try:
        # Get the directory where the batch file is located
        dir_path = os.path.dirname(file_path)
        # Launch the process with the specified working directory
        process = subprocess.Popen(file_path, cwd=dir_path, shell=True)
        logging.info(f"Started batch file: {file_path}")
        return process
    except Exception as e:
        logging.exception(f"Error starting batch file: {file_path} - {e}")
        return None

def main():
    # Paths for the batch files
    mongodb_start = r".\mongodb.bat"
    jupiter_start_all = r".\start_instances.bat"
    jupiter_health_check = r".\health_check.bat"

    try:
        logging.info("Starting MongoDB...")
        process1 = launch_batch_file(mongodb_start)
    except Exception as e:
        logging.exception(f"Failed to start MongoDB: {e}")

    # Wait for 5 seconds
    time.sleep(5)

    try:
        logging.info("Starting Jupiter start_all...")
        process2 = launch_batch_file(jupiter_start_all)
    except Exception as e:
        logging.exception(f"Failed to start Jupiter start_all: {e}")

    # Wait for 5 seconds
    time.sleep(5)

    try:
        logging.info("Starting Jupiter health_check...")
        process3 = launch_batch_file(jupiter_health_check)
    except Exception as e:
        logging.exception(f"Failed to start Jupiter health_check: {e}")

    logging.info("All processes have been started.")

if __name__ == "__main__":
    main()
