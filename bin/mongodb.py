import os
import subprocess
import logging


def setup_logging(log_file):
    # Set up logging configuration to write messages to both startup.log and the console
    logging.basicConfig(
        level=logging.DEBUG,
        format='[DEBUG] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )
    logging.debug("Starting MongoDB startup script")


def main():
    log_file = "startup.log"
    setup_logging(log_file)

    # Check if the directory "./mongodb/data" exists; if not, create it.
    data_dir = os.path.join("mongodb", "data")
    if not os.path.exists(data_dir):
        logging.debug(f"Directory {data_dir} does not exist, creating it...")
        os.makedirs(data_dir)

    # Start MongoDB with the specified configuration file.
    logging.debug("Starting MongoDB...")
    mongo_executable = os.path.join("mongodb", "bin", "mongod.exe")
    config_file = "mongod.cfg"

    try:
        # Execute the MongoDB process with the configuration.
        # This call will block until the process terminates.
        subprocess.run([mongo_executable, "--config", config_file], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to start MongoDB: {e}")

    # Wait for user input before exiting (similar to the pause command in batch).
    input("Press Enter to exit...")


if __name__ == "__main__":
    main()
