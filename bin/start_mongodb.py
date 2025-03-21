import os
import subprocess
import logging

# Configure logging to output to a file named "startup.log"
logging.basicConfig(
    filename='startup.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define the path for the MongoDB data directory
data_dir = os.path.join('.', 'mongodb', 'data')

# Check if the directory exists; if not, create it
if not os.path.exists(data_dir):
    logging.info(f"Directory {data_dir} does not exist. Creating it...")
    os.makedirs(data_dir)
else:
    logging.info(f"Directory {data_dir} already exists.")

# Build the command to start MongoDB with the specified configuration
logging.info("Starting MongoDB...")
command = [os.path.join('.', 'mongodb', 'bin', 'mongod.exe'), '--config', 'mongod.cfg']

# Execute the command and wait for it to complete
try:
    subprocess.run(command, check=True)
    logging.info("MongoDB started successfully.")
except subprocess.CalledProcessError as e:
    logging.error("Error starting MongoDB: %s", e)

# Wait for user input before exiting (simulate 'pause')
input("Press Enter to exit...")
