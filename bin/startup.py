import subprocess
import time
import os
import logging
import sys

# Configura il logging in inglese, scrivendo su startup.log
logging.basicConfig(
    filename="startup.log",
    filemode="a",  # Modalità append per preservare i log precedenti
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def launch_python_script(file_path):
    try:
        # Ottieni la directory in cui si trova lo script
        dir_path = os.path.dirname(file_path)
        # Lancia il processo in una nuova finestra di console
        process = subprocess.Popen(
            [sys.executable, file_path],
            cwd=dir_path,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        logging.info(f"Started python script: {file_path}")
        return process
    except Exception as e:
        logging.exception(f"Error starting python script: {file_path} - {e}")
        return None

def main():
    # Percorsi per gli script Python
    mongodb_start = r".\mongodb.py"
    jupiter_start_all = r".\start_instances.py"
    jupiter_health_check = r".\health_check.py"

    try:
        logging.info("Starting MongoDB...")
        process1 = launch_python_script(mongodb_start)
    except Exception as e:
        logging.exception(f"Failed to start MongoDB: {e}")

    # Attendi 5 secondi
    time.sleep(5)

    try:
        logging.info("Starting Jupiter start_all...")
        process2 = launch_python_script(jupiter_start_all)
    except Exception as e:
        logging.exception(f"Failed to start Jupiter start_all: {e}")

    # Attendi altri 5 secondi
    time.sleep(5)

    try:
        logging.info("Starting Jupiter health_check...")
        process3 = launch_python_script(jupiter_health_check)
    except Exception as e:
        logging.exception(f"Failed to start Jupiter health_check: {e}")

    logging.info("All processes have been started.")

if __name__ == "__main__":
    main()
