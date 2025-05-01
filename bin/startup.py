import subprocess
import time
import os
import logging
import sys

# Imposta la variabile d'ambiente NEW_RELIC_CONFIG_FILE
script_directory = os.path.dirname(os.path.abspath(__file__))
new_relic_config_file_path = os.path.join(script_directory, 'newrelic.ini')
os.environ['NEW_RELIC_CONFIG_FILE'] = new_relic_config_file_path

# Configura il logging in inglese, scrivendo su startup.log
logging.basicConfig(
    filename="startup.log",
    filemode="a",  # Modalità append per preservare i log precedenti
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def launch_health_check():
    bot_health_check = r".\health_check.py"
    try:
        dir_path = os.path.dirname(bot_health_check)
        process = subprocess.Popen(
            [sys.executable, bot_health_check],
            cwd=dir_path,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        logging.info(f"Started python script: {bot_health_check}")
        with open("health_check.pid", "w") as pid_file:
            pid_file.write(str(process.pid))
        return process
    except Exception as e:
        logging.exception("Error starting MongoDB: {}".format(e))
        return None


def launch_bot_instances():
    jupiter_start_all = r".\start_instances.py"
    try:
        dir_path = os.path.dirname(jupiter_start_all)
        process = subprocess.Popen(
            [sys.executable, jupiter_start_all],
            cwd=dir_path,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        logging.info(f"Started python script: {jupiter_start_all}")
        return process
    except Exception as e:
        logging.exception(f"Error starting python script: {jupiter_start_all} - {e}")
        return None


def main():
    try:
        logging.info("Starting bot instances...")
        process2 = launch_bot_instances()
    except Exception as e:
        logging.exception(f"Failed to start Jupiter start_all: {e}")

    time.sleep(5)

    try:
        logging.info("Starting bot health_check service...")
        process3 = launch_health_check()
    except Exception as e:
        logging.exception(f"Failed to start Jupiter health_check: {e}")

    logging.info("All processes have been started.")


if __name__ == "__main__":
    main()
