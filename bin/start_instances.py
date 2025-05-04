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
    logging.debug(f"Found {len(config_files)} {config_type} configuration file(s).")
    for config_file in config_files:
        logging.debug(f"Preparing to start {config_type} instance with configuration: {config_file}")
        # Percorsi assoluti per evitare problemi
        # Assicurati che questi percorsi relativi siano corretti rispetto alla posizione di esecuzione dello script startup.py
        python_exe = os.path.abspath(os.path.join(os.path.dirname(__file__), r"..\venv\Scripts\python.exe"))
        main_script = os.path.abspath(os.path.join(os.path.dirname(__file__), r"..\main.py"))
        config_file_abs = os.path.abspath(config_file)

        # Verifica l'esistenza degli eseguibili/script prima di tentare l'esecuzione
        if not os.path.exists(python_exe):
             logging.error(f"Python executable not found at: {python_exe}")
             continue # Salta questo file di configurazione
        if not os.path.exists(main_script):
             logging.error(f"Main script not found at: {main_script}")
             continue # Salta questo file di configurazione
        if not os.path.exists(config_file_abs):
             logging.error(f"Config file not found at: {config_file_abs}")
             continue # Salta questo file di configurazione

        # Costruisci il comando come lista di argomenti
        cmd = [python_exe, main_script, config_file_abs]
        if silent_param:
            cmd.append(silent_param)
            logging.debug(f"Silent parameter added: {silent_param}")

        logging.debug("Command to run: " + " ".join(cmd))
        try:
            # Lancia il processo senza reindirizzare stdout/stderr, in modo da usare la console della nuova finestra
            process = subprocess.Popen(
                cmd,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
                # Opzionale: imposta la directory di lavoro se main.py si aspetta di essere eseguito da una posizione specifica
                # cwd=os.path.dirname(main_script)
            )
            pid = process.pid
            logging.debug(f"Started process with PID {pid} for configuration: {config_file}")
            if not os.path.exists(pid_dir):
                os.makedirs(pid_dir)
                logging.debug(f"Created PID directory: {pid_dir}")
            pid_file_name = os.path.join(pid_dir, f"{os.path.basename(config_file)}.pid")
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

    # Define directories relative to the script's location
    script_dir = os.path.dirname(__file__) or '.' # Directory corrente se __file__ non è definito
    config_dir = os.path.join(script_dir, "configs")
    pid_dir = os.path.join(script_dir, "pid_instances")
    logs_dir = os.path.join(script_dir, "logs")
    output_dir = os.path.join(script_dir, "output")
    delete_logs_sem = os.path.join(script_dir, "delete_logs.sem") # Path per il file semaforo
    silent_sem = os.path.join(script_dir, "silent.sem") # Path per il file semaforo

    logging.debug(f"Script directory: {script_dir}")
    logging.debug(f"Configuration directory: {config_dir}")
    logging.debug(f"PID directory: {pid_dir}")
    logging.debug(f"Logs directory: {logs_dir}")
    logging.debug(f"Output directory: {output_dir}")

    # Check for the existence of delete_logs.sem to trigger deletion
    logging.debug(f"Checking for semaphore file: {delete_logs_sem}")
    if os.path.exists(delete_logs_sem):
        logging.info(f"{os.path.basename(delete_logs_sem)} found: proceeding with deletion of logs and output directories.")
        delete_directories([logs_dir, output_dir])
    else:
        logging.debug(f"{os.path.basename(delete_logs_sem)} not found: logs and output directories will not be deleted.")

    # Check for the existence of silent.sem to set an extra parameter
    logging.debug(f"Checking for semaphore file: {silent_sem}")
    if os.path.exists(silent_sem):
        silent_param = "start_silent"
        logging.debug(f"{os.path.basename(silent_sem)} found: adding parameter start_silent")
    else:
        silent_param = ""
        logging.debug(f"{os.path.basename(silent_sem)} not found: no extra parameter")

    # Retrieve all configuration files from the configs directory
    if not os.path.isdir(config_dir):
        logging.error(f"Configuration directory not found: {config_dir}. Exiting.")
        return

    config_files_all = glob.glob(os.path.join(config_dir, "*"))
    logging.debug(f"Total configuration files found in {config_dir}: {len(config_files_all)}")
    if not config_files_all:
        logging.warning("No configuration files found in configs directory. Exiting main.") # Cambiato a warning
        return

    middleware_configs = []
    generator_configs = []
    executor_configs = []

    for config_file in config_files_all:
        if not os.path.isfile(config_file): # Ignora eventuali sottodirectory
             logging.debug(f"Skipping non-file item in config dir: {config_file}")
             continue
        try:
            with open(config_file, 'r', encoding='utf-8') as f: # Aggiunto encoding
                config_data = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {config_file}: {e}")
            continue
        except Exception as e:
            logging.error(f"Error reading or parsing {config_file}: {e}")
            continue

        # Leggi il valore del campo "mode" e convertilo in minuscolo
        mode = config_data.get("mode", "").lower()

        if mode == "middleware":
            middleware_configs.append(config_file)
        elif mode == "generator":
            generator_configs.append(config_file)
        elif mode == "executor":
            executor_configs.append(config_file)
        else:
             logging.warning(f"Unknown mode '{mode}' found in config file: {config_file}")


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