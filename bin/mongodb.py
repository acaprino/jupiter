import os
import subprocess
import logging
import time

def setup_logging(log_file):
    # Configura il logging per scrivere sia su file che su console.
    logging.basicConfig(
        level=logging.DEBUG,
        format='[DEBUG] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )
    logging.debug("Starting MongoDB startup script")

def get_child_pid(parent_pid):
    """
    Utilizza WMIC per ottenere il PID del processo figlio avente come genitore parent_pid.
    """
    try:
        result = subprocess.check_output(
            ["wmic", "process", "where", f"ParentProcessId={parent_pid}", "get", "ProcessId"],
            universal_newlines=True
        )
        lines = result.strip().splitlines()
        child_pids = []
        for line in lines:
            line = line.strip()
            if line.isdigit():
                child_pids.append(int(line))
        if child_pids:
            return child_pids[0]  # Prende il primo PID trovato
    except Exception as e:
        logging.error(f"Errore durante l'ottenimento del PID figlio: {e}")
    return None

def main():
    log_file = "startup.log"
    setup_logging(log_file)

    # Verifica se la directory "./mongodb/data" esiste, altrimenti creala.
    data_dir = os.path.join("mongodb", "data")
    if not os.path.exists(data_dir):
        logging.debug(f"Directory {data_dir} non esiste, la creo...")
        os.makedirs(data_dir)

    logging.debug("Starting MongoDB...")
    mongo_executable = os.path.join("mongodb", "bin", "mongod.exe")
    config_file = "mongod.cfg"
    pid_file = "mongodb.pid"

    try:
        # Avvia mongo.exe senza creare una nuova finestra CMD.
        process = subprocess.Popen(
            [mongo_executable, "--config", config_file],
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        logging.debug(f"Processo avviato con PID {process.pid}. Attendo la creazione del processo figlio...")

        # Attende alcuni secondi per verificare se viene creato un processo figlio
        child_pid = None
        for _ in range(10):
            child_pid = get_child_pid(process.pid)
            if child_pid:
                break
            time.sleep(1)

        if child_pid:
            actual_pid = child_pid
            logging.debug(f"Trovato processo figlio con PID {actual_pid}")
        else:
            actual_pid = process.pid
            logging.debug(f"Nessun processo figlio trovato; uso il PID del processo genitore: {actual_pid}")

        # Scrive il PID effettivo nel file mongodb.pid nel formato "PID;mongodb"
        with open(pid_file, "w") as f:
            f.write(f"{actual_pid};mongodb")
        logging.debug(f"PID {actual_pid} scritto in {pid_file}")

        # Attende che il processo (genitore) termini.
        process.wait()
    except Exception as e:
        logging.error(f"Impossibile avviare MongoDB: {e}")

if __name__ == "__main__":
    main()
