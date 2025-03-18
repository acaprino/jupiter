import os
import subprocess
import logging

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

def main():
    log_file = "startup.log"
    setup_logging(log_file)

    # Controlla se la directory "./mongodb/data" esiste; se non esiste, creala.
    data_dir = os.path.join("mongodb", "data")
    if not os.path.exists(data_dir):
        logging.debug(f"Directory {data_dir} non esiste, la creo...")
        os.makedirs(data_dir)

    # Specifica l'eseguibile di MongoDB e il file di configurazione.
    logging.debug("Starting MongoDB...")
    mongo_executable = os.path.join("mongodb", "bin", "mongod.exe")
    config_file = "mongod.cfg"

    try:
        # Avvia MongoDB con Popen per poter ottenere il PID
        process = subprocess.Popen([mongo_executable, "--config", config_file])
        logging.debug(f"MongoDB avviato con PID {process.pid}")

        # Scrive il PID su file mongodb.pid
        pid_file = "mongodb.pid"
        with open(pid_file, "w") as f:
            f.write(str(process.pid))
        logging.debug(f"PID {process.pid} scritto in {pid_file}")

        # Attende che il processo termini.
        process.wait()
    except Exception as e:
        logging.error(f"Impossibile avviare MongoDB: {e}")

    # Attende l'input dell'utente prima di terminare (simile al comando pause in batch).
    input("Premi Invio per uscire...")

if __name__ == "__main__":
    main()
