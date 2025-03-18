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
    mongo_executable = os.path.join("mongodb", "bin", "mongod")
    config_file = "mongod.cfg"
    pid_file = "mongodb.pid"

    try:
        # Avvia MongoDB in modalità fork specificando il percorso del file PID.
        # Nota: l'opzione --fork è disponibile solo su sistemi Unix.
        process = subprocess.Popen([
            mongo_executable,
            "--config", config_file,
            "--fork",
            "--pidfilepath", pid_file
        ])
        logging.debug("MongoDB avviato in modalità fork.")

        # Attende che il file del PID venga creato da MongoDB.
        for i in range(10):
            if os.path.exists(pid_file):
                with open(pid_file, "r") as f:
                    actual_pid = f.read().strip()
                logging.debug(f"PID corretto letto da file: {actual_pid}")
                break
            time.sleep(1)
        else:
            logging.error("Il file del PID non è stato creato.")
    except Exception as e:
        logging.error(f"Impossibile avviare MongoDB: {e}")

    # Attende l'input dell'utente prima di terminare.
    input("Premi Invio per uscire...")

if __name__ == "__main__":
    main()
