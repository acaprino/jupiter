import os
import sys
import glob
import logging
import subprocess
import signal

def setup_logging(log_file):
    # Configura il logging per scrivere sia sul file che sulla console
    logging.basicConfig(
        level=logging.DEBUG,
        format='[DEBUG] %(message)s',
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
        # Windows: usa taskkill per terminare il processo
        try:
            result = subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True)
            if result.returncode == 0:
                logging.debug(f"Successfully killed process {pid}")
            else:
                logging.error(f"Failed to kill process {pid}: {result.stderr.strip()}")
        except Exception as e:
            logging.error(f"Exception while killing process {pid}: {e}")
    else:
        # Unix-like: invia SIGTERM al processo
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
    else:
        # Recupera tutti i file .pid nella cartella pid (inclusi quelli nascosti)
        pid_files = glob.glob(os.path.join(pid_dir, "*.pid")) + glob.glob(os.path.join(pid_dir, ".*.pid"))
        if not pid_files:
            logging.debug("No PID files found in PID directory.")
        else:
            # Raggruppa i processi per tipo (formato atteso: "PID;type")
            processes_by_type = {}
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
                    processes_by_type.setdefault(proc_type, []).append((pid_str, pid_file))
                except Exception as e:
                    logging.error(f"Error reading PID file {pid_file}: {e}")

            # Ordine di terminazione: executor, poi generator, poi middleware
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

def kill_additional_instances():
    # I file healt_check.pid e mongodb.pid si trovano nella stessa directory dello script
    script_dir = os.path.dirname(os.path.realpath(__file__))
    additional_files = ["healt_check.pid", "mongodb.pid"]
    for file_name in additional_files:
        file_path = os.path.join(script_dir, file_name)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    content = f.read().strip()
                # Se il file contiene il carattere ';', si assume formato "PID;qualcosa"
                if ";" in content:
                    pid_str = content.split(";")[0].strip()
                else:
                    pid_str = content.strip()
                logging.debug(f"Attempting to kill process from file {file_path} with PID {pid_str}")
                kill_process(pid_str)
                os.remove(file_path)
                logging.debug(f"Removed PID file: {file_path}")
            except Exception as e:
                logging.error(f"Error processing additional PID file {file_path}: {e}")
        else:
            logging.debug(f"Additional PID file {file_path} does not exist.")

def main():
    log_file = "startup.log"
    setup_logging(log_file)
    kill_instances()
    kill_additional_instances()

if __name__ == "__main__":
    main()
