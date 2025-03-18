from flask import Flask, jsonify
import os
import psutil
import threading
import time
import logging

# Configure logging to write to startup.log
logging.basicConfig(
    filename="startup.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_print(message):
    """Prints the message to console and logs it to startup.log."""
    print(message)
    logging.info(message)

app = Flask(__name__)

@app.route('/status', methods=['GET'])
def status():
    log_print("Status endpoint called.")
    pid_dir = 'pid'
    errors = {}  # Dictionary to collect any errors

    # Check that the 'pid' folder exists
    if not os.path.isdir(pid_dir):
        err_msg = f'The directory "{pid_dir}" was not found.'
        logging.error(err_msg)
        return jsonify({
            'status': 'KO',
            'message': err_msg
        }), 500

    # For each file in the 'pid' folder
    for filename in os.listdir(pid_dir):
        file_path = os.path.join(pid_dir, filename)
        try:
            with open(file_path, 'r') as file:
                content = file.read().strip()
                pid_str = content.split(';')[0]
                pid = int(pid_str)
        except Exception as e:
            errors[filename] = f'Error reading or converting PID: {e}'
            logging.error(f"Error in file {filename}: {e}")
            continue

        # Check if the process with the read PID exists
        if not psutil.pid_exists(pid):
            errors[filename] = f'Process with PID {pid} not found.'
            logging.error(f"Process with PID {pid} not found in file {filename}.")

    # Return "OK" if no errors, otherwise "KO" with error details
    if not errors:
        return "OK", 200
    else:
        return jsonify({
            'status': 'KO',
            'details': errors
        }), 500

def write_pid_file():
    # Create the 'pid' folder if it doesn't exist
    pid_file = "health_check.pid"
    # If the file exists, delete it
    if os.path.exists(pid_file):
        log_print(f"[INFO] {pid_file} already exists. Deleting the old file...")
        os.remove(pid_file)
    # Write the current process PID to the file
    pid = os.getpid()
    with open(pid_file, "w") as f:
        f.write(str(pid))
    log_print(f"[INFO] PID {pid} saved in {pid_file}")

def health_check_loop():
    log_print("Starting health check...")
    while True:
        log_print("[HEALTH CHECK] Running health check...")
        time.sleep(10)  # Sleep for 10 seconds (similar to the batch timeout)

if __name__ == '__main__':
    # Write the PID file inside the "pid" folder
    write_pid_file()
    # Start the health check loop in a separate daemon thread
    health_thread = threading.Thread(target=health_check_loop, daemon=True)
    health_thread.start()
    log_print("Starting Flask app...")
    # Start the Flask application
    app.run(host='0.0.0.0', debug=False, use_reloader=False)
