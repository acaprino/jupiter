import json

from flask import Flask, jsonify
import os
import psutil
import logging

app = Flask(__name__)


@app.route('/status', methods=['GET'])
def status():
    pid_dir = 'pid_instances'
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


@app.route('/logs/<log_name>', methods=['GET'])
def get_logs(log_name):
    logs_dir = './logs'
    result = {}

    # Costruisco il path del file di log basato sul parametro log_name
    log_file_path = os.path.join(logs_dir, f"{log_name}.log")
    if not os.path.isfile(log_file_path):
        logging.error(f"Log file {log_file_path} not found for bot {log_name}")
        return jsonify({
            'status': 'KO',
            'message': f"Log file for '{log_name}' not found."
        }), 404

    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as log_file:
            lines = log_file.readlines()
            # Se il file contiene almeno 1000 righe, prendi le ultime 1000, altrimenti prendi tutte le righe
            last_lines = lines[-1000:] if len(lines) >= 1000 else lines
            # Rimuovo il carattere newline da ciascuna riga
            last_lines = [line.rstrip('\n') for line in last_lines]
            result[log_name] = last_lines
    except Exception as e:
        logging.error(f"Error reading log file {log_file_path} for bot {log_name}: {e}")
        return jsonify({
            'status': 'KO',
            'message': f"Error reading log file: {e}"
        }), 500

    return jsonify(result), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, use_reloader=False)
