from flask import Flask, jsonify
import os
import psutil

app = Flask(__name__)

@app.route('/status', methods=['GET'])
def status():
    pid_dir = 'pid'
    errors = {}  # Dictionary to collect any errors or missing processes

    # Check if the 'pid' directory exists
    if not os.path.isdir(pid_dir):
        return jsonify({
            'status': 'ERROR',
            'message': f'Directory "{pid_dir}" not found.'
        }), 500

    # Process each file in the 'pid' directory
    for filename in os.listdir(pid_dir):
        file_path = os.path.join(pid_dir, filename)
        try:
            with open(file_path, 'r') as file:
                content = file.read().strip()
                pid = int(content)
        except Exception as e:
            errors[filename] = f'Error reading or converting PID: {e}'
            continue

        # Check if the process with the given PID exists
        if not psutil.pid_exists(pid):
            errors[filename] = f'Process with PID {pid} not found.'

    # Return "OK" if there are no errors, otherwise return the error details
    if not errors:
        return "OK", 200
    else:
        return jsonify({
            'status': 'ERROR',
            'details': errors
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
