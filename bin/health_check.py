from flask import Flask, jsonify
import os
import psutil

app = Flask(__name__)

@app.route('/status', methods=['GET'])
def status():
    pid_dir = 'pid'
    errors = {}  # Dizionario per raccogliere eventuali errori

    # Verifica che la cartella 'pid' esista
    if not os.path.isdir(pid_dir):
        return jsonify({
            'status': 'KO',
            'message': f'La cartella "{pid_dir}" non è stata trovata.'
        }), 500

    # Per ogni file nella cartella 'pid'
    for filename in os.listdir(pid_dir):
        file_path = os.path.join(pid_dir, filename)
        try:
            with open(file_path, 'r') as file:
                content = file.read().strip()
                pid = int(content)
        except Exception as e:
            errors[filename] = f'Errore nella lettura o conversione del PID: {e}'
            continue

        # Verifica se il processo con il PID letto esiste
        if not psutil.pid_exists(pid):
            errors[filename] = f'Processo con PID {pid} non trovato.'

    # Se non ci sono errori, restituisce "OK", altrimenti "KO"
    if not errors:
        return "OK", 200
    else:
        return jsonify({
            'status': 'KO',
            'details': errors
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
