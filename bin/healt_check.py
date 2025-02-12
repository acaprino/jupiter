from flask import Flask, jsonify
import os
import psutil

app = Flask(__name__)

@app.route('/status', methods=['GET'])
def status():
    pid_dir = 'pid'
    errori = {}  # per raccogliere eventuali errori o processi non trovati

    # Verifica che la directory pid esista
    if not os.path.isdir(pid_dir):
        return jsonify({
            'status': 'ERROR',
            'message': f'La cartella "{pid_dir}" non è stata trovata.'
        }), 500

    # Per ogni file nella cartella pid
    for filename in os.listdir(pid_dir):
        file_path = os.path.join(pid_dir, filename)
        try:
            with open(file_path, 'r') as file:
                contenuto = file.read().strip()
                pid = int(contenuto)
        except Exception as e:
            errori[filename] = f'Errore nella lettura o conversione del PID: {e}'
            continue

        # Verifica se il processo esiste
        if not psutil.pid_exists(pid):
            errori[filename] = f'Processo con PID {pid} non trovato.'

    # Se non ci sono errori, tutto è OK
    if not errori:
        return "OK", 200
    else:
        return jsonify({
            'status': 'ERROR',
            'details': errori
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
