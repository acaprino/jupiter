@echo off
setlocal enabledelayedexpansion

:: Determina la directory in cui risiede questo script
set "SCRIPT_DIR=%~dp0"

:: Imposta il file di log nella directory dello script
set "log_file=%SCRIPT_DIR%git_pull.log"

echo [DEBUG] Starting script at %date% %time% >> "%log_file%"
echo [INFO] Avvio script alle %date% %time% >> "%log_file%"

:: Salva la directory corrente
set "original_dir=%cd%"
echo [DEBUG] Directory originale: %original_dir% >> "%log_file%"

:: Passa alla directory padre (presumibilmente dove risiede il repository Git)
cd ..
echo [DEBUG] Cambiata directory in: %cd% >> "%log_file%"
echo [INFO] Eseguo git pull nella directory: %cd% >> "%log_file%"

:: Imposta il file temporaneo per l'output di git nella directory dello script
set "git_output=%SCRIPT_DIR%temp_git_output.txt"
git pull > "%git_output%" 2>&1

:: Aggiunge l'output di git pull al file di log
echo [INFO] Output di git pull: >> "%log_file%"
type "%git_output%" >> "%log_file%"
echo [DEBUG] Git pull completato >> "%log_file%"

if %errorlevel% neq 0 (
    echo [ERROR] Git pull fallito, controlla il log per i dettagli. >> "%log_file%"
) else (
    echo [INFO] Git pull completato con successo. >> "%log_file%"
)

:: Verifica se requirements.txt è stato modificato
echo [DEBUG] Verifica modifiche a requirements.txt >> "%log_file%"
findstr /i "requirements.txt" "%git_output%" >nul
if %errorlevel%==0 (
    echo [INFO] Il file requirements.txt è stato modificato, installo le dipendenze... >> "%log_file%"
    call venv\Scripts\activate.bat >> "%log_file%" 2>&1
    pip install -r requirements.txt >> "%log_file%" 2>&1
    echo [INFO] Installazione delle dipendenze completata. >> "%log_file%"
) else (
    echo [INFO] Nessuna modifica a requirements.txt, salto l'installazione. >> "%log_file%"
)

:: Rimuove il file temporaneo per l'output di git
echo [DEBUG] Rimozione del file temporaneo di git output >> "%log_file%"
del "%git_output%"

:: Torna alla directory originale
echo [DEBUG] Ritorno alla directory originale: %original_dir% >> "%log_file%"
cd "%original_dir%"
echo [INFO] Ritorno alla directory originale: %original_dir% >> "%log_file%"
