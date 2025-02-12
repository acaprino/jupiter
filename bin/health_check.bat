@echo off
REM Avvia il servizio Health Check in una nuova finestra
start "Health Check Service" python health_check.py

echo Il servizio Health Check è stato avviato.
pause
