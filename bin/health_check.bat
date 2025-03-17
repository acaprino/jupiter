@echo off
REM Get the current process PID using PowerShell and save it in the variable MyPID
for /f "usebackq" %%p in (`powershell -NoProfile -Command "$PID"`) do set MyPID=%%p

REM Check if health_check.pid already exists; if yes, delete it
if exist health_check.pid (
    echo [INFO] health_check.pid already exists. Deleting the old file...
    del health_check.pid
)

REM Write the PID into the file health_check.pid
echo %MyPID% > health_check.pid
echo [INFO] PID %MyPID% saved in health_check.pid

REM Start the health check routine
echo Starting health check...

REM Example infinite loop for the health check (modify the logic as needed)
:health_loop
echo [HEALTH CHECK] Running health check...
timeout /t 10 >nul
goto health_loop
