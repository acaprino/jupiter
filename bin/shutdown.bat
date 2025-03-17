@echo off
echo Killing processes launched by startup.bat...
echo.

REM Run kill_instances.bat to terminate Python processes
echo Running kill_instances.bat for Python processes...
call kill_instances.bat

echo.

REM Kill MongoDB process (adjust process name if needed)
echo Killing MongoDB process...
taskkill /IM mongod.exe /F

echo.

REM Kill Meta Trader process (terminal64.exe) as the bot process substitute
echo Killing Meta Trader (terminal64.exe) process...
taskkill /IM terminal64.exe /F

echo.

REM Kill Health Check process using the PID from health_check.pid
if exist health_check.pid (
    for /f "usebackq" %%i in (`type health_check.pid`) do (
        echo Killing Health Check process with PID %%i...
        taskkill /PID %%i /F
    )
) else (
    echo [INFO] health_check.pid not found. Health Check process may not be running.
)

echo.
echo All specified processes have been terminated.
pause
