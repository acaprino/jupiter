@echo off
setlocal enabledelayedexpansion

:: Set log file for overall script
set "log_file=overall_script.log"
echo [DEBUG] Starting script >> "%log_file%"

:: Directory containing configurations and project name
set "config_dir=configs"
set "pid_dir=pid"
set "logs_dir=logs"
set "output_dir=output"

:: Check if logs directory exists
echo [DEBUG] Checking if logs directory exists >> "%log_file%"
if exist "%logs_dir%" (
    echo [DEBUG] Removing logs directory >> "%log_file%"
    rmdir /s /q "%logs_dir%" >> "%log_file%" 2>&1
)

:: Check if output directory exists
echo [DEBUG] Checking if output directory exists >> "%log_file%"
if exist "%output_dir%" (
    echo [DEBUG] Removing output directory >> "%log_file%"
    rmdir /s /q "%output_dir%" >> "%log_file%" 2>&1
)

:: Check if PID directory exists
echo [DEBUG] Checking if PID directory exists >> "%log_file%"
if not exist "%pid_dir%" (
    echo [DEBUG] Creating PID directory >> "%log_file%"
    mkdir "%pid_dir%" >> "%log_file%" 2>&1
)

:: Check if silent.sem exists and set extra parameter accordingly
if exist "silent.sem" (
    set "extraParam=start_silent"
    echo [DEBUG] silent.sem trovato: aggiungo il parametro start_silent >> "%log_file%"
) else (
    set "extraParam="
    echo [DEBUG] silent.sem non trovato: nessun parametro extra >> "%log_file%"
)

:: Temporary files for categorization
echo [DEBUG] Setting temporary files for categorization >> "%log_file%"
set "temp_gen=generator_configs.tmp"
set "temp_sent=sentinel_configs.tmp"
set "temp_middle=middleware_configs.tmp"

:: Clear temporary files
echo [DEBUG] Clearing temporary files >> "%log_file%"
echo. > "%temp_gen%"
echo. > "%temp_sent%"
echo. > "%temp_middle%"

:: Categorize JSON configurations by "mode" and check "enabled"
echo [DEBUG] Categorizing JSON configurations by "mode" and checking "enabled" >> "%log_file%"
for %%f in ("%config_dir%\*.json") do (
    echo [DEBUG] Processing file: %%f >> "%log_file%"
    set "is_enabled="
    set "mode="

    :: Use jq to get "enabled" value
    for /f "delims=" %%m in ('jq -r ".enabled" "%%~f" 2^>nul') do (
        set "is_enabled=%%m"
    )
    echo [DEBUG] is_enabled=!is_enabled! >> "%log_file%"

    :: Use jq to get "mode" value
    for /f "delims=" %%m in ('jq -r ".mode" "%%~f" 2^>nul') do (
        set "mode=%%m"
    )
    echo [DEBUG] mode=!mode! >> "%log_file%"

    :: Categorize files based on mode and enabled status
    if /i "!is_enabled!"=="true" (
        if /i "!mode!"=="GENERATOR" (
            echo [DEBUG] Adding to GENERATOR list >> "%log_file%"
            echo %%f >> "%temp_gen%"
        ) else if /i "!mode!"=="SENTINEL" (
            echo [DEBUG] Adding to SENTINEL list >> "%log_file%"
            echo %%f >> "%temp_sent%"
        ) else if /i "!mode!"=="MIDDLEWARE" (
            echo [DEBUG] Adding to MIDDLEWARE list >> "%log_file%"
            echo %%f >> "%temp_middle%"
        ) else (
            echo [DEBUG] File %%f has an unknown mode, skipping >> "%log_file%"
        )
    ) else (
        echo [DEBUG] File %%f is not enabled, skipping >> "%log_file%"
    )
)

:: Process MIDDLEWARE configurations first
echo [DEBUG] Processing MIDDLEWARE configurations >> "%log_file%"
if exist "%temp_middle%" (
    for /f "delims=" %%f in ('type "%temp_middle%"') do (
        call :processConfigs "%%f" "MIDDLEWARE"
    )
)

:: Wait 10 seconds between MIDDLEWARE and SENTINEL
if exist "%temp_middle%" (
    echo [DEBUG] Waiting 10 seconds before processing SENTINEL configurations... >> "%log_file%"
    timeout /t 10 >nul
)

:: Process SENTINEL configurations
echo [DEBUG] Processing SENTINEL configurations >> "%log_file%"
if exist "%temp_sent%" (
    for /f "delims=" %%f in ('type "%temp_sent%"') do (
        call :processConfigs "%%f" "SENTINEL"
    )
)

:: Wait 10 seconds between SENTINEL and GENERATOR
if exist "%temp_sent%" (
    echo [DEBUG] Waiting 10 seconds before processing GENERATOR configurations... >> "%log_file%"
    timeout /t 10 >nul
)

:: Process GENERATOR configurations
echo [DEBUG] Processing GENERATOR configurations >> "%log_file%"
if exist "%temp_gen%" (
    for /f "delims=" %%f in ('type "%temp_gen%"') do (
        call :processConfigs "%%f" "GENERATOR"
    )
)

:: Cleanup
echo [DEBUG] Cleaning up temporary files... >> "%log_file%"
del "%temp_gen%" "%temp_sent%" "%temp_middle%" >> "%log_file%" 2>&1
goto :eof

:processConfigs
setlocal
echo [DEBUG] Entering processConfigs function >> "%log_file%"

:: Set parameters for the current function call
set "config_file=%~1"
set "type=%~2"
echo [DEBUG] Processing configuration: %config_file% of type: %type% >> "%log_file%"

:: Extract the config name (filename without extension)
set "config_name=%~n1"

if not exist "%config_file%" (
    echo [ERROR] File not found: %config_file%. Skipping... >> "%log_file%"
    goto :next
)

echo [DEBUG] Starting process for configuration: %config_file% >> "%log_file%"
echo Command: ..\venv\Scripts\python.exe ..\main.py "%config_file%" %extraParam% >> "%log_file%"

:: Build the PowerShell command
set "psCommand=$process = Start-Process -FilePath '..\venv\Scripts\python.exe' -ArgumentList '..\main.py \"%config_file%\" %extraParam%' -PassThru; Set-Content -Path '%pid_dir%\%config_name%.pid' -Value $process.Id"

:: Execute the PowerShell command to start the process and write the PID
powershell -NoProfile -ExecutionPolicy Bypass -Command "%psCommand%"

echo [DEBUG] Started process for %config_name%, PID written to %pid_dir%\%config_name%.pid >> "%log_file%"

:next
echo [DEBUG] Exiting processConfigs function >> "%log_file%"
endlocal & exit /b
