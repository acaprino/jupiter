@echo off
setlocal enabledelayedexpansion

:: Set log file for overall script
set "log_file=overall_script.log"
echo [DEBUG] Starting script >> "%log_file%"

:: Save the current directory
set "original_dir=%cd%"
echo [DEBUG] Original directory: %original_dir% >> "%log_file%"

:: Perform git pull in the parent directory
cd ..
echo [DEBUG] Performing git pull in the parent directory >> "%log_file%"
set "git_output=temp_git_output.txt"
git pull > "%git_output%" 2>> "%log_file%"
if %errorlevel% neq 0 (
    echo [ERROR] Git pull failed, see log for details. >> "%log_file%"
)

:: Check if requirements.txt was modified
echo [DEBUG] Checking if requirements.txt was modified >> "%log_file%"
findstr /i "requirements.txt" "%git_output%" >nul
if %errorlevel%==0 (
    echo [DEBUG] requirements.txt changed, installing dependencies... >> "%log_file%"
    call venv\Scripts\activate.bat
    pip install -r requirements.txt >> "%log_file%" 2>&1
) else (
    echo [DEBUG] requirements.txt not changed, skipping installation... >> "%log_file%"
)

:: Clean up temporary git output file
echo [DEBUG] Cleaning up temporary git output file >> "%log_file%"
del "%git_output%"

:: Return to the original directory
echo [DEBUG] Returning to original directory >> "%log_file%"
cd "%original_dir%"

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
    for /f "delims=" %%m in ('jq -r ".bot.mode" "%%~f" 2^>nul') do (
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
echo Command: ..\venv\Scripts\python.exe ..\main.py "%config_file%" >> "%log_file%"
start "Launching %config_name%" ..\venv\Scripts\python.exe ..\main.py "%config_file%"
timeout /t 2 >nul

:: Get the PID of the started process (may require adjustment)
for /f "tokens=2 delims=," %%a in ('wmic process where "CommandLine like '%%main.py%%'" get ProcessId /format:csv ^| findstr /i /c:"python.exe"') do (
    echo [DEBUG] Capturing PID %%a for config %config_name% >> "%log_file%"
    echo %%a > "%pid_dir%\%config_name%.pid"
)

:next
echo [DEBUG] Exiting processConfigs function >> "%log_file%"
endlocal & exit /b
