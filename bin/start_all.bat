@echo off
setlocal enabledelayedexpansion

:: -----------------------------------------------------------
:: Script to start bot instances based on JSON configurations
:: -----------------------------------------------------------

:: Set the log file
set "log_file=start_bot_instances.log"
echo [DEBUG] [%date% %time%] Starting bot instances script >> "%log_file%"

:: Set directories for configurations and PID files
set "config_dir=configs"
set "pid_dir=pid"

:: Check if the PID directory exists, otherwise create it
if not exist "%pid_dir%" (
    echo [DEBUG] [%date% %time%] PID directory not found, creating "%pid_dir%" >> "%log_file%"
    mkdir "%pid_dir%"
) else (
    echo [DEBUG] [%date% %time%] PID directory exists: "%pid_dir%" >> "%log_file%"
)

:: Set temporary files for categorizing configurations
set "temp_gen=generator_configs.tmp"
set "temp_sent=sentinel_configs.tmp"
set "temp_middle=middleware_configs.tmp"

:: Clear temporary files (if they exist)
echo [DEBUG] [%date% %time%] Clearing temporary files >> "%log_file%"
break > "%temp_gen%"
break > "%temp_sent%"
break > "%temp_middle%"

:: -----------------------------------------------------------
:: Categorize JSON files based on "enabled" and "mode"
:: -----------------------------------------------------------
echo [DEBUG] [%date% %time%] Starting categorization of JSON files in directory "%config_dir%" >> "%log_file%"
for %%f in ("%config_dir%\*.json") do (
    echo [DEBUG] [%date% %time%] Processing file: %%f >> "%log_file%"
    set "is_enabled="
    set "mode="

    :: Get the "enabled" value using jq
    for /f "delims=" %%m in ('jq -r ".enabled" "%%~f" 2^>nul') do (
        set "is_enabled=%%m"
    )
    echo [DEBUG] [%date% %time%] "enabled" value for %%f: !is_enabled! >> "%log_file%"

    :: Get the "mode" value using jq
    for /f "delims=" %%m in ('jq -r ".mode" "%%~f" 2^>nul') do (
        set "mode=%%m"
    )
    echo [DEBUG] [%date% %time%] "mode" value for %%f: !mode! >> "%log_file%"

    :: If the file is enabled, add it to the appropriate list based on its mode
    if /i "!is_enabled!"=="true" (
        if /i "!mode!"=="GENERATOR" (
            echo [DEBUG] [%date% %time%] Adding file %%f to GENERATOR list >> "%log_file%"
            echo %%f >> "%temp_gen%"
        ) else if /i "!mode!"=="SENTINEL" (
            echo [DEBUG] [%date% %time%] Adding file %%f to SENTINEL list >> "%log_file%"
            echo %%f >> "%temp_sent%"
        ) else if /i "!mode!"=="MIDDLEWARE" (
            echo [DEBUG] [%date% %time%] Adding file %%f to MIDDLEWARE list >> "%log_file%"
            echo %%f >> "%temp_middle%"
        ) else (
            echo [DEBUG] [%date% %time%] File %%f has unknown mode ("!mode!"), skipping >> "%log_file%"
        )
    ) else (
        echo [DEBUG] [%date% %time%] File %%f is not enabled, skipping >> "%log_file%"
    )
)

:: -----------------------------------------------------------
:: Function to start a bot process for a given configuration
:: -----------------------------------------------------------
:processConfigs
setlocal
set "config_file=%~1"
set "type=%~2"
echo [DEBUG] [%date% %time%] Starting process for configuration: %config_file% (Type: %type%) >> "%log_file%"

:: Extract the configuration name (filename without extension)
set "config_name=%~n1"

if not exist "%config_file%" (
    echo [ERROR] [%date% %time%] Configuration file not found: %config_file%. Skipping. >> "%log_file%"
    goto :next
)

echo [DEBUG] [%date% %time%] Preparing to execute command for configuration: %config_file% >> "%log_file%"

:: Check if silent.sem exists, and set extra parameter if it does
if exist "silent.sem" (
    echo [DEBUG] [%date% %time%] silent.sem found, adding 'silent' parameter >> "%log_file%"
    set "silentParam=silent"
) else (
    set "silentParam="
)

echo [DEBUG] [%date% %time%] Executing command: ..\venv\Scripts\python.exe ..\main.py "%config_file%" %silentParam% >> "%log_file%"

:: Build the PowerShell command to start the process and save its PID
set "psCommand=$process = Start-Process -FilePath '..\venv\Scripts\python.exe' -ArgumentList '..\main.py \"%config_file%\" %silentParam%' -PassThru; Set-Content -Path '%pid_dir%\%config_name%.pid' -Value $process.Id"
echo [DEBUG] [%date% %time%] PowerShell command: %psCommand% >> "%log_file%"

:: Execute the PowerShell command
powershell -NoProfile -ExecutionPolicy Bypass -Command "%psCommand%"

echo [DEBUG] [%date% %time%] Process for %config_name% started; PID saved in "%pid_dir%\%config_name%.pid" >> "%log_file%"
:next
endlocal & exit /b

:: -----------------------------------------------------------
:: Start instances in the following order: MIDDLEWARE, SENTINEL, GENERATOR
:: -----------------------------------------------------------

:: Process MIDDLEWARE configurations first
echo [DEBUG] [%date% %time%] Processing MIDDLEWARE configurations >> "%log_file%"
if exist "%temp_middle%" (
    for /f "delims=" %%f in ('type "%temp_middle%"') do (
        call :processConfigs "%%f" "MIDDLEWARE"
    )
) else (
    echo [DEBUG] [%date% %time%] No MIDDLEWARE configurations found >> "%log_file%"
)

:: Wait 10 seconds before processing SENTINEL configurations
echo [DEBUG] [%date% %time%] Waiting 10 seconds before processing SENTINEL configurations >> "%log_file%"
timeout /t 10 >nul

:: Process SENTINEL configurations
echo [DEBUG] [%date% %time%] Processing SENTINEL configurations >> "%log_file%"
if exist "%temp_sent%" (
    for /f "delims=" %%f in ('type "%temp_sent%"') do (
        call :processConfigs "%%f" "SENTINEL"
    )
) else (
    echo [DEBUG] [%date% %time%] No SENTINEL configurations found >> "%log_file%"
)

:: Wait another 10 seconds before processing GENERATOR configurations
echo [DEBUG] [%date% %time%] Waiting 10 seconds before processing GENERATOR configurations >> "%log_file%"
timeout /t 10 >nul

:: Process GENERATOR configurations
echo [DEBUG] [%date% %time%] Processing GENERATOR configurations >> "%log_file%"
if exist "%temp_gen%" (
    for /f "delims=" %%f in ('type "%temp_gen%"') do (
        call :processConfigs "%%f" "GENERATOR"
    )
) else (
    echo [DEBUG] [%date% %time%] No GENERATOR configurations found >> "%log_file%"
)

:: -----------------------------------------------------------
:: Clean up temporary files and finish the script
:: -----------------------------------------------------------
echo [DEBUG] [%date% %time%] Cleaning up temporary files >> "%log_file%"
del "%temp_gen%" "%temp_sent%" "%temp_middle%" >> "%log_file%" 2>&1

echo [DEBUG] [%date% %time%] Bot instances startup completed >> "%log_file%"
endlocal
