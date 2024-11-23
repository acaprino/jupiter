@echo off
setlocal enabledelayedexpansion

:: Directory containing configurations and project name
set "config_dir=configs"
set "pid_dir=pid"
set "logs_dir=logs"
set "output_dir=output"

if exist %logs_dir% (
    rmdir /s /q %logs_dir%
)

if exist %output_dir% (
    rmdir /s /q %output_dir%
)

if not exist %pid_dir% mkdir %pid_dir%

:: Temporary files for categorization
set "temp_gen=generator_configs.tmp"
set "temp_sent=sentinel_configs.tmp"
set "temp_other=other_configs.tmp"
set "temp_middle=middleware_configs.tmp"

:: Clear temporary files
> %temp_gen% echo:
> %temp_sent% echo:
> %temp_other% echo:
> %temp_middle% echo:

:: Categorize JSON configurations by "mode" and check "enabled"
for %%f in (%config_dir%\*.json) do (
    set "is_enabled="
    set "mode="
    for /f "delims=" %%m in ('jq -r ".enabled" "%%f" 2^>nul') do set "is_enabled=%%m"
    for /f "delims=" %%m in ('jq -r ".mode" "%%f" 2^>nul') do set "mode=%%m"
    if /i "!is_enabled!"=="true" (
        if "!mode!"=="GENERATOR" (
            echo %%f>>%temp_gen%
        ) else if "!mode!"=="SENTINEL" (
            echo %%f>>%temp_sent%
        ) else if "!mode!"=="MIDDLEWARE" (
            echo %%f>>%temp_middle%
        ) else (
            echo %%f>>%temp_other%
        )
    )
)

:: Process MIDDLEWARE configurations first
if exist %temp_middle% (
    call :processConfigs %temp_middle% "MIDDLEWARE"
)

:: Process SENTINEL configurations
call :processConfigs %temp_sent% "SENTINEL"

:: Wait 10 seconds between SENTINEL and GENERATOR
if exist %temp_sent% (
    echo Waiting 10 seconds before processing GENERATOR configurations...
    timeout /t 10 >nul
)

:: Process GENERATOR configurations
call :processConfigs %temp_gen% "GENERATOR"

:: Process OTHER configurations
call :processConfigs %temp_other% "OTHER"

:: Cleanup
echo Cleaning up temporary files...
del %temp_gen% %temp_sent% %temp_other% %temp_middle%
goto :eof

:processConfigs
set "file_list=%1"
set "type=%2"
echo Processing configurations of type: %type%
for /f "delims=" %%f in (%file_list%) do (
    set "config_file=%%f"
    set "config_name=%%~nf"
    echo Starting configuration: %%f
    echo Command: ..\venv\Scripts\python.exe ..\main.py %%f
    start "Launching %%~nf" ..\venv\Scripts\python.exe ..\main.py %%f
    timeout /t 2 >nul
    for /f "tokens=2" %%a in ('tasklist /fi "imagename eq python.exe" /fo list ^| findstr "PID"') do (
        echo %%a > %pid_dir%\%%~nf.pid
    )
)
goto :eof
