@echo off
setlocal enabledelayedexpansion

:: Set log file for overall script
set "log_file=git_script.log"
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