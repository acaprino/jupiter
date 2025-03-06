@echo off
REM Check if the directory .\mongodb\data exists; if not, create it.
if not exist ".\mongodb\data" (
    echo Directory .\mongodb\data does not exist, creating it...
    mkdir ".\mongodb\data"
)

REM Start MongoDB with the specified dbpath and bind to all interfaces.
echo Starting MongoDB...
".\mongodb\bin\mongod.exe" --config mongod.cfg

pause
