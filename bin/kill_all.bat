:: Script per rilanciare tutte le configurazioni trovate nella subdirectory 'configs'
@echo off
setlocal

:: Directory contenente le configurazioni e il nome del progetto
set "pid_dir=pid"

:: Elimina la directory 'logs' se esiste
:: Svuota la directory 'pid'
if exist %pid_dir% (
    for %%p in (%pid_dir%\*.pid) do (
        for /f %%a in (%%p) do (
            echo Terminazione processo con PID %%a
            taskkill /pid %%a /f
        )
        del %%p
    )
)

endlocal