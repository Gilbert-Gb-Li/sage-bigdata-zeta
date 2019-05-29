@echo off

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set SAGE_HOME=%%~dpfI


::Reset env
set PROC=
set PORT=
set PORTSTR=

::Get daemon internal port number
for /f "tokens=1 delims=" %%i in ('type %SAGE_HOME%\\conf\worker.conf ^| findstr "worker.daemon.port"') do (
   set PORTSTR=%%i
)
echo %PORTSTR%

for %%i in (%PORTSTR%) do (
   set PORT=%%i
)
echo %PORT%

::Get PID
for /f "tokens=5" %%i in ('netstat -aon ^| findstr "LISTENING" ^| findstr "%PORT%"') do (
    set PROC=%%i
    goto :kill_PROC
)

:kill_PROC
echo %PROC%

echo Try to end task with daemon PID: %PROC%
taskkill /F /PID %PROC%

