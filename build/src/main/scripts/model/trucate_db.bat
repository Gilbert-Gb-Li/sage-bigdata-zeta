@echo off
SetLocal EnableDelayedExpansion

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set SAGE_HOME=%%~dpfI

echo %SAGE_HOME%

if exist "%SAGE_HOME%\jre\1.8" (
    set JAVA_HOME=%SAGE_HOME%\jre\1.8
    goto cont
) else (
    if DEFINED JAVA_HOME goto cont
)

:err
echo JAVA_HOME environment variable must be set!
pause
exit

:cont
set path=%JAVA_HOME%\bin;%path%
set DERBY_HOME=%SAGE_HOME%\db
echo %JAVA_HOME%
echo %DERBY_HOME%

SETLOCAL
TITLE sage-bigdata-etl-trucate-db

call %DERBY_HOME%\bin\ij %SAGE_HOME%\bin\trucate.sql

ENDLOCAL