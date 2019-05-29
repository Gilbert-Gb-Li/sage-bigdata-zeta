@echo off
SetLocal EnableDelayedExpansion

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set SAGE_HOME=%%~dpfI

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
REM ***** Replace worker.conf path *****
Set confFile=%SAGE_HOME%\conf\auth.conf
Set confFileTmp=%SAGE_HOME%\conf\auth.conf.tmp
Set confStr=./db/
Set confReplace=%SAGE_HOME:\=\\%\\db\\
Set confFile=%confFile:"=%
copy %confFile% %confFileTmp%
cd.>%confFileTmp%
For /F "Usebackq Delims=" %%i In ("%confFile%") Do (
    Set "Line=%%i"
	Echo !Line:%confStr%=%confReplace%! >>%confFileTmp%
)
copy %confFileTmp% %confFile%
del %confFileTmp%

REM ***** Replace logback-auth.xml path *****
Set logFile=%SAGE_HOME%\conf\logback-auth.xml
Set logFileTmp=%SAGE_HOME%\conf\logback-auth.xml.tmp
Set logStr=logs/
Set logReplace=%SAGE_HOME:\=\\%\\logs\\
Set logFile=%logFile:"=%
copy %logFile% %logFileTmp%
cd.>%logFileTmp%
For /F "Usebackq Delims=" %%i In ("%logFile%") Do (
    Set "Line=%%i"
    Echo !Line:%logStr%=%logReplace%! >>%logFileTmp%
)
copy %logFileTmp% %logFile%
del %logFileTmp%

REM ***** JAVA options *****

if "%sage-bigdata-etl_MIN_MEM%" == "" (
set sage-bigdata-etl_MIN_MEM=256m
)

if "%sage-bigdata-etl_MAX_MEM%" == "" (
set sage-bigdata-etl_MAX_MEM=1g
)

if NOT "%sage-bigdata-etl_HEAP_SIZE%" == "" (
set sage-bigdata-etl_MIN_MEM=%sage-bigdata-etl_HEAP_SIZE%
set sage-bigdata-etl_MAX_MEM=%sage-bigdata-etl_HEAP_SIZE%
)

REM min and max heap sizsage-bigdata-etl should be set to the same value to avoid
REM stop-the-world GC paussage-bigdata-etl during rsage-bigdata-etlize, and so that we can lock the
REM heap in memory on startup to prevent any of it from being swapped
REM out.
set JAVA_OPTS=%JAVA_OPTS% -Xms%sage-bigdata-etl_MIN_MEM% -Xmx%sage-bigdata-etl_MAX_MEM%

REM new generation
if NOT "%sage-bigdata-etl_HEAP_NEWSIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Xmn%sage-bigdata-etl_HEAP_NEWSIZE%
)

REM max direct memory
if NOT "%sage-bigdata-etl_DIRECT_SIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=%sage-bigdata-etl_DIRECT_SIZE%
)

REM reduce the per-thread stack size
set JAVA_OPTS=%JAVA_OPTS% -Xss256k

REM set to headlsage-bigdata-etls, just in case
set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headlsage-bigdata-etls=true

REM Force the JVM to use IPv4 stack
if NOT "%sage-bigdata-etl_USE_IPV4%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Djava.net.preferIPv4Stack=true
)

set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly

REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark

if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimsage-bigdata-etltamps
if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
if NOT "%sage-bigdata-etl_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -Xloggc:%SAGE_HOME%/logs/gc.log

REM Caussage-bigdata-etl the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$SAGE_HOME/logs/heapdump.hprof

REM Disablsage-bigdata-etl explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC

REM Ensure UTF-8 encoding by default (e.g. filenamsage-bigdata-etl)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

set sage-bigdata-etl_CLASSPATH=%SAGE_HOME%\conf\;%SAGE_HOME%\lib\*


SETLOCAL
TITLE sage-bigdata-etl-auth

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %sage-bigdata-etl_JAVA_OPTS% %sage-bigdata-etl_PARAMS% %* -cp "%sage-bigdata-etl_CLASSPATH%" "porter.runner.Auth"

ENDLOCAL
