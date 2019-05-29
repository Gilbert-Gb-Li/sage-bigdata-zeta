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
REM ***** Replace logback-daemon.xml path *****
Set logFile=%SAGE_HOME%\conf\logback-daemon.xml
Set logFileTmp=%SAGE_HOME%\conf\logback-daemon.xml.tmp
Set logStr=logs/
Set logReplace=%SAGE_HOME:\=\\%\\logs\\
Set logFile=%logFile:"=%
copy %logFile% %logFileTmp%
cd.>%logFileTmp%
For /F "Usebackq Delims=" %%i In ("%logFile%") Do (
    Set "Line=%%i"
    Echo !Line:%logStr%=%logReplace%! >>%logFileTmp%
)
type %logFileTmp% >%logFile%
del %logFileTmp%

REM ***** JAVA options *****

if "%SAGE_MIN_MEM%" == "" (
set SAGE_MIN_MEM=256m
)

if "%SAGE_MAX_MEM%" == "" (
set SAGE_MAX_MEM=1g
)

if NOT "%SAGE_HEAP_SIZE%" == "" (
set SAGE_MIN_MEM=%SAGE_HEAP_SIZE%
set SAGE_MAX_MEM=%SAGE_HEAP_SIZE%
)

REM min and max heap sizSAGE should be set to the same value to avoid
REM stop-the-world GC pausSAGE during rSAGEize, and so that we can lock the
REM heap in memory on startup to prevent any of it from being swapped
REM out.
set JAVA_OPTS=%JAVA_OPTS% -Xms%SAGE_MIN_MEM% -Xmx%SAGE_MAX_MEM%

REM new generation
if NOT "%SAGE_HEAP_NEWSIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Xmn%SAGE_HEAP_NEWSIZE%
)

REM max direct memory
if NOT "%SAGE_DIRECT_SIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=%SAGE_DIRECT_SIZE%
)

REM reduce the per-thread stack size
set JAVA_OPTS=%JAVA_OPTS% -Xss256k

REM set to headlSAGEs, just in case
set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headlSAGEs=true

REM Force the JVM to use IPv4 stack
if NOT "%SAGE_USE_IPV4%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Djava.net.preferIPv4Stack=true
)

set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly

REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark

if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimSAGEtamps
if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
if NOT "%SAGE_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -Xloggc:%SAGE_HOME%/logs/gc.log

REM CausSAGE the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$SAGE_HOME/logs/heapdump.hprof

REM DisablSAGE explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC

REM Ensure UTF-8 encoding by default (e.g. filenamSAGE)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

set SAGE_CLASSPATH=%SAGE_HOME%\conf\;%SAGE_HOME%\lib\*
SETLOCAL
TITLE SAGE-daemon

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %SAGE_JAVA_OPTS% %SAGE_PARAMS% %* -cp "%SAGE_CLASSPATH%" "com.haima.SAGE.bigdata.etl.daemon.SAGEDaemon"

ENDLOCAL
