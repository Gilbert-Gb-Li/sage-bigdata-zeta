@ECHO OFF
SET SCRIPT_DIR=%~dp0
FOR %%I IN ("%SCRIPT_DIR%..") DO SET PLUGIN_DIR=%%~dpfI\
ECHO "PLUGIN_DIR==>>%PLUGIN_DIR%"
FOR %%I IN ("%PLUGIN_DIR%..") DO SET SAGE_HOME=%%~dpfI\
ECHO "SAGE_HOME==>>%SAGE_HOME%"
ECHO copy jars
FOR /F %%i IN ('DIR %PLUGIN_DIR%lib /S /B /L /A-D') DO (
  ECHO copy file %%i to directory %SAGE_HOME%lib\
  COPY /Y "%%i" "%SAGE_HOME%lib\"
)
ECHO copy conf files
FOR /F %%i IN ('DIR %PLUGIN_DIR%conf /S /B /L /A-D') DO (
  ECHO copy file %%i to directory %SAGE_HOME%conf\
  COPY /Y "%%i" "%SAGE_HOME%conf\"
)

ECHO Plugin install successed....
PAUSE
