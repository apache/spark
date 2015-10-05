@echo off

rem ##########################################################################
rem # Copyright 2002-2013 LAMP/EPFL
rem #
rem # This is free software; see the distribution for copying conditions.
rem # There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A
rem # PARTICULAR PURPOSE.
rem ##########################################################################

setlocal enableextensions enabledelayedexpansion

set _LINE_TOOLCP=

rem Use "%~1" to handle spaces in paths. See http://ss64.com/nt/syntax-args.html
rem SI-7295 The goto here is needed to avoid problems with `scala Script.cmd "arg(with)paren"`,
rem         we must not evaluate %~2 eagerly, but delayed expansion doesn't seem to allow
rem         removal of quotation marks.
if not [%~1]==[-toolcp] (
  goto :notoolcp
)
shift
set _LINE_TOOLCP=%~1
shift

:notoolcp

rem We keep in _JAVA_PARAMS all -J-prefixed and -D-prefixed arguments
set _JAVA_PARAMS=

if [%1]==[] goto param_afterloop
set _TEST_PARAM=%~1
if not "%_TEST_PARAM:~0,1%"=="-" goto param_afterloop

rem ignore -e "scala code"
if "%_TEST_PARAM:~0,2%"=="-e" (
  shift
  shift
  if [%1]==[] goto param_afterloop
)

set _TEST_PARAM=%~1
if "%_TEST_PARAM:~0,2%"=="-J" (
  set _JAVA_PARAMS=%_TEST_PARAM:~2%
)

if "%_TEST_PARAM:~0,2%"=="-D" (
  rem test if this was double-quoted property "-Dprop=42"
  for /F "delims== tokens=1-2" %%G in ("%_TEST_PARAM%") DO (
    if not "%%G" == "%_TEST_PARAM%" (
      rem double quoted: "-Dprop=42" -> -Dprop="42"
      set _JAVA_PARAMS=%%G="%%H"
    ) else if [%2] neq [] (
      rem it was a normal property: -Dprop=42 or -Drop="42"
      set _JAVA_PARAMS=%_TEST_PARAM%=%2
      shift
    )
  )
)

:param_loop
shift

if [%1]==[] goto param_afterloop
set _TEST_PARAM=%~1
if not "%_TEST_PARAM:~0,1%"=="-" goto param_afterloop 

rem ignore -e "scala code"
if "%_TEST_PARAM:~0,2%"=="-e" (
  shift
  shift
  if [%1]==[] goto param_afterloop
)

set _TEST_PARAM=%~1
if "%_TEST_PARAM:~0,2%"=="-J" (
  set _JAVA_PARAMS=%_JAVA_PARAMS% %_TEST_PARAM:~2%
)

if "%_TEST_PARAM:~0,2%"=="-D" (
  rem test if this was double-quoted property "-Dprop=42"
  for /F "delims== tokens=1-2" %%G in ("%_TEST_PARAM%") DO (
    if not "%%G" == "%_TEST_PARAM%" (
      rem double quoted: "-Dprop=42" -> -Dprop="42"
      set _JAVA_PARAMS=%_JAVA_PARAMS% %%G="%%H"
    ) else if [%2] neq [] (
      rem it was a normal property: -Dprop=42 or -Drop="42"
      set _JAVA_PARAMS=%_JAVA_PARAMS% %_TEST_PARAM%=%2
      shift
    )
  )
)
goto param_loop
:param_afterloop

if "%OS%" NEQ "Windows_NT" (
  echo "Warning, your version of Windows is not supported.  Attempting to start scala anyway."
)

@setlocal
call :set_home

rem We use the value of the JAVACMD environment variable if defined
set _JAVACMD=%JAVACMD%

if not defined _JAVACMD (
  if not "%JAVA_HOME%"=="" (
    if exist "%JAVA_HOME%\bin\java.exe" set "_JAVACMD=%JAVA_HOME%\bin\java.exe"
  )
)

if "%_JAVACMD%"=="" set _JAVACMD=java

rem We use the value of the JAVA_OPTS environment variable if defined
set _JAVA_OPTS=%JAVA_OPTS%
if not defined _JAVA_OPTS set _JAVA_OPTS=-Xmx256M -Xms32M

rem We append _JAVA_PARAMS java arguments to JAVA_OPTS if necessary
if defined _JAVA_PARAMS set _JAVA_OPTS=%_JAVA_OPTS% %_JAVA_PARAMS%

set _TOOL_CLASSPATH=
if "%_TOOL_CLASSPATH%"=="" (
  for %%f in ("!_SCALA_HOME!\lib\*") do call :add_cpath "%%f"
  for /d %%f in ("!_SCALA_HOME!\lib\*") do call :add_cpath "%%f"
)

if not "%_LINE_TOOLCP%"=="" call :add_cpath "%_LINE_TOOLCP%"

set _PROPS=-Dscala.home="!_SCALA_HOME!" -Denv.emacs="%EMACS%" -Dscala.usejavacp=true 

rem echo "%_JAVACMD%" %_JAVA_OPTS% %_PROPS% -cp "%_TOOL_CLASSPATH%" scala.tools.nsc.Main  %*
"%_JAVACMD%" %_JAVA_OPTS% %_PROPS% -cp "%_TOOL_CLASSPATH%" scala.tools.nsc.Main  %*
goto end

rem ##########################################################################
rem # subroutines

:add_cpath
  if "%_TOOL_CLASSPATH%"=="" (
    set _TOOL_CLASSPATH=%~1
  ) else (
    set _TOOL_CLASSPATH=%_TOOL_CLASSPATH%;%~1
  )
goto :eof

rem Variable "%~dps0" works on WinXP SP2 or newer
rem (see http://support.microsoft.com/?kbid=833431)
rem set _SCALA_HOME=%~dps0..
:set_home
  set _BIN_DIR=
  for %%i in (%~sf0) do set _BIN_DIR=%_BIN_DIR%%%~dpsi
  set _SCALA_HOME=%_BIN_DIR%..
goto :eof

:end
@endlocal

REM exit code fix, see http://stackoverflow.com/questions/4632891/exiting-batch-with-exit-b-x-where-x-1-acts-as-if-command-completed-successfu
@"%COMSPEC%" /C exit %errorlevel% >nul
