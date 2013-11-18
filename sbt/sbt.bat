@REM SBT launcher script
@REM 
@REM Envioronment:
@REM JAVA_HOME - location of a JDK home dir (mandatory)
@REM SBT_OPTS  - JVM options (optional)
@REM Configuration:
@REM sbtconfig.txt found in the SBT_HOME.

@REM   ZOMG! We need delayed expansion to build up CFG_OPTS later 
@setlocal enabledelayedexpansion

@echo off
set SBT_HOME=%~dp0
set ERROR_CODE=0

rem FIRST we load the config file of extra options.
set FN=%SBT_HOME%sbtconfig.txt
set CFG_OPTS=
FOR /F "tokens=* eol=# usebackq delims=" %%i IN ("%FN%") DO (
  set DO_NOT_REUSE_ME=%%i
  rem ZOMG (Part #2) WE use !! here to delay the expansion of
  rem CFG_OPTS, otherwise it remains "" for this loop.
  set CFG_OPTS=!CFG_OPTS! !DO_NOT_REUSE_ME!
)

rem We use the value of the JAVACMD environment variable if defined
set _JAVACMD=%JAVACMD%

if "%_JAVACMD%"=="" (
  if not "%JAVA_HOME%"=="" (
    if exist "%JAVA_HOME%\bin\java.exe" set "_JAVACMD=%JAVA_HOME%\bin\java.exe"
  )
)

if "%_JAVACMD%"=="" set _JAVACMD=java

rem We use the value of the JAVA_OPTS environment variable if defined, rather than the config.
set _JAVA_OPTS=%JAVA_OPTS%
if "%_JAVA_OPTS%"=="" set _JAVA_OPTS=%CFG_OPTS%

:run

"%_JAVACMD%" %_JAVA_OPTS% %SBT_OPTS% -cp "%SBT_HOME%jansi.jar;%SBT_HOME%sbt-launch.jar;%SBT_HOME%classes" SbtJansiLaunch %*
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end

@endlocal

exit /B %ERROR_CODE%
