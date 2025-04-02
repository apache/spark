@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem Figure out where the Spark framework is installed
call "%~dp0find-spark-home.cmd"

call "%SPARK_HOME%\bin\load-spark-env.cmd"

rem Test that an argument was given
if "x%1"=="x" (
  echo Usage: spark-class ^<class^> [^<args^>]
  exit /b 1
)

rem Find Spark jars.
if exist "%SPARK_HOME%\jars" (
  set SPARK_JARS_DIR=%SPARK_HOME%\jars
) else (
  set SPARK_JARS_DIR=%SPARK_HOME%\assembly\target\scala-%SPARK_SCALA_VERSION%\jars
)

if not exist "%SPARK_JARS_DIR%" (
  echo Failed to find Spark jars directory.
  echo You need to build Spark before running this program.
  exit /b 1
)

set LAUNCH_CLASSPATH=%SPARK_JARS_DIR%\*

rem Add the launcher build dir to the classpath if requested.
if not "x%SPARK_PREPEND_CLASSES%"=="x" (
  set LAUNCH_CLASSPATH="%SPARK_HOME%\launcher\target\scala-%SPARK_SCALA_VERSION%\classes;%LAUNCH_CLASSPATH%"
)

rem Figure out where java is.
set RUNNER=java
if not "x%JAVA_HOME%"=="x" (
  set RUNNER=%JAVA_HOME%\bin\java
) else (
  where /q "%RUNNER%"
  if ERRORLEVEL 1 (
    echo Java not found and JAVA_HOME environment variable is not set.
    echo Install Java and set JAVA_HOME to point to the Java installation directory.
    exit /b 1
  )
)

rem SPARK-23015: We create a temporary text file when launching Spark. 
rem This file must be given a unique name or else we risk a race condition when launching multiple instances close together.
rem The best way to create a unique file name is to add a GUID to the file name. Use Powershell to generate the GUID.
where powershell.exe >nul 2>&1
if %errorlevel%==0 (
  FOR /F %%a IN ('POWERSHELL -COMMAND "$([guid]::NewGuid().ToString())"') DO (set RANDOM_SUFFIX=%%a)
) else (
  rem If Powershell is not installed, try to create a random file name suffix using the Windows %RANDOM%.
  rem %RANDOM% is seeded with 1-second granularity so it is highly likely that two Spark instances
  rem launched within the same second will fail to start.
  rem Note that Powershell is automatically installed on all Windows OS from Windows 7/Windows Server 2008 R2 and onward.
  set RANDOM_SUFFIX=%RANDOM%
)

rem The launcher library prints the command to be executed in a single line suitable for being
rem executed by the batch interpreter. So read all the output of the launcher into a variable.
set LAUNCHER_OUTPUT=%temp%\spark-class-launcher-output-%RANDOM_SUFFIX%.txt

rem unset SHELL to indicate non-bash environment to launcher/Main
set SHELL=
"%RUNNER%" -Xmx128m -cp "%LAUNCH_CLASSPATH%" org.apache.spark.launcher.Main %* > %LAUNCHER_OUTPUT%
for /f "tokens=*" %%i in (%LAUNCHER_OUTPUT%) do (
  set SPARK_CMD=%%i
)
del %LAUNCHER_OUTPUT%
%SPARK_CMD%
