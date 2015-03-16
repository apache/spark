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
set SPARK_HOME=%~dp0..

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%SPARK_HOME%\conf\spark-env.cmd" call "%SPARK_HOME%\conf\spark-env.cmd"

rem Test that an argument was given
if "x%1"=="x" (
  echo Usage: spark-class ^<class^> [^<args^>]
  exit /b 1
)

set LAUNCHER_CP=0
if exist %SPARK_HOME%\RELEASE goto find_release_launcher

rem Look for the Spark launcher in both Scala build directories. The launcher doesn't use Scala so
rem it doesn't really matter which one is picked up. Add the compiled classes directly to the
rem classpath instead of looking for a jar file, since it's very common for people using sbt to use
rem the "assembly" target instead of "package".
set LAUNCHER_CLASSES=%SPARK_HOME%\launcher\target\scala-2.10\classes
if exist %LAUNCHER_CLASSES% (
  set LAUNCHER_CP=%LAUNCHER_CLASSES%
)
set LAUNCHER_CLASSES=%SPARK_HOME%\launcher\target\scala-2.11\classes
if exist %LAUNCHER_CLASSES% (
  set LAUNCHER_CP=%LAUNCHER_CLASSES%
)
goto check_launcher

:find_release_launcher
for %%d in (%SPARK_HOME%\lib\spark-launcher*.jar) do (
  set LAUNCHER_CP=%%d
)

:check_launcher
if "%LAUNCHER_CP%"=="0" (
  echo Failed to find Spark launcher JAR.
  echo You need to build Spark before running this program.
  exit /b 1
)

rem Figure out where java is.
set RUNNER=java
if not "x%JAVA_HOME%"=="x" set RUNNER=%JAVA_HOME%\bin\java

rem The launcher library prints the command to be executed in a single line suitable for being
rem executed by the batch interpreter. So read all the output of the launcher into a variable.
for /f "tokens=*" %%i in ('cmd /C ""%RUNNER%" -cp %LAUNCHER_CP% org.apache.spark.launcher.Main %*"') do (
  set SPARK_CMD=%%i
)
%SPARK_CMD%
