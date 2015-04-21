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

set SCALA_VERSION=2.10

rem Figure out where the Spark framework is installed
set FWDIR=%~dp0..\

rem Export this as SPARK_HOME
set SPARK_HOME=%FWDIR%

call %SPARK_HOME%\bin\load-spark-env.cmd

rem Test that an argument was given
if not "x%1"=="x" goto arg_given
  echo Usage: run-example ^<example-class^> [example-args]
  echo   - set MASTER=XX to use a specific master
  echo   - can use abbreviated example class name relative to com.apache.spark.examples
  echo      (e.g. SparkPi, mllib.LinearRegression, streaming.KinesisWordCountASL)
  goto exit
:arg_given

set EXAMPLES_DIR=%FWDIR%examples

rem Figure out the JAR file that our examples were packaged into.
set SPARK_EXAMPLES_JAR=
if exist "%FWDIR%RELEASE" (
  for %%d in ("%FWDIR%lib\spark-examples*.jar") do (
    set SPARK_EXAMPLES_JAR=%%d
  )
) else (
  for %%d in ("%EXAMPLES_DIR%\target\scala-%SCALA_VERSION%\spark-examples*.jar") do (
    set SPARK_EXAMPLES_JAR=%%d
  )
)
if "x%SPARK_EXAMPLES_JAR%"=="x" (
  echo Failed to find Spark examples assembly JAR.
  echo You need to build Spark before running this program.
  goto exit
)

rem Set master from MASTER environment variable if given
if "x%MASTER%"=="x" (
  set EXAMPLE_MASTER=local[*]
) else (
  set EXAMPLE_MASTER=%MASTER%
)

rem If the EXAMPLE_CLASS does not start with org.apache.spark.examples, add that
set EXAMPLE_CLASS=%1
set PREFIX=%EXAMPLE_CLASS:~0,25%
if not %PREFIX%==org.apache.spark.examples (
  set EXAMPLE_CLASS=org.apache.spark.examples.%EXAMPLE_CLASS%
)

rem Get the tail of the argument list, to skip the first one. This is surprisingly
rem complicated on Windows.
set "ARGS="
:top
shift
if "%~1" neq "" (
  set ARGS=%ARGS% "%~1"
  goto :top
)
if defined ARGS set ARGS=%ARGS:~1%

call "%FWDIR%bin\spark-submit.cmd" ^
  --master %EXAMPLE_MASTER% ^
  --class %EXAMPLE_CLASS% ^
  "%SPARK_EXAMPLES_JAR%" %ARGS%

:exit
