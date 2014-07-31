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
set FWDIR=%~dp0..\..\..\

rem Export this as SPARK_HOME
set SPARK_HOME=%FWDIR%

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Test that an argument was given
if not "x%1"=="x" goto arg_given
  echo Usage: SPARK_HOME/extras/kinesis-asl/bin run-kinesis-example ^<example-class^> [example-args]
  echo   - set MASTER=XX to use a specific master
  echo   - can use abbreviated example class name (e.g. KinesisWordCount, JavaKinesisWordCount)
  echo "  - must set AWS_ACCESS_KEY_ID and AWS_SECRET_KEY env variables" 1>&2

  goto exit
:arg_given

set KINESIS_EXAMPLES_DIR=%FWDIR%extras\kinesis-asl

rem Figure out the JAR file that our examples were packaged into.
set SPARK_KINESIS_EXAMPLES_JAR=
if exist "%FWDIR%RELEASE" (
  for %%d in ("%FWDIR%lib\kinesis-asl*.jar") do (
    set SPARK_KINESIS_EXAMPLES_JAR=%%d
  )
) else (
  for %%d in ("%KINESIS_EXAMPLES_DIR%\target\kinesis-asl*.jar") do (
    set SPARK_KINESIS_EXAMPLES_JAR=%%d
  )
)
if "x%SPARK_KINESIS_EXAMPLES_JAR%"=="x" (
  echo Failed to find Spark Kinesis examples assembly JAR.
  echo You need to build Spark with maven using 'mvn -Pkinesis-asl package' before running this program.
  goto exit
)

rem Set master from MASTER environment variable if given
if "x%MASTER%"=="x" (
  set EXAMPLE_MASTER=local[*]
) else (
  set EXAMPLE_MASTER=%MASTER%
)

rem If the EXAMPLE_CLASS does not start with org.apache.spark.examples.streaming, add that
set EXAMPLE_CLASS=%1
set PREFIX=%EXAMPLE_CLASS:~0,25%
if not %PREFIX%==org.apache.spark.examples.streaming (
  set EXAMPLE_CLASS=org.apache.spark.examples.streaming.%EXAMPLE_CLASS%
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
  "%SPARK_KINESIS_EXAMPLES_JAR%" %ARGS%

:exit
