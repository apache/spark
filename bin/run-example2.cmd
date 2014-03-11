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

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Test that an argument was given
if not "x%1"=="x" goto arg_given
  echo Usage: run-example ^<example-class^> [^<args^>]
  goto exit
:arg_given

set EXAMPLES_DIR=%FWDIR%examples

rem Figure out the JAR file that our examples were packaged into.
set SPARK_EXAMPLES_JAR=
for %%d in ("%EXAMPLES_DIR%\target\scala-%SCALA_VERSION%\spark-examples*assembly*.jar") do (
  set SPARK_EXAMPLES_JAR=%%d
)
if "x%SPARK_EXAMPLES_JAR%"=="x" (
  echo Failed to find Spark examples assembly JAR.
  echo You need to build Spark with sbt\sbt assembly before running this program.
  goto exit
)

rem Compute Spark classpath using external script
set DONT_PRINT_CLASSPATH=1
call "%FWDIR%bin\compute-classpath.cmd"
set DONT_PRINT_CLASSPATH=0
set CLASSPATH=%SPARK_EXAMPLES_JAR%;%CLASSPATH%

rem Figure out where java is.
set RUNNER=java
if not "x%JAVA_HOME%"=="x" set RUNNER=%JAVA_HOME%\bin\java

"%RUNNER%" -cp "%CLASSPATH%" %JAVA_OPTS% %*
:exit
