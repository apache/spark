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

setlocal enabledelayedexpansion

set SCALA_VERSION=2.10

rem Figure out where the Spark framework is installed
set FWDIR=%~dp0..\

rem Export this as SPARK_HOME
set SPARK_HOME=%FWDIR%

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Test that an argument was given
if not "x%1"=="x" goto arg_given
  echo Usage: spark-class ^<class^> [^<args^>]
  goto exit
:arg_given

if not "x%SPARK_MEM%"=="x" (
  echo Warning: SPARK_MEM is deprecated, please use a more specific config option
  echo e.g., spark.executor.memory or SPARK_DRIVER_MEMORY.
)

rem Use SPARK_MEM or 512m as the default memory, to be overridden by specific options
set OUR_JAVA_MEM=%SPARK_MEM%
if "x%OUR_JAVA_MEM%"=="x" set OUR_JAVA_MEM=512m

set SPARK_DAEMON_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS% -Dspark.akka.logLifecycleEvents=true

rem Add java opts and memory settings for master, worker, history server, executors, and repl.
rem Master, Worker and HistoryServer use SPARK_DAEMON_JAVA_OPTS (and specific opts) + SPARK_DAEMON_MEMORY.
if "%1"=="org.apache.spark.deploy.master.Master" (
  set OUR_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS% %SPARK_MASTER_OPTS%
  if not "x%SPARK_DAEMON_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_DAEMON_MEMORY%
) else if "%1"=="org.apache.spark.deploy.worker.Worker" (
  set OUR_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS% %SPARK_WORKER_OPTS%
  if not "x%SPARK_DAEMON_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_DAEMON_MEMORY%
) else if "%1"=="org.apache.spark.deploy.history.HistoryServer" (
  set OUR_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS% %SPARK_HISTORY_OPTS%
  if not "x%SPARK_DAEMON_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_DAEMON_MEMORY%

rem Executors use SPARK_JAVA_OPTS + SPARK_EXECUTOR_MEMORY.
) else if "%1"=="org.apache.spark.executor.CoarseGrainedExecutorBackend" (
  set OUR_JAVA_OPTS=%SPARK_JAVA_OPTS% %SPARK_EXECUTOR_OPTS%
  if not "x%SPARK_EXECUTOR_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_EXECUTOR_MEMORY%
) else if "%1"=="org.apache.spark.executor.MesosExecutorBackend" (
  set OUR_JAVA_OPTS=%SPARK_JAVA_OPTS% %SPARK_EXECUTOR_OPTS%
  if not "x%SPARK_EXECUTOR_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_EXECUTOR_MEMORY%

rem All drivers use SPARK_JAVA_OPTS + SPARK_DRIVER_MEMORY. The repl also uses SPARK_REPL_OPTS.
) else if "%1"=="org.apache.spark.repl.Main" (
  set OUR_JAVA_OPTS=%SPARK_JAVA_OPTS% %SPARK_REPL_OPTS%
  if not "x%SPARK_DRIVER_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_DRIVER_MEMORY%
) else (
  set OUR_JAVA_OPTS=%SPARK_JAVA_OPTS%
  if not "x%SPARK_DRIVER_MEMORY%"=="x" set OUR_JAVA_MEM=%SPARK_DRIVER_MEMORY%
)

rem Set JAVA_OPTS to be able to load native libraries and to set heap size
for /f "tokens=3" %%i in ('java -version 2^>^&1 ^| find "version"') do set jversion=%%i
for /f "tokens=1 delims=_" %%i in ("%jversion:~1,-1%") do set jversion=%%i
if "%jversion%" geq "1.8.0" (
  set JAVA_OPTS=%OUR_JAVA_OPTS% -Djava.library.path=%SPARK_LIBRARY_PATH% -Xms%OUR_JAVA_MEM% -Xmx%OUR_JAVA_MEM%
) else (
  set JAVA_OPTS=-XX:MaxPermSize=128m %OUR_JAVA_OPTS% -Djava.library.path=%SPARK_LIBRARY_PATH% -Xms%OUR_JAVA_MEM% -Xmx%OUR_JAVA_MEM%
)
rem Attention: when changing the way the JAVA_OPTS are assembled, the change must be reflected in CommandUtils.scala!

rem Test whether the user has built Spark
if exist "%FWDIR%RELEASE" goto skip_build_test
set FOUND_JAR=0
for %%d in ("%FWDIR%assembly\target\scala-%SCALA_VERSION%\spark-assembly*hadoop*.jar") do (
  set FOUND_JAR=1
)
if "%FOUND_JAR%"=="0" (
  echo Failed to find Spark assembly JAR.
  echo You need to build Spark with sbt\sbt assembly before running this program.
  goto exit
)
:skip_build_test

set TOOLS_DIR=%FWDIR%tools
set SPARK_TOOLS_JAR=
for %%d in ("%TOOLS_DIR%\target\scala-%SCALA_VERSION%\spark-tools*assembly*.jar") do (
  set SPARK_TOOLS_JAR=%%d
)

rem Compute classpath using external script
set DONT_PRINT_CLASSPATH=1
call "%FWDIR%bin\compute-classpath.cmd"
set DONT_PRINT_CLASSPATH=0
set CLASSPATH=%CLASSPATH%;%SPARK_TOOLS_JAR%

rem Figure out where java is.
set RUNNER=java
if not "x%JAVA_HOME%"=="x" set RUNNER=%JAVA_HOME%\bin\java

"%RUNNER%" -cp "%CLASSPATH%" %JAVA_OPTS% %*
:exit
