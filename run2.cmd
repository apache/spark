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

set SCALA_VERSION=2.9.3

rem Figure out where the Spark framework is installed
set FWDIR=%~dp0

rem Export this as SPARK_HOME
set SPARK_HOME=%FWDIR%

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Test that an argument was given
if not "x%1"=="x" goto arg_given
  echo Usage: run ^<spark-class^> [^<args^>]
  goto exit
:arg_given

set RUNNING_DAEMON=0
if "%1"=="spark.deploy.master.Master" set RUNNING_DAEMON=1
if "%1"=="spark.deploy.worker.Worker" set RUNNING_DAEMON=1
if "x%SPARK_DAEMON_MEMORY%" == "x" set SPARK_DAEMON_MEMORY=512m
set SPARK_DAEMON_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS% -Dspark.akka.logLifecycleEvents=true
if "%RUNNING_DAEMON%"=="1" set SPARK_MEM=%SPARK_DAEMON_MEMORY%
rem Do not overwrite SPARK_JAVA_OPTS environment variable in this script
if "%RUNNING_DAEMON%"=="0" set OUR_JAVA_OPTS=%SPARK_JAVA_OPTS%
if "%RUNNING_DAEMON%"=="1" set OUR_JAVA_OPTS=%SPARK_DAEMON_JAVA_OPTS%

rem Check that SCALA_HOME has been specified
if not "x%SCALA_HOME%"=="x" goto scala_exists
  echo SCALA_HOME is not set
  goto exit
:scala_exists

rem Figure out how much memory to use per executor and set it as an environment
rem variable so that our process sees it and can report it to Mesos
if "x%SPARK_MEM%"=="x" set SPARK_MEM=512m

rem Set JAVA_OPTS to be able to load native libraries and to set heap size
set JAVA_OPTS=%OUR_JAVA_OPTS% -Djava.library.path=%SPARK_LIBRARY_PATH% -Xms%SPARK_MEM% -Xmx%SPARK_MEM%
rem Attention: when changing the way the JAVA_OPTS are assembled, the change must be reflected in ExecutorRunner.scala!

set CORE_DIR=%FWDIR%core
set EXAMPLES_DIR=%FWDIR%examples
set REPL_DIR=%FWDIR%repl

rem Compute classpath using external script
set DONT_PRINT_CLASSPATH=1
call "%FWDIR%bin\compute-classpath.cmd"
set DONT_PRINT_CLASSPATH=0

rem Figure out the JAR file that our examples were packaged into.
rem First search in the build path from SBT:
for %%d in ("examples/target/scala-%SCALA_VERSION%/spark-examples*.jar") do (
  set SPARK_EXAMPLES_JAR=examples/target/scala-%SCALA_VERSION%/%%d
)
rem Then search in the build path from Maven:
for %%d in ("examples/target/spark-examples*hadoop*.jar") do (
  set SPARK_EXAMPLES_JAR=examples/target/%%d
)

rem Figure out whether to run our class with java or with the scala launcher.
rem In most cases, we'd prefer to execute our process with java because scala
rem creates a shell script as the parent of its Java process, which makes it
rem hard to kill the child with stuff like Process.destroy(). However, for
rem the Spark shell, the wrapper is necessary to properly reset the terminal
rem when we exit, so we allow it to set a variable to launch with scala.
if "%SPARK_LAUNCH_WITH_SCALA%" NEQ 1 goto java_runner
  set RUNNER=%SCALA_HOME%\bin\scala
  # Java options will be passed to scala as JAVA_OPTS
  set EXTRA_ARGS=
  goto run_spark
:java_runner
  set CLASSPATH=%CLASSPATH%;%SCALA_HOME%\lib\scala-library.jar;%SCALA_HOME%\lib\scala-compiler.jar;%SCALA_HOME%\lib\jline.jar
  set RUNNER=java
  if not "x%JAVA_HOME%"=="x" set RUNNER=%JAVA_HOME%\bin\java
  rem The JVM doesn't read JAVA_OPTS by default so we need to pass it in
  set EXTRA_ARGS=%JAVA_OPTS%
:run_spark

"%RUNNER%" -cp "%CLASSPATH%" %EXTRA_ARGS% %*
:exit
