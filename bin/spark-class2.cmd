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
  echo Usage: spark-class ^<class^> [^<args^>]
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

rem Figure out how much memory to use per executor and set it as an environment
rem variable so that our process sees it and can report it to Mesos
if "x%SPARK_MEM%"=="x" set SPARK_MEM=512m

rem Set JAVA_OPTS to be able to load native libraries and to set heap size
set JAVA_OPTS=%OUR_JAVA_OPTS% -Djava.library.path=%SPARK_LIBRARY_PATH% -Xms%SPARK_MEM% -Xmx%SPARK_MEM%
rem Attention: when changing the way the JAVA_OPTS are assembled, the change must be reflected in ExecutorRunner.scala!

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
