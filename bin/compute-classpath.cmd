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

rem This script computes Spark's classpath and prints it to stdout; it's used by both the "run"
rem script and the ExecutorRunner in standalone cluster mode.

set SCALA_VERSION=2.10

rem Figure out where the Spark framework is installed
set FWDIR=%~dp0..\

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Build up classpath
set CLASSPATH=%FWDIR%conf
if exist "%FWDIR%RELEASE" (
  for %%d in ("%FWDIR%jars\spark-assembly*.jar") do (
    set ASSEMBLY_JAR=%%d
  )
) else (
  for %%d in ("%FWDIR%assembly\target\scala-%SCALA_VERSION%\spark-assembly*hadoop*.jar") do (
    set ASSEMBLY_JAR=%%d
  )
)

set CLASSPATH=%CLASSPATH%;%ASSEMBLY_JAR%

set SPARK_CLASSES=%FWDIR%core\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%repl\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%mllib\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%bagel\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%graphx\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%streaming\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%tools\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%sql\catalyst\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%sql\core\target\scala-%SCALA_VERSION%\classes
set SPARK_CLASSES=%SPARK_CLASSES%;%FWDIR%sql\hive\target\scala-%SCALA_VERSION%\classes

set SPARK_TEST_CLASSES=%FWDIR%core\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%repl\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%mllib\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%bagel\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%graphx\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%streaming\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%sql\catalyst\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%sql\core\target\scala-%SCALA_VERSION%\test-classes
set SPARK_TEST_CLASSES=%SPARK_TEST_CLASSES%;%FWDIR%sql\hive\target\scala-%SCALA_VERSION%\test-classes

if "x%SPARK_TESTING%"=="x1" (
  rem Add test clases to path - note, add SPARK_CLASSES and SPARK_TEST_CLASSES before CLASSPATH
  rem so that local compilation takes precedence over assembled jar
  set CLASSPATH=%SPARK_CLASSES%;%SPARK_TEST_CLASSES%;%CLASSPATH%
)

rem Add hadoop conf dir - else FileSystem.*, etc fail
rem Note, this assumes that there is either a HADOOP_CONF_DIR or YARN_CONF_DIR which hosts
rem the configurtion files.
if "x%HADOOP_CONF_DIR%"=="x" goto no_hadoop_conf_dir
  set CLASSPATH=%CLASSPATH%;%HADOOP_CONF_DIR%
:no_hadoop_conf_dir

if "x%YARN_CONF_DIR%"=="x" goto no_yarn_conf_dir
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%
:no_yarn_conf_dir

rem A bit of a hack to allow calling this script within run2.cmd without seeing output
if "%DONT_PRINT_CLASSPATH%"=="1" goto exit

echo %CLASSPATH%

:exit
