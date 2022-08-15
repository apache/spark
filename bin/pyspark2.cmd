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
set _SPARK_CMD_USAGE=Usage: bin\pyspark.cmd [options]

rem Figure out which Python to use.
if "x%PYSPARK_DRIVER_PYTHON%"=="x" (
  set PYSPARK_DRIVER_PYTHON=python
  if not [%PYSPARK_PYTHON%] == [] set PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%
)

set PYTHONPATH=%SPARK_HOME%\python;%PYTHONPATH%
set PYTHONPATH=%SPARK_HOME%\python\lib\py4j-0.10.9.7-src.zip;%PYTHONPATH%

set OLD_PYTHONSTARTUP=%PYTHONSTARTUP%
set PYTHONSTARTUP=%SPARK_HOME%\python\pyspark\shell.py

call "%SPARK_HOME%\bin\spark-submit2.cmd" pyspark-shell-main --name "PySparkShell" %*
