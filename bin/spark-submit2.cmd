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

rem NOTE: Any changes in this file must be reflected in SparkSubmitDriverBootstrapper.scala!

set SPARK_HOME=%~dp0..
set ORIG_ARGS=%*

rem Reset the values of all variables used
set SPARK_SUBMIT_DEPLOY_MODE=client
set SPARK_SUBMIT_PROPERTIES_FILE=%SPARK_HOME%\conf\spark-defaults.conf
set SPARK_SUBMIT_DRIVER_MEMORY=
set SPARK_SUBMIT_LIBRARY_PATH=
set SPARK_SUBMIT_CLASSPATH=
set SPARK_SUBMIT_OPTS=
set SPARK_SUBMIT_BOOTSTRAP_DRIVER=

:loop
if [%1] == [] goto continue
  if [%1] == [--deploy-mode] (
    set SPARK_SUBMIT_DEPLOY_MODE=%2
  ) else if [%1] == [--properties-file] (
    set SPARK_SUBMIT_PROPERTIES_FILE=%2
  ) else if [%1] == [--driver-memory] (
    set SPARK_SUBMIT_DRIVER_MEMORY=%2
  ) else if [%1] == [--driver-library-path] (
    set SPARK_SUBMIT_LIBRARY_PATH=%2
  ) else if [%1] == [--driver-class-path] (
    set SPARK_SUBMIT_CLASSPATH=%2
  ) else if [%1] == [--driver-java-options] (
    set SPARK_SUBMIT_OPTS=%2
  ) else if [%1] == [--master] (
    set MASTER=%2
  )
  shift
goto loop
:continue

if [%MASTER%] == [yarn-cluster] (
  set SPARK_SUBMIT_DEPLOY_MODE=cluster
)

rem For client mode, the driver will be launched in the same JVM that launches
rem SparkSubmit, so we may need to read the properties file for any extra class
rem paths, library paths, java options and memory early on. Otherwise, it will
rem be too late by the time the driver JVM has started.

if [%SPARK_SUBMIT_DEPLOY_MODE%] == [client] (
  if exist %SPARK_SUBMIT_PROPERTIES_FILE% (
    rem Parse the properties file only if the special configs exist
    for /f %%i in ('findstr /r /c:"^[\t ]*spark.driver.memory" /c:"^[\t ]*spark.driver.extra" ^
      %SPARK_SUBMIT_PROPERTIES_FILE%') do (
      set SPARK_SUBMIT_BOOTSTRAP_DRIVER=1
    )
  )
)

cmd /V /E /C %SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.SparkSubmit %ORIG_ARGS%
