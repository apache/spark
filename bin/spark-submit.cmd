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

set SPARK_HOME=%~dp0..
set ORIG_ARGS=%*

rem Clear the values of all variables used
set DEPLOY_MODE=
set DRIVER_MEMORY=
set SPARK_SUBMIT_LIBRARY_PATH=
set SPARK_SUBMIT_CLASSPATH=
set SPARK_SUBMIT_OPTS=
set SPARK_DRIVER_MEMORY=

:loop
if [%1] == [] goto continue
  if [%1] == [--deploy-mode] (
    set DEPLOY_MODE=%2
  ) else if [%1] == [--driver-memory] (
    set DRIVER_MEMORY=%2
  ) else if [%1] == [--driver-library-path] (
    set SPARK_SUBMIT_LIBRARY_PATH=%2
  ) else if [%1] == [--driver-class-path] (
    set SPARK_SUBMIT_CLASSPATH=%2
  ) else if [%1] == [--driver-java-options] (
    set SPARK_SUBMIT_OPTS=%2
  )
  shift
goto loop
:continue

if [%DEPLOY_MODE%] == [] (
  set DEPLOY_MODE=client
)

if not [%DRIVER_MEMORY%] == [] if [%DEPLOY_MODE%] == [client] (
  set SPARK_DRIVER_MEMORY=%DRIVER_MEMORY%
)

cmd /V /E /C %SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.SparkSubmit %ORIG_ARGS%
