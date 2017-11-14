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

rem This script loads spark-env.cmd if it exists, and ensures it is only loaded once.
rem spark-env.cmd is loaded from SPARK_CONF_DIR if set, or within the current directory's
rem conf\ subdirectory.

if [%SPARK_ENV_LOADED%] == [] (
  set SPARK_ENV_LOADED=1

  if [%SPARK_CONF_DIR%] == [] (
    set SPARK_CONF_DIR=%~dp0..\conf
  )

  call :LoadSparkEnv
)

rem Setting SPARK_SCALA_VERSION if not already set.

set ASSEMBLY_DIR2="%SPARK_HOME%\assembly\target\scala-2.11"
set ASSEMBLY_DIR1="%SPARK_HOME%\assembly\target\scala-2.12"

if [%SPARK_SCALA_VERSION%] == [] (

  if exist %ASSEMBLY_DIR2% if exist %ASSEMBLY_DIR1% (
    echo "Presence of build for multiple Scala versions detected."
    echo "Either clean one of them or, set SPARK_SCALA_VERSION in spark-env.cmd."
    exit 1
  )
  if exist %ASSEMBLY_DIR2% (
    set SPARK_SCALA_VERSION=2.11
  ) else (
    set SPARK_SCALA_VERSION=2.12
  )
)
exit /b 0

:LoadSparkEnv
if exist "%SPARK_CONF_DIR%\spark-env.cmd" (
  call "%SPARK_CONF_DIR%\spark-env.cmd"
)
