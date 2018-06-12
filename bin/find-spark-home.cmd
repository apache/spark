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

rem Path to Python script finding SPARK_HOME
set FIND_SPARK_HOME_PYTHON_SCRIPT=%~dp0find_spark_home.py

rem Default to standard python interpreter unless told otherwise
set PYTHON_RUNNER=python
rem If PYSPARK_DRIVER_PYTHON is set, it overwrites the python version
if not "x%PYSPARK_DRIVER_PYTHON%"=="x" (
  set PYTHON_RUNNER=%PYSPARK_DRIVER_PYTHON%
)
rem If PYSPARK_PYTHON is set, it overwrites the python version
if not "x%PYSPARK_PYTHON%"=="x" (
  set PYTHON_RUNNER=%PYSPARK_PYTHON%
)

rem If there is python installed, trying to use the root dir as SPARK_HOME
where %PYTHON_RUNNER% > nul 2>&1
if %ERRORLEVEL% neq 0 (
  if not exist %PYTHON_RUNNER% (
    if "x%SPARK_HOME%"=="x" (
      echo Missing Python executable '%PYTHON_RUNNER%', defaulting to '%~dp0..' for SPARK_HOME ^
environment variable. Please install Python or specify the correct Python executable in ^
PYSPARK_DRIVER_PYTHON or PYSPARK_PYTHON environment variable to detect SPARK_HOME safely.
      set SPARK_HOME=%~dp0..
    )
  )
)

rem Only attempt to find SPARK_HOME if it is not set.
if "x%SPARK_HOME%"=="x" (
  if not exist "%FIND_SPARK_HOME_PYTHON_SCRIPT%" (
    rem If we are not in the same directory as find_spark_home.py we are not pip installed so we don't
    rem need to search the different Python directories for a Spark installation.
    rem Note only that, if the user has pip installed PySpark but is directly calling pyspark-shell or
    rem spark-submit in another directory we want to use that version of PySpark rather than the
    rem pip installed version of PySpark.
    set SPARK_HOME=%~dp0..
  ) else (
    rem We are pip installed, use the Python script to resolve a reasonable SPARK_HOME
    for /f "delims=" %%i in ('%PYTHON_RUNNER% %FIND_SPARK_HOME_PYTHON_SCRIPT%') do set SPARK_HOME=%%i
  )
)
