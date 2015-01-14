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

echo "%*" | findstr " --help -h" >nul
if %ERRORLEVEL% equ 0 (
  call :usage
  exit /b 0
)

call %SPARK_HOME%\bin\windows-utils.cmd %*
if %ERRORLEVEL% equ 1 (
  call :usage
  exit /b 1
)

cmd /V /E /C %SPARK_HOME%\bin\spark-submit.cmd --class org.apache.spark.repl.Main %SUBMISSION_OPTS% spark-shell %APPLICATION_OPTS%

exit /b 0

:usage
echo "Usage: .\bin\spark-shell.cmd [options]" >&2
%SPARK_HOME%\bin\spark-submit --help 2>&1 | findstr /V "Usage" 1>&2
exit /b 0
