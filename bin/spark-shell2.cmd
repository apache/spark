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
set _SPARK_CMD_USAGE=Usage: .\bin\spark-shell.cmd [options]

rem SPARK-4161: scala does not assume use of the java classpath,
rem so we need to add the "-Dscala.usejavacp=true" flag manually. We
rem do this specifically for the Spark shell because the scala REPL
rem has its own class loader, and any additional classpath specified
rem through spark.driver.extraClassPath is not automatically propagated.
if "x%SPARK_SUBMIT_OPTS%"=="x" (
  set SPARK_SUBMIT_OPTS=-Dscala.usejavacp=true
  goto run_shell
)
set SPARK_SUBMIT_OPTS="%SPARK_SUBMIT_OPTS% -Dscala.usejavacp=true"

:run_shell
%SPARK_HOME%\bin\spark-submit2.cmd --class org.apache.spark.repl.Main --name "Spark shell" %*
