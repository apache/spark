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

rem Gather all spark-submit options into SUBMISSION_OPTS

set SUBMISSION_OPTS=
set APPLICATION_OPTS=

rem NOTE: If you add or remove spark-sumbmit options,
rem modify NOT ONLY this script but also SparkSubmitArgument.scala

:OptsLoop
if "x%1"=="x" (
  goto :OptsLoopEnd
)

SET opts="\<--master\> \<--deploy-mode\> \<--class\> \<--name\> \<--jars\> \<--py-files\> \<--files\>"
SET opts="%opts:~1,-1% \<--conf\> \<--properties-file\> \<--driver-memory\> \<--driver-java-options\>"
SET opts="%opts:~1,-1% \<--driver-library-path\> \<--driver-class-path\> \<--executor-memory\>"
SET opts="%opts:~1,-1% \<--driver-cores\> \<--total-executor-cores\> \<--executor-cores\> \<--queue\>"
SET opts="%opts:~1,-1% \<--num-executors\> \<--archives\>"

echo %1 | findstr %opts% >nul
if %ERRORLEVEL% equ 0 (
  if "x%2"=="x" (
    echo "%1" requires an argument. >&2
    exit /b 1
  )
  set SUBMISSION_OPTS=%SUBMISSION_OPTS% %1 %2
  shift
  shift
  goto :OptsLoop
)
echo %1 | findstr "\<--verbose\> \<-v\> \<--supervise\>" >nul
if %ERRORLEVEL% equ 0 (
  set SUBMISSION_OPTS=%SUBMISSION_OPTS% %1
  shift
  goto :OptsLoop
)
set APPLICATION_OPTS=%APPLICATION_OPTS% %1
shift
goto :OptsLoop

:OptsLoopEnd
exit /b 0
