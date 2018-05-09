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

rem Figure out where java is.
set RUNNER=java
if not "x%JAVA_HOME%"=="x" (
  set RUNNER=%JAVA_HOME%\bin\java
) else (
  where /q "%RUNNER%"
  if ERRORLEVEL 1 (
    echo Java not found and JAVA_HOME environment variable is not set.
    echo Install Java and set JAVA_HOME to point to the Java installation directory.
    exit /b 1
  )
)

for /f "tokens=3" %%g in ('%RUNNER% -version 2^>^&1 ^| findstr /i "version"') do ( @echo %%~g )
