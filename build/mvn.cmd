@echo off
set _DIR=%~dp0
set ZINC_PORT=3030
bash %_DIR%installdep.sh
set _CALLING_DIR="%_DIR%..\"
set _COMPILE_JVM_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"


if defined MAVEN_OPTS (
    echo %MAVEN_OPTS%
) else (
    MAVEN_OPTS=%_COMPILE_JVM_OPTS%
)

where /q mvn
IF ERRORLEVEL 1 (
    set MAVEN_BIN="%_DIR%\apache-maven-3.3.3\bin\mvn"
) ELSE (
    set MAVEN_BIN="mvn"
)

ECHO "using maven from %MAVEN_BIN%"

%MAVEN_BIN% -DzincPort=%ZINC_PORT% %*
