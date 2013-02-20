@echo off
set EXTRA_ARGS=
if not "%MESOS_HOME%x"=="x" set EXTRA_ARGS=-Djava.library.path=%MESOS_HOME%\lib\java
set SPARK_HOME=%~dp0..
java -Xmx1200M -XX:MaxPermSize=200m %EXTRA_ARGS% -jar %SPARK_HOME%\sbt\sbt-launch-0.11.3-2.jar "%*"
