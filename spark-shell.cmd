@echo off
set FWDIR=%~dp0
set SPARK_LAUNCH_WITH_SCALA=1
cmd /V /E /C %FWDIR%run2.cmd spark.repl.Main %*
