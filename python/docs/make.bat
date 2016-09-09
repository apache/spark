@ECHO OFF

rem This is the entry point for running Sphinx documentation. To avoid polluting the
rem environment, it just launches a new cmd to do the real work.

cmd /V /E /C %~dp0make2.bat %*
