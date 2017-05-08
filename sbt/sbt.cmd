@echo off
powershell -ExecutionPolicy RemoteSigned -NoProfile -File "%~dp0\%~n0.ps1" %*
