@echo off

rem Install development version of SparkR
rem

MKDIR .\lib

R.exe CMD INSTALL --library=".\lib" pkg\
