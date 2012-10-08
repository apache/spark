@echo off

set SCALA_VERSION=2.9.1

rem Figure out where the Spark framework is installed
set FWDIR=%~dp0

rem Export this as SPARK_HOME
set SPARK_HOME=%FWDIR%

rem Load environment variables from conf\spark-env.cmd, if it exists
if exist "%FWDIR%conf\spark-env.cmd" call "%FWDIR%conf\spark-env.cmd"

rem Check that SCALA_HOME has been specified
if not "x%SCALA_HOME%"=="x" goto scala_exists
  echo "SCALA_HOME is not set"
  goto exit
:scala_exists

rem If the user specifies a Mesos JAR, put it before our included one on the classpath
set MESOS_CLASSPATH=
if not "x%MESOS_JAR%"=="x" set MESOS_CLASSPATH=%MESOS_JAR%

rem Figure out how much memory to use per executor and set it as an environment
rem variable so that our process sees it and can report it to Mesos
if "x%SPARK_MEM%"=="x" set SPARK_MEM=512m

rem Set JAVA_OPTS to be able to load native libraries and to set heap size
set JAVA_OPTS=%SPARK_JAVA_OPTS% -Djava.library.path=%SPARK_LIBRARY_PATH% -Xms%SPARK_MEM% -Xmx%SPARK_MEM%
rem Load extra JAVA_OPTS from conf/java-opts, if it exists
if exist "%FWDIR%conf\java-opts.cmd" call "%FWDIR%conf\java-opts.cmd"

set CORE_DIR=%FWDIR%core
set REPL_DIR=%FWDIR%repl
set EXAMPLES_DIR=%FWDIR%examples
set BAGEL_DIR=%FWDIR%bagel

rem Build up classpath
set CLASSPATH=%SPARK_CLASSPATH%;%MESOS_CLASSPATH%;%FWDIR%conf;%CORE_DIR%\target\scala-%SCALA_VERSION%\classes
set CLASSPATH=%CLASSPATH%;%CORE_DIR%\target\scala-%SCALA_VERSION%\test-classes;%CORE_DIR%\src\main\resources
set CLASSPATH=%CLASSPATH%;%REPL_DIR%\target\scala-%SCALA_VERSION%\classes;%EXAMPLES_DIR%\target\scala-%SCALA_VERSION%\classes
for /R "%FWDIR%\lib_managed\jars" %%j in (*.jar) do set CLASSPATH=!CLASSPATH!;%%j
for /R "%FWDIR%\lib_managed\bundles" %%j in (*.jar) do set CLASSPATH=!CLASSPATH!;%%j
for /R "%REPL_DIR%\lib" %%j in (*.jar) do set CLASSPATH=!CLASSPATH!;%%j
set CLASSPATH=%CLASSPATH%;%BAGEL_DIR%\target\scala-%SCALA_VERSION%\classes

rem Figure out whether to run our class with java or with the scala launcher.
rem In most cases, we'd prefer to execute our process with java because scala
rem creates a shell script as the parent of its Java process, which makes it
rem hard to kill the child with stuff like Process.destroy(). However, for
rem the Spark shell, the wrapper is necessary to properly reset the terminal
rem when we exit, so we allow it to set a variable to launch with scala.
if "%SPARK_LAUNCH_WITH_SCALA%" NEQ 1 goto java_runner
  set RUNNER=%SCALA_HOME%\bin\scala
  # Java options will be passed to scala as JAVA_OPTS
  set EXTRA_ARGS=
  goto run_spark
:java_runner
  set CLASSPATH=%CLASSPATH%;%SCALA_HOME%\lib\scala-library.jar;%SCALA_HOME%\lib\scala-compiler.jar;%SCALA_HOME%\lib\jline.jar
  set RUNNER=java
  if not "x%JAVA_HOME%"=="x" set RUNNER=%JAVA_HOME%\bin\java
  rem The JVM doesn't read JAVA_OPTS by default so we need to pass it in
  set EXTRA_ARGS=%JAVA_OPTS%
:run_spark

%RUNNER% -cp "%CLASSPATH%" %EXTRA_ARGS% %*
:exit
