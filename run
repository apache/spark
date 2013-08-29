#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCALA_VERSION=2.9.3

# Figure out where the Scala framework is installed
FWDIR="$(cd `dirname $0`; pwd)"

# Export this as SPARK_HOME
export SPARK_HOME="$FWDIR"

# Load environment variables from conf/spark-env.sh, if it exists
if [ -e $FWDIR/conf/spark-env.sh ] ; then
  . $FWDIR/conf/spark-env.sh
fi

if [ -z "$1" ]; then
  echo "Usage: run <spark-class> [<args>]" >&2
  exit 1
fi

# If this is a standalone cluster daemon, reset SPARK_JAVA_OPTS and SPARK_MEM to reasonable
# values for that; it doesn't need a lot
if [ "$1" = "spark.deploy.master.Master" -o "$1" = "spark.deploy.worker.Worker" ]; then
  SPARK_MEM=${SPARK_DAEMON_MEMORY:-512m}
  SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Dspark.akka.logLifecycleEvents=true"
  # Do not overwrite SPARK_JAVA_OPTS environment variable in this script
  OUR_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS"   # Empty by default
else
  OUR_JAVA_OPTS="$SPARK_JAVA_OPTS"
fi


# Add java opts for master, worker, executor. The opts maybe null
case "$1" in
  'spark.deploy.master.Master')
    OUR_JAVA_OPTS="$OUR_JAVA_OPTS $SPARK_MASTER_OPTS"
    ;;
  'spark.deploy.worker.Worker')
    OUR_JAVA_OPTS="$OUR_JAVA_OPTS $SPARK_WORKER_OPTS"
    ;;
  'spark.executor.StandaloneExecutorBackend')
    OUR_JAVA_OPTS="$OUR_JAVA_OPTS $SPARK_EXECUTOR_OPTS"
    ;;
  'spark.executor.MesosExecutorBackend')
    OUR_JAVA_OPTS="$OUR_JAVA_OPTS $SPARK_EXECUTOR_OPTS"
    ;;
  'spark.repl.Main')
    OUR_JAVA_OPTS="$OUR_JAVA_OPTS $SPARK_REPL_OPTS"
    ;;
esac

# Figure out whether to run our class with java or with the scala launcher.
# In most cases, we'd prefer to execute our process with java because scala
# creates a shell script as the parent of its Java process, which makes it
# hard to kill the child with stuff like Process.destroy(). However, for
# the Spark shell, the wrapper is necessary to properly reset the terminal
# when we exit, so we allow it to set a variable to launch with scala.
# We still fall back on java for the shell if this is a "release" created
# from make-distribution.sh since it's possible scala is not installed
# but we have everything we need to run the shell.
if [[ "$SPARK_LAUNCH_WITH_SCALA" == "1" && ! -f "$FWDIR/RELEASE" ]]; then
  if [ "$SCALA_HOME" ]; then
    RUNNER="${SCALA_HOME}/bin/scala"
  else
    if [ `command -v scala` ]; then
      RUNNER="scala"
    else
      echo "SCALA_HOME is not set and scala is not in PATH" >&2
      exit 1
    fi
  fi
else
  if [ -n "${JAVA_HOME}" ]; then
    RUNNER="${JAVA_HOME}/bin/java"
  else
    if [ `command -v java` ]; then
      RUNNER="java"
    else
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
  fi
  if [[ ! -f "$FWDIR/RELEASE" && -z "$SCALA_LIBRARY_PATH" ]]; then
    if [ -z "$SCALA_HOME" ]; then
      echo "SCALA_HOME is not set" >&2
      exit 1
    fi
    SCALA_LIBRARY_PATH="$SCALA_HOME/lib"
  fi
fi

# Figure out how much memory to use per executor and set it as an environment
# variable so that our process sees it and can report it to Mesos
if [ -z "$SPARK_MEM" ] ; then
  SPARK_MEM="512m"
fi
export SPARK_MEM

# Set JAVA_OPTS to be able to load native libraries and to set heap size
JAVA_OPTS="$OUR_JAVA_OPTS"
JAVA_OPTS="$JAVA_OPTS -Djava.library.path=$SPARK_LIBRARY_PATH"
JAVA_OPTS="$JAVA_OPTS -Xms$SPARK_MEM -Xmx$SPARK_MEM"
# Load extra JAVA_OPTS from conf/java-opts, if it exists
if [ -e $FWDIR/conf/java-opts ] ; then
  JAVA_OPTS="$JAVA_OPTS `cat $FWDIR/conf/java-opts`"
fi
export JAVA_OPTS
# Attention: when changing the way the JAVA_OPTS are assembled, the change must be reflected in ExecutorRunner.scala!

if [ ! -f "$FWDIR/RELEASE" ]; then
  CORE_DIR="$FWDIR/core"
  EXAMPLES_DIR="$FWDIR/examples"
  REPL_DIR="$FWDIR/repl"

  # Exit if the user hasn't compiled Spark
  if [ ! -e "$CORE_DIR/target" ]; then
    echo "Failed to find Spark classes in $CORE_DIR/target" >&2
    echo "You need to compile Spark before running this program" >&2
    exit 1
  fi

  if [[ "$@" = *repl* && ! -e "$REPL_DIR/target" ]]; then
    echo "Failed to find Spark classes in $REPL_DIR/target" >&2
    echo "You need to compile Spark repl module before running this program" >&2
    exit 1
  fi

  # Figure out the JAR file that our examples were packaged into. This includes a bit of a hack
  # to avoid the -sources and -doc packages that are built by publish-local.
  if [ -e "$EXAMPLES_DIR/target/scala-$SCALA_VERSION/spark-examples"*[0-9T].jar ]; then
    # Use the JAR from the SBT build
    export SPARK_EXAMPLES_JAR=`ls "$EXAMPLES_DIR/target/scala-$SCALA_VERSION/spark-examples"*[0-9T].jar`
  fi
  if [ -e "$EXAMPLES_DIR/target/spark-examples"*[0-9T].jar ]; then
    # Use the JAR from the Maven build
    export SPARK_EXAMPLES_JAR=`ls "$EXAMPLES_DIR/target/spark-examples"*[0-9T].jar`
  fi
fi

# Compute classpath using external script
CLASSPATH=`$FWDIR/bin/compute-classpath.sh`
export CLASSPATH

if [ "$SPARK_LAUNCH_WITH_SCALA" == "1" ]; then
  EXTRA_ARGS=""     # Java options will be passed to scala as JAVA_OPTS
else
  # The JVM doesn't read JAVA_OPTS by default so we need to pass it in
  EXTRA_ARGS="$JAVA_OPTS"
fi

command="$RUNNER -cp \"$CLASSPATH\" $EXTRA_ARGS $@"
if [ "$SPARK_PRINT_LAUNCH_COMMAND" == "1" ]; then
  echo "Spark Command: $command"
  echo "========================================"
  echo
fi

exec "$RUNNER" -cp "$CLASSPATH" $EXTRA_ARGS "$@"
