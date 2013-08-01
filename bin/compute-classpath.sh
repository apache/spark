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

# This script computes Spark's classpath and prints it to stdout; it's used by both the "run"
# script and the ExecutorRunner in standalone cluster mode.

SCALA_VERSION=2.9.3

# Figure out where Spark is installed
FWDIR="$(cd `dirname $0`/..; pwd)"

# Load environment variables from conf/spark-env.sh, if it exists
if [ -e $FWDIR/conf/spark-env.sh ] ; then
  . $FWDIR/conf/spark-env.sh
fi

CORE_DIR="$FWDIR/core"
REPL_DIR="$FWDIR/repl"
REPL_BIN_DIR="$FWDIR/repl-bin"
EXAMPLES_DIR="$FWDIR/examples"
BAGEL_DIR="$FWDIR/bagel"
MLLIB_DIR="$FWDIR/mllib"
TOOLS_DIR="$FWDIR/tools"
STREAMING_DIR="$FWDIR/streaming"
PYSPARK_DIR="$FWDIR/python"

# Build up classpath
CLASSPATH="$SPARK_CLASSPATH"

function dev_classpath {
  CLASSPATH="$CLASSPATH:$FWDIR/conf"
  CLASSPATH="$CLASSPATH:$CORE_DIR/target/scala-$SCALA_VERSION/classes"
  if [ -n "$SPARK_TESTING" ] ; then
    CLASSPATH="$CLASSPATH:$CORE_DIR/target/scala-$SCALA_VERSION/test-classes"
    CLASSPATH="$CLASSPATH:$STREAMING_DIR/target/scala-$SCALA_VERSION/test-classes"
  fi
  CLASSPATH="$CLASSPATH:$CORE_DIR/src/main/resources"
  CLASSPATH="$CLASSPATH:$REPL_DIR/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$EXAMPLES_DIR/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$STREAMING_DIR/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$STREAMING_DIR/lib/org/apache/kafka/kafka/0.7.2-spark/*" # <-- our in-project Kafka Jar
  if [ -e "$FWDIR/lib_managed" ]; then
    CLASSPATH="$CLASSPATH:$FWDIR/lib_managed/jars/*"
    CLASSPATH="$CLASSPATH:$FWDIR/lib_managed/bundles/*"
  fi
  CLASSPATH="$CLASSPATH:$REPL_DIR/lib/*"
  # Add the shaded JAR for Maven builds
  if [ -e $REPL_BIN_DIR/target ]; then
    for jar in `find "$REPL_BIN_DIR/target" -name 'spark-repl-*-shaded-hadoop*.jar'`; do
      CLASSPATH="$CLASSPATH:$jar"
    done
    # The shaded JAR doesn't contain examples, so include those separately
    EXAMPLES_JAR=`ls "$EXAMPLES_DIR/target/spark-examples"*[0-9T].jar`
    CLASSPATH+=":$EXAMPLES_JAR"
  fi
  CLASSPATH="$CLASSPATH:$BAGEL_DIR/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$MLLIB_DIR/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$TOOLS_DIR/target/scala-$SCALA_VERSION/classes"
  for jar in `find $PYSPARK_DIR/lib -name '*jar'`; do
    CLASSPATH="$CLASSPATH:$jar"
  done

  # Add Scala standard library
  if [ -z "$SCALA_LIBRARY_PATH" ]; then
    if [ -z "$SCALA_HOME" ]; then
      echo "SCALA_HOME is not set" >&2
      exit 1
    fi
    SCALA_LIBRARY_PATH="$SCALA_HOME/lib"
  fi
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/scala-library.jar"
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/scala-compiler.jar"
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/jline.jar"
}

function release_classpath {
  CLASSPATH="$CLASSPATH:$FWDIR/jars/*"
}

if [ -f "$FWDIR/RELEASE" ]; then
  release_classpath
else
  dev_classpath
fi

# Add hadoop conf dir - else FileSystem.*, etc fail !
# Note, this assumes that there is either a HADOOP_CONF_DIR or YARN_CONF_DIR which hosts
# the configurtion files.
if [ "x" != "x$HADOOP_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$HADOOP_CONF_DIR"
fi
if [ "x" != "x$YARN_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$YARN_CONF_DIR"
fi

echo "$CLASSPATH"
