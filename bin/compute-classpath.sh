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

SCALA_VERSION=2.10

# Figure out where Spark is installed
FWDIR="$(cd `dirname $0`/..; pwd)"

# Load environment variables from conf/spark-env.sh, if it exists
if [ -e $FWDIR/conf/spark-env.sh ] ; then
  . $FWDIR/conf/spark-env.sh
fi

# Build up classpath
CLASSPATH="$SPARK_CLASSPATH:$FWDIR/conf"

# First check if we have a dependencies jar. If so, include binary classes with the deps jar
if [ -f "$FWDIR"/assembly/target/scala-$SCALA_VERSION/spark-assembly*hadoop*-deps.jar ]; then
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SCALA_VERSION/classes"

  DEPS_ASSEMBLY_JAR=`ls "$FWDIR"/assembly/target/scala-$SCALA_VERSION/spark-assembly*hadoop*-deps.jar`
  CLASSPATH="$CLASSPATH:$DEPS_ASSEMBLY_JAR"
else
  # Else use spark-assembly jar from either RELEASE or assembly directory
  if [ -f "$FWDIR/RELEASE" ]; then
    ASSEMBLY_JAR=`ls "$FWDIR"/jars/spark-assembly*.jar`
  else
    ASSEMBLY_JAR=`ls "$FWDIR"/assembly/target/scala-$SCALA_VERSION/spark-assembly*hadoop*.jar`
  fi
  CLASSPATH="$CLASSPATH:$ASSEMBLY_JAR"
fi

# Add test classes if we're running from SBT or Maven with SPARK_TESTING set to 1
if [[ $SPARK_TESTING == 1 ]]; then
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SCALA_VERSION/test-classes"
fi

# Add hadoop conf dir if given -- otherwise FileSystem.*, etc fail !
# Note, this assumes that there is either a HADOOP_CONF_DIR or YARN_CONF_DIR which hosts
# the configurtion files.
if [ "x" != "x$HADOOP_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$HADOOP_CONF_DIR"
fi
if [ "x" != "x$YARN_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$YARN_CONF_DIR"
fi

echo "$CLASSPATH"
