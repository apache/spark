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

. $FWDIR/bin/load-spark-env.sh

# Build up classpath
CLASSPATH="$SPARK_CLASSPATH:$FWDIR/conf"

ASSEMBLY_DIR="$FWDIR/assembly/target/scala-$SCALA_VERSION"

# First check if we have a dependencies jar. If so, include binary classes with the deps jar
if [ -f "$ASSEMBLY_DIR"/spark-assembly*hadoop*-deps.jar ]; then
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/graphx/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/tools/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/catalyst/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/core/target/scala-$SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/hive/target/scala-$SCALA_VERSION/classes"

  DEPS_ASSEMBLY_JAR=`ls "$ASSEMBLY_DIR"/spark-assembly*hadoop*-deps.jar`
  CLASSPATH="$CLASSPATH:$DEPS_ASSEMBLY_JAR"
else
  # Else use spark-assembly jar from either RELEASE or assembly directory
  if [ -f "$FWDIR/RELEASE" ]; then
    ASSEMBLY_JAR=`ls "$FWDIR"/jars/spark*-assembly*.jar`
  else
    ASSEMBLY_JAR=`ls "$ASSEMBLY_DIR"/spark*-assembly*hadoop*.jar`
  fi
  CLASSPATH="$CLASSPATH:$ASSEMBLY_JAR"
fi

# When Hive support is needed, Datanucleus jars must be included on the classpath.
# Datanucleus jars do not work if only included in the  uber jar as plugin.xml metadata is lost.
# Both sbt and maven will populate "lib_managed/jars/" with the datanucleus jars when Spark is
# built with Hive, so first check if the datanucleus jars exist, and then ensure the current Spark
# assembly is built for Hive, before actually populating the CLASSPATH with the jars.
# Note that this check order is faster (by up to half a second) in the case where Hive is not used.
num_datanucleus_jars=$(ls "$FWDIR"/lib_managed/jars/ 2>/dev/null | grep "datanucleus-.*\\.jar" | wc -l)
if [ $num_datanucleus_jars -gt 0 ]; then
  AN_ASSEMBLY_JAR=${ASSEMBLY_JAR:-$DEPS_ASSEMBLY_JAR}
  num_hive_files=$(jar tvf "$AN_ASSEMBLY_JAR" org/apache/hadoop/hive/ql/exec 2>/dev/null | wc -l)
  if [ $num_hive_files -gt 0 ]; then
    echo "Spark assembly has been built with Hive, including Datanucleus jars on classpath" 1>&2
    DATANUCLEUSJARS=$(echo "$FWDIR/lib_managed/jars"/datanucleus-*.jar | tr " " :)
    CLASSPATH=$CLASSPATH:$DATANUCLEUSJARS
  fi
fi

# Add test classes if we're running from SBT or Maven with SPARK_TESTING set to 1
if [[ $SPARK_TESTING == 1 ]]; then
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/graphx/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/catalyst/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/core/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/hive/target/scala-$SCALA_VERSION/test-classes"
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
