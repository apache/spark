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

# Figure out where Spark is installed
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

. "$FWDIR"/bin/load-spark-env.sh

if [ -n "$SPARK_CLASSPATH" ]; then
  CLASSPATH="$SPARK_CLASSPATH:$SPARK_SUBMIT_CLASSPATH"
else
  CLASSPATH="$SPARK_SUBMIT_CLASSPATH"
fi

# Build up classpath
if [ -n "$SPARK_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$SPARK_CONF_DIR"
else
  CLASSPATH="$CLASSPATH:$FWDIR/conf"
fi

ASSEMBLY_DIR="$FWDIR/assembly/target/scala-$SPARK_SCALA_VERSION"

if [ -n "$JAVA_HOME" ]; then
  JAR_CMD="$JAVA_HOME/bin/jar"
else
  JAR_CMD="jar"
fi

# A developer option to prepend more recently compiled Spark classes
if [ -n "$SPARK_PREPEND_CLASSES" ]; then
  echo "NOTE: SPARK_PREPEND_CLASSES is set, placing locally compiled Spark"\
    "classes ahead of assembly." >&2
  # Spark classes
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/graphx/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/tools/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/catalyst/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/core/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/hive/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/hive-thriftserver/target/scala-$SPARK_SCALA_VERSION/classes"
  CLASSPATH="$CLASSPATH:$FWDIR/yarn/stable/target/scala-$SPARK_SCALA_VERSION/classes"
  # Jars for shaded deps in their original form (copied here during build)
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/jars/*"
fi

# Use spark-assembly jar from either RELEASE or assembly directory
if [ -f "$FWDIR/RELEASE" ]; then
  assembly_folder="$FWDIR"/lib
else
  assembly_folder="$ASSEMBLY_DIR"
fi

num_jars=0

for f in "${assembly_folder}"/spark-assembly*hadoop*.jar; do
  if [[ ! -e "$f" ]]; then
    echo "Failed to find Spark assembly in $assembly_folder" 1>&2
    echo "You need to build Spark before running this program." 1>&2
    exit 1
  fi
  ASSEMBLY_JAR="$f"
  num_jars=$((num_jars+1))
done

if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Spark assembly jars in $assembly_folder:" 1>&2
  ls "${assembly_folder}"/spark-assembly*hadoop*.jar 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

# Verify that versions of java used to build the jars and run Spark are compatible
jar_error_check=$("$JAR_CMD" -tf "$ASSEMBLY_JAR" nonexistent/class/path 2>&1)
if [[ "$jar_error_check" =~ "invalid CEN header" ]]; then
  echo "Loading Spark jar with '$JAR_CMD' failed. " 1>&2
  echo "This is likely because Spark was compiled with Java 7 and run " 1>&2
  echo "with Java 6. (see SPARK-1703). Please use Java 7 to run Spark " 1>&2
  echo "or build Spark with Java 6." 1>&2
  exit 1
fi

CLASSPATH="$CLASSPATH:$ASSEMBLY_JAR"

# When Hive support is needed, Datanucleus jars must be included on the classpath.
# Datanucleus jars do not work if only included in the uber jar as plugin.xml metadata is lost.
# Both sbt and maven will populate "lib_managed/jars/" with the datanucleus jars when Spark is
# built with Hive, so first check if the datanucleus jars exist, and then ensure the current Spark
# assembly is built for Hive, before actually populating the CLASSPATH with the jars.
# Note that this check order is faster (by up to half a second) in the case where Hive is not used.
if [ -f "$FWDIR/RELEASE" ]; then
  datanucleus_dir="$FWDIR"/lib
else
  datanucleus_dir="$FWDIR"/lib_managed/jars
fi

datanucleus_jars="$(find "$datanucleus_dir" 2>/dev/null | grep "datanucleus-.*\\.jar$")"
datanucleus_jars="$(echo "$datanucleus_jars" | tr "\n" : | sed s/:$//g)"

if [ -n "$datanucleus_jars" ]; then
  hive_files=$("$JAR_CMD" -tf "$ASSEMBLY_JAR" org/apache/hadoop/hive/ql/exec 2>/dev/null)
  if [ -n "$hive_files" ]; then
    echo "Spark assembly has been built with Hive, including Datanucleus jars on classpath" 1>&2
    CLASSPATH="$CLASSPATH:$datanucleus_jars"
  fi
fi

# Add test classes if we're running from SBT or Maven with SPARK_TESTING set to 1
if [[ $SPARK_TESTING == 1 ]]; then
  CLASSPATH="$CLASSPATH:$FWDIR/core/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/repl/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/mllib/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/bagel/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/graphx/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/streaming/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/catalyst/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/core/target/scala-$SPARK_SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$FWDIR/sql/hive/target/scala-$SPARK_SCALA_VERSION/test-classes"
fi

# Add hadoop conf dir if given -- otherwise FileSystem.*, etc fail !
# Note, this assumes that there is either a HADOOP_CONF_DIR or YARN_CONF_DIR which hosts
# the configurtion files.
if [ -n "$HADOOP_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$HADOOP_CONF_DIR"
fi
if [ -n "$YARN_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$YARN_CONF_DIR"
fi

# To allow for distributions to append needed libraries to the classpath (e.g. when
# using the "hadoop-provided" profile to build Spark), check SPARK_DIST_CLASSPATH and
# append it to tbe final classpath.
if [ -n "$SPARK_DIST_CLASSPATH" ]; then
  CLASSPATH="$CLASSPATH:$SPARK_DIST_CLASSPATH"
fi

echo "$CLASSPATH"
