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

#
# Script to create a binary distribution for easy deploys of Spark.
# The distribution directory defaults to dist/ but can be overridden below.
# The distribution contains fat (assembly) jars that include the Scala library,
# so it is completely self contained.
# It does not contain source or *.class files.
#
# Optional Arguments
#      --tgz: Additionally creates spark-$VERSION-bin.tar.gz
#      --hadoop VERSION: Builds against specified version of Hadoop.
#      --with-yarn: Enables support for Hadoop YARN.
#
# Recommended deploy/testing procedure (standalone mode):
# 1) Rsync / deploy the dist/ dir to one host
# 2) cd to deploy dir; ./bin/start-master.sh
# 3) Verify master is up by visiting web page, ie http://master-ip:8080.  Note the spark:// URL.
# 4) ./bin/start-slave.sh 1 <<spark:// URL>>
# 5) MASTER="spark://my-master-ip:7077" ./spark-shell
#

# Figure out where the Spark framework is installed
FWDIR="$(cd `dirname $0`; pwd)"
DISTDIR="$FWDIR/dist"

# Get version from SBT
export TERM=dumb   # Prevents color codes in SBT output
VERSION=$($FWDIR/sbt/sbt "show version" | tail -1 | cut -f 2 | sed 's/^\([a-zA-Z0-9.-]*\).*/\1/')

# Initialize defaults
SPARK_HADOOP_VERSION=1.0.4
SPARK_YARN=false
MAKE_TGZ=false

# Parse arguments
while (( "$#" )); do
  case $1 in
    --hadoop)
      SPARK_HADOOP_VERSION="$2"
      shift
      ;;
    --with-yarn)
      SPARK_YARN=true
      ;;
    --tgz)
      MAKE_TGZ=true
      ;;
  esac
  shift
done

if [ "$MAKE_TGZ" == "true" ]; then
	echo "Making spark-$VERSION-hadoop_$SPARK_HADOOP_VERSION-bin.tar.gz"
else
	echo "Making distribution for Spark $VERSION in $DISTDIR..."
fi

echo "Hadoop version set to $SPARK_HADOOP_VERSION"
if [ "$SPARK_YARN" == "true" ]; then
  echo "YARN enabled"
else
  echo "YARN disabled"
fi

# Build fat JAR
export SPARK_HADOOP_VERSION
export SPARK_YARN
"$FWDIR/sbt/sbt" "assembly/assembly"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/jars"
echo "Spark $VERSION built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"

# Copy jars
cp $FWDIR/assembly/target/scala*/*assembly*hadoop*.jar "$DISTDIR/jars/"

# Copy other things
mkdir "$DISTDIR"/conf
cp "$FWDIR"/conf/*.template "$DISTDIR"/conf
cp -r "$FWDIR/bin" "$DISTDIR"
cp -r "$FWDIR/python" "$DISTDIR"
cp "$FWDIR/spark-class" "$DISTDIR"
cp "$FWDIR/spark-shell" "$DISTDIR"
cp "$FWDIR/spark-executor" "$DISTDIR"
cp "$FWDIR/pyspark" "$DISTDIR"


if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR="$FWDIR/spark-$VERSION"
  cp -r "$DISTDIR" "$TARDIR"
  tar -zcf "spark-$VERSION-hadoop_$SPARK_HADOOP_VERSION-bin.tar.gz" -C "$FWDIR" "spark-$VERSION"
  rm -rf "$TARDIR"
fi
