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

set -o pipefail
set -e
set -x

# Figure out where the Spark framework is installed
SPARK_HOME="$(cd "`dirname "$0"`/.."; pwd)"
DISTDIR="$SPARK_HOME/dist"

MAKE_TGZ=false
NAME=none
MVN="$SPARK_HOME/build/mvn"
SBT="$SPARK_HOME/build/sbt"

function exit_with_usage {
  echo "make-distribution.sh - tool for making binary distributions of Spark"
  echo ""
  echo "usage:"
  cl_options="[--name] [--tgz] [--mvn <mvn-command>]"
  echo "make-distribution.sh $cl_options <maven build options>"
  echo "See Spark's \"Building Spark\" doc for correct Maven options."
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --hadoop)
      echo "Error: '--hadoop' is no longer supported:"
      echo "Error: use Maven profiles and options -Dhadoop.version and -Dyarn.version instead."
      echo "Error: Related profiles include hadoop-2.2, hadoop-2.3, hadoop-2.4, hadoop-2.6 and hadoop-2.7."
      exit_with_usage
      ;;
    --with-yarn)
      echo "Error: '--with-yarn' is no longer supported, use Maven option -Pyarn"
      exit_with_usage
      ;;
    --with-hive)
      echo "Error: '--with-hive' is no longer supported, use Maven options -Phive and -Phive-thriftserver"
      exit_with_usage
      ;;
    --tgz)
      MAKE_TGZ=true
      ;;
    --mvn)
      MVN="$2"
      shift
      ;;
    --name)
      NAME="$2"
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    *)
      break
      ;;
  esac
  shift
done

if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if [ $(command -v  rpm) ]; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if [ $(command -v git) ]; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z "$GITREV" ]; then
        GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
fi


if [ ! "$(command -v "$MVN")" ] ; then
    echo -e "Could not locate Maven command: '$MVN'."
    echo -e "Specify the Maven command with the --mvn flag"
    exit -1;
fi

VERSION=$(git describe --tags)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)
SPARK_HADOOP_VERSION=$("$MVN" help:evaluate -Dexpression=hadoop.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)
SPARK_HIVE=$("$MVN" help:evaluate -Dexpression=project.activeProfiles -pl sql/hive $@ 2>/dev/null\
    | grep -v "INFO"\
    | fgrep --count "<id>hive</id>";\
    # Reset exit status to 0, otherwise the script stops here if the last grep finds nothing\
    # because we use "set -o pipefail"
    echo -n)

if [ "$NAME" == "none" ]; then
  NAME=$SPARK_HADOOP_VERSION
fi

echo "Spark version is $VERSION"

if [ "$MAKE_TGZ" == "true" ]; then
  echo "Making spark-$VERSION-bin-$NAME.tgz"
else
  echo "Making distribution for Spark $VERSION in $DISTDIR..."
fi

# Build uber fat JAR
cd "$SPARK_HOME"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$SBT" assembly examples/package network-yarn/assembly $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/jars"
echo "Spark $VERSION$GITREVSTRING built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"
echo "Build flags: $@" >> "$DISTDIR/RELEASE"

# Copy jars
cp "$SPARK_HOME"/assembly/target/scala*/jars/* "$DISTDIR/jars/"

# Only create the yarn directory if the yarn artifacts were build.
if [ -f "$SPARK_HOME"/common/network-yarn/target/scala*/spark-network-yarn-*.jar ]; then
  mkdir "$DISTDIR"/yarn
  cp "$SPARK_HOME"/common/network-yarn/target/scala*/spark-network-yarn-*.jar "$DISTDIR/yarn/spark-${VERSION}-yarn-shuffle.jar"
fi

# Copy examples and dependencies
mkdir -p "$DISTDIR/examples/jars"
cp "$SPARK_HOME"/examples/target/scala*/jars/* "$DISTDIR/examples/jars"

# Deduplicate jars that have already been packaged as part of the main Spark dependencies.
for f in "$DISTDIR/examples/jars/"*; do
  name=$(basename "$f")
  if [ -f "$DISTDIR/jars/$name" ]; then
    rm "$DISTDIR/examples/jars/$name"
  fi
done

# Copy example sources (needed for python and SQL)
mkdir -p "$DISTDIR/examples/src/main"
cp -r "$SPARK_HOME"/examples/src/main "$DISTDIR/examples/src/"

# Copy license and ASF files
cp "$SPARK_HOME/LICENSE" "$DISTDIR"
cp -r "$SPARK_HOME/licenses" "$DISTDIR"
cp "$SPARK_HOME/NOTICE" "$DISTDIR"

if [ -e "$SPARK_HOME"/CHANGES.txt ]; then
  cp "$SPARK_HOME/CHANGES.txt" "$DISTDIR"
fi

# Copy data files
cp -r "$SPARK_HOME/data" "$DISTDIR"

# Copy other things
mkdir "$DISTDIR"/conf
cp "$SPARK_HOME"/conf/*.template "$DISTDIR"/conf
cp "$SPARK_HOME/README.md" "$DISTDIR"
cp -r "$SPARK_HOME/bin" "$DISTDIR"
cp -r "$SPARK_HOME/python" "$DISTDIR"
cp -r "$SPARK_HOME/sbin" "$DISTDIR"
# Copy SparkR if it exists
if [ -d "$SPARK_HOME"/R/lib/SparkR ]; then
  mkdir -p "$DISTDIR"/R/lib
  cp -r "$SPARK_HOME/R/lib/SparkR" "$DISTDIR"/R/lib
  cp "$SPARK_HOME/R/lib/sparkr.zip" "$DISTDIR"/R/lib
fi

if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=spark-$VERSION-bin-$NAME
  TARDIR="$SPARK_HOME/$TARDIR_NAME"
  rm -rf "$TARDIR"
  cp -r "$DISTDIR" "$TARDIR"
  tar czf "spark-$VERSION-bin-$NAME.tgz" -C "$SPARK_HOME" "$TARDIR_NAME"
  rm -rf "$TARDIR"
fi
