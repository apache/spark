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

# Figure out where the Spark framework is installed
FWDIR="$(cd `dirname $0`; pwd)"
DISTDIR="$FWDIR/dist"

SPARK_TACHYON=false
MAKE_TGZ=false
NAME=none

function exit_with_usage {
  echo "make-distribution.sh - tool for making binary distributions of Spark"
  echo ""
  echo "usage:"
  echo "./make-distribution.sh [--name] [--tgz] [--with-tachyon] <maven build options>"
  echo "See Spark's \"Building with Maven\" doc for correct Maven options."
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --hadoop)
      echo "Error: '--hadoop' is no longer supported:"
      echo "Error: use Maven options -Phadoop.version and -Pyarn.version"
      exit_with_usage
      ;;
    --with-yarn)
      echo "Error: '--with-yarn' is no longer supported, use Maven option -Pyarn"
      exit_with_usage
      ;;
    --with-hive)
      echo "Error: '--with-hive' is no longer supported, use Maven option -Phive"
      exit_with_usage
      ;;
    --skip-java-test)
      SKIP_JAVA_TEST=true
      ;;
    --with-tachyon)
      SPARK_TACHYON=true
      ;;
    --tgz)
      MAKE_TGZ=true
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
  if which rpm &>/dev/null; then
    RPM_JAVA_HOME=$(rpm -E %java_home 2>/dev/null)
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME=$RPM_JAVA_HOME
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if which git &>/dev/null; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z $GITREV ]; then
	 GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
fi

if ! which mvn &>/dev/null; then
    echo -e "You need Maven installed to build Spark."
    echo -e "Download Maven from https://maven.apache.org/"
    exit -1;
fi

VERSION=$(mvn help:evaluate -Dexpression=project.version 2>/dev/null | grep -v "INFO" | tail -n 1)
SPARK_HADOOP_VERSION=$(mvn help:evaluate -Dexpression=hadoop.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)
SPARK_HIVE=$(mvn help:evaluate -Dexpression=project.activeProfiles $@ 2>/dev/null\
    | grep -v "INFO"\
    | fgrep --count "<id>hive</id>";\
    # Reset exit status to 0, otherwise the script stops here if the last grep finds nothing\
    # because we use "set -o pipefail"
    echo -n)

JAVA_CMD="$JAVA_HOME"/bin/java
JAVA_VERSION=$("$JAVA_CMD" -version 2>&1)
if [[ ! "$JAVA_VERSION" =~ "1.6" && -z "$SKIP_JAVA_TEST" ]]; then
  echo "***NOTE***: JAVA_HOME is not set to a JDK 6 installation. The resulting"
  echo "            distribution may not work well with PySpark and will not run"
  echo "            with Java 6 (See SPARK-1703 and SPARK-1911)."
  echo "            This test can be disabled by adding --skip-java-test."
  echo "Output from 'java -version' was:"
  echo "$JAVA_VERSION"
  read -p "Would you like to continue anyways? [y,n]: " -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Okay, exiting."
    exit 1
  fi
fi

if [ "$NAME" == "none" ]; then
  NAME=$SPARK_HADOOP_VERSION
fi

echo "Spark version is $VERSION"

if [ "$MAKE_TGZ" == "true" ]; then
  echo "Making spark-$VERSION-bin-$NAME.tgz"
else
  echo "Making distribution for Spark $VERSION in $DISTDIR..."
fi

if [ "$SPARK_TACHYON" == "true" ]; then
  echo "Tachyon Enabled"
else
  echo "Tachyon Disabled"
fi

# Build uber fat JAR
cd "$FWDIR"

export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

BUILD_COMMAND="mvn clean package -DskipTests $@"

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/lib"
echo "Spark $VERSION$GITREVSTRING built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"

# Copy jars
cp "$FWDIR"/assembly/target/scala*/*assembly*hadoop*.jar "$DISTDIR/lib/"
cp "$FWDIR"/examples/target/scala*/spark-examples*.jar "$DISTDIR/lib/"

# Copy example sources (needed for python and SQL)
mkdir -p "$DISTDIR/examples/src/main"
cp -r "$FWDIR"/examples/src/main "$DISTDIR/examples/src/"

if [ "$SPARK_HIVE" == "1" ]; then
  cp "$FWDIR"/lib_managed/jars/datanucleus*.jar "$DISTDIR/lib/"
fi

# Copy license and ASF files
cp "$FWDIR/LICENSE" "$DISTDIR"
cp "$FWDIR/NOTICE" "$DISTDIR"

if [ -e "$FWDIR"/CHANGES.txt ]; then
  cp "$FWDIR/CHANGES.txt" "$DISTDIR"
fi

# Copy other things
mkdir "$DISTDIR"/conf
cp "$FWDIR"/conf/*.template "$DISTDIR"/conf
cp "$FWDIR"/conf/slaves "$DISTDIR"/conf
cp "$FWDIR/README.md" "$DISTDIR"
cp -r "$FWDIR/bin" "$DISTDIR"
cp -r "$FWDIR/python" "$DISTDIR"
cp -r "$FWDIR/sbin" "$DISTDIR"
cp -r "$FWDIR/ec2" "$DISTDIR"

# Download and copy in tachyon, if requested
if [ "$SPARK_TACHYON" == "true" ]; then
  TACHYON_VERSION="0.5.0"
  TACHYON_URL="https://github.com/amplab/tachyon/releases/download/v${TACHYON_VERSION}/tachyon-${TACHYON_VERSION}-bin.tar.gz"

  TMPD=`mktemp -d 2>/dev/null || mktemp -d -t 'disttmp'`

  pushd $TMPD > /dev/null
  echo "Fetching tachyon tgz"
  wget "$TACHYON_URL"

  tar xf "tachyon-${TACHYON_VERSION}-bin.tar.gz"
  cp "tachyon-${TACHYON_VERSION}/target/tachyon-${TACHYON_VERSION}-jar-with-dependencies.jar" "$DISTDIR/lib"
  mkdir -p "$DISTDIR/tachyon/src/main/java/tachyon/web"
  cp -r "tachyon-${TACHYON_VERSION}"/{bin,conf,libexec} "$DISTDIR/tachyon"
  cp -r "tachyon-${TACHYON_VERSION}"/src/main/java/tachyon/web/resources "$DISTDIR/tachyon/src/main/java/tachyon/web"

  if [[ `uname -a` == Darwin* ]]; then
    # need to run sed differently on osx
    nl=$'\n'; sed -i "" -e "s|export TACHYON_JAR=\$TACHYON_HOME/target/\(.*\)|# This is set for spark's make-distribution\\$nl  export TACHYON_JAR=\$TACHYON_HOME/../lib/\1|" "$DISTDIR/tachyon/libexec/tachyon-config.sh"
  else
    sed -i "s|export TACHYON_JAR=\$TACHYON_HOME/target/\(.*\)|# This is set for spark's make-distribution\n  export TACHYON_JAR=\$TACHYON_HOME/../lib/\1|" "$DISTDIR/tachyon/libexec/tachyon-config.sh"
  fi

  popd > /dev/null
  rm -rf $TMPD
fi

if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=spark-$VERSION-bin-$NAME
  TARDIR="$FWDIR/$TARDIR_NAME"
  rm -rf "$TARDIR"
  cp -r "$DISTDIR" "$TARDIR"
  tar czf "spark-$VERSION-bin-$NAME.tgz" -C "$FWDIR" "$TARDIR_NAME"
  rm -rf "$TARDIR"
fi
