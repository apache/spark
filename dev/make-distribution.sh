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
MAKE_PIP=false
MAKE_R=false
NAME=none
MVN="$SPARK_HOME/build/mvn"

function exit_with_usage {
  set +x
  echo "make-distribution.sh - tool for making binary distributions of Spark"
  echo ""
  echo "usage:"
  cl_options="[--name] [--tgz] [--pip] [--r] [--mvn <mvn-command>]"
  echo "make-distribution.sh $cl_options <maven build options>"
  echo "See Spark's \"Building Spark\" doc for correct Maven options."
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --tgz)
      MAKE_TGZ=true
      ;;
    --pip)
      MAKE_PIP=true
      ;;
    --r)
      MAKE_R=true
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
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
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

  if [ -z "$JAVA_HOME" ]; then
    if [ `command -v java` ]; then
      # If java is in /usr/bin/java, we want /usr
      JAVA_HOME="$(dirname $(dirname $(which java)))"
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

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK_HADOOP_VERSION=$("$MVN" help:evaluate -Dexpression=hadoop.version $@ \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK_HIVE=$("$MVN" help:evaluate -Dexpression=project.activeProfiles -pl sql/hive $@ \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | grep -F --count "<id>hive</id>";\
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
  echo "Making distribution for Spark $VERSION in '$DISTDIR'..."
fi

# Build uber fat JAR
cd "$SPARK_HOME"

export MAVEN_OPTS="${MAVEN_OPTS:--Xss128m -Xmx4g -XX:ReservedCodeCacheSize=128m}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" clean package \
    -DskipTests \
    -Dmaven.javadoc.skip=true \
    -Dmaven.scaladoc.skip=true \
    -Dmaven.source.skip \
    -Dcyclonedx.skip=true \
    $@)

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
cp -r "$SPARK_HOME"/assembly/target/scala*/jars/* "$DISTDIR/jars/"

# Only create the hive-jackson directory if they exist.
if [ -f "$DISTDIR"/jars/jackson-core-asl-1.9.13.jar ]; then
  for f in "$DISTDIR"/jars/jackson-*-asl-*.jar; do
    mkdir -p "$DISTDIR"/hive-jackson
    mv $f "$DISTDIR"/hive-jackson/
  done
fi

# Only create the yarn directory if the yarn artifacts were built.
if [ -f "$SPARK_HOME"/common/network-yarn/target/scala*/spark-*-yarn-shuffle.jar ]; then
  mkdir "$DISTDIR/yarn"
  cp "$SPARK_HOME"/common/network-yarn/target/scala*/spark-*-yarn-shuffle.jar "$DISTDIR/yarn"
fi

# Only create and copy the dockerfiles directory if the kubernetes artifacts were built.
if [ -d "$SPARK_HOME"/resource-managers/kubernetes/core/target/ ]; then
  mkdir -p "$DISTDIR/kubernetes/"
  cp -a "$SPARK_HOME"/resource-managers/kubernetes/docker/src/main/dockerfiles "$DISTDIR/kubernetes/"
  cp -a "$SPARK_HOME"/resource-managers/kubernetes/integration-tests/tests "$DISTDIR/kubernetes/"
fi

# Copy examples and dependencies
mkdir -p "$DISTDIR/examples/jars"
cp "$SPARK_HOME"/examples/target/scala*/jars/* "$DISTDIR/examples/jars"

# Deduplicate jars that have already been packaged as part of the main Spark dependencies.
for f in "$DISTDIR"/examples/jars/*; do
  name=$(basename "$f")
  if [ -f "$DISTDIR/jars/$name" ]; then
    rm "$DISTDIR/examples/jars/$name"
  fi
done

# Copy example sources (needed for python and SQL)
mkdir -p "$DISTDIR/examples/src/main"
cp -r "$SPARK_HOME/examples/src/main" "$DISTDIR/examples/src/"

# Copy license and ASF files
if [ -e "$SPARK_HOME/LICENSE-binary" ]; then
  cp "$SPARK_HOME/LICENSE-binary" "$DISTDIR/LICENSE"
  cp -r "$SPARK_HOME/licenses-binary" "$DISTDIR/licenses"
  cp "$SPARK_HOME/NOTICE-binary" "$DISTDIR/NOTICE"
else
  echo "Skipping copying LICENSE files"
fi

if [ -e "$SPARK_HOME/CHANGES.txt" ]; then
  cp "$SPARK_HOME/CHANGES.txt" "$DISTDIR"
fi

# Copy data files
cp -r "$SPARK_HOME/data" "$DISTDIR"

# Make pip package
if [ "$MAKE_PIP" == "true" ]; then
  echo "Building python distribution package"
  pushd "$SPARK_HOME/python" > /dev/null
  # Delete the egg info file if it exists, this can cache older setup files.
  rm -rf pyspark.egg-info || echo "No existing egg info file, skipping deletion"
  python3 packaging/classic/setup.py sdist
  python3 packaging/connect/setup.py sdist
  popd > /dev/null
else
  echo "Skipping building python distribution package"
fi

# Make R package - this is used for both CRAN release and packing R layout into distribution
if [ "$MAKE_R" == "true" ]; then
  echo "Building R source package"
  R_PACKAGE_VERSION=`grep Version "$SPARK_HOME/R/pkg/DESCRIPTION" | awk '{print $NF}'`
  pushd "$SPARK_HOME/R" > /dev/null
  # Build source package and run full checks
  # Do not source the check-cran.sh - it should be run from where it is for it to set SPARK_HOME
  NO_TESTS=1 "$SPARK_HOME/R/check-cran.sh"

  # Move R source package to match the Spark release version if the versions are not the same.
  # NOTE(shivaram): `mv` throws an error on Linux if source and destination are same file
  if [ "$R_PACKAGE_VERSION" != "$VERSION" ]; then
    mv "$SPARK_HOME/R/SparkR_$R_PACKAGE_VERSION.tar.gz" "$SPARK_HOME/R/SparkR_$VERSION.tar.gz"
  fi

  # Install source package to get it to generate vignettes rds files, etc.
  VERSION=$VERSION "$SPARK_HOME/R/install-source-package.sh"
  popd > /dev/null
else
  echo "Skipping building R source package"
fi

# Copy other things
mkdir "$DISTDIR/conf"
cp "$SPARK_HOME"/conf/*.template "$DISTDIR/conf"
cp "$SPARK_HOME/README.md" "$DISTDIR"
cp -r "$SPARK_HOME/bin" "$DISTDIR"
cp -r "$SPARK_HOME/python" "$DISTDIR"

# Remove the python distribution from dist/ if we built it
if [ "$MAKE_PIP" == "true" ]; then
  rm -f "$DISTDIR"/python/dist/pyspark-*.tar.gz
fi

cp -r "$SPARK_HOME/sbin" "$DISTDIR"
# Copy SparkR if it exists
if [ -d "$SPARK_HOME/R/lib/SparkR" ]; then
  mkdir -p "$DISTDIR/R/lib"
  cp -r "$SPARK_HOME/R/lib/SparkR" "$DISTDIR/R/lib"
  cp "$SPARK_HOME/R/lib/sparkr.zip" "$DISTDIR/R/lib"
fi

if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=spark-$VERSION-bin-$NAME
  TARDIR="$SPARK_HOME/$TARDIR_NAME"
  rm -rf "$TARDIR"
  cp -r "$DISTDIR" "$TARDIR"
  TAR="tar"
  if [ "$(uname -s)" = "Darwin" ]; then
    TAR="tar --no-mac-metadata --no-xattrs --no-fflags"
  fi
  $TAR -czf "spark-$VERSION-bin-$NAME.tgz" -C "$SPARK_HOME" "$TARDIR_NAME"
  rm -rf "$TARDIR"
fi
