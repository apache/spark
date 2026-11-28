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

# The Apache LICENSE and NOTICE are copied into the Python package directory
# below so they are bundled into the source distribution. Remove them on exit
# so a failed build does not leave stray files behind.
function cleanup_dist_license_files {
  rm -f "$SPARK_HOME/python/LICENSE" "$SPARK_HOME/python/NOTICE"
}
trap cleanup_dist_license_files EXIT

MAKE_TGZ=false
MAKE_PIP=false
MAKE_SPARK_CONNECT=false
NAME=none
MVN="$SPARK_HOME/build/mvn"
SBT_ENABLED=false
SBT="$SPARK_HOME/build/sbt"

function exit_with_usage {
  set +x
  echo "make-distribution.sh - tool for making binary distributions of Spark"
  echo ""
  echo "usage:"
  cl_options="[--name] [--tgz] [--pip] [--connect] [--mvn <mvn-command>] [--sbt-enabled] [--sbt <sbt-command>]"
  echo "make-distribution.sh $cl_options <maven/sbt build options>"
  echo "See Spark's \"Building Spark\" doc for correct Maven/SBT options."
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
    --connect)
      MAKE_SPARK_CONNECT=true
      ;;
    --mvn)
      MVN="$2"
      shift
      ;;
    --sbt-enabled)
      SBT_ENABLED=true
      ;;
    --sbt)
      SBT="$2"
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

if [ "$SBT_ENABLED" == "true" ] && [ ! "$(command -v "$SBT")" ]; then
  echo -e "Could not locate SBT command: '$SBT'."
  echo -e "Specify the SBT command with the --sbt flag"
  exit -1;
elif [ ! "$(command -v "$MVN")" ]; then
  echo -e "Could not locate Maven command: '$MVN'."
  echo -e "Specify the Maven command with the --mvn flag"
  exit -1;
fi

if [ "$SBT_ENABLED" == "true" ]; then
  VERSION=$("$SBT" -no-colors "show version" | awk '/\[info\]/{ver=$2} END{print ver}')
  SCALA_VERSION=$("$SBT" -no-colors "show scalaBinaryVersion" | awk '/\[info\]/{ver=$2} END{print ver}')
  SPARK_HADOOP_VERSION=$("$SBT" -no-colors "show hadoopVersion" | awk '/\[info\]/{ver=$2} END{print ver}')
else
  VERSION=$("$MVN" help:evaluate -Dexpression=project.version "$@" -q -DforceStdout)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version "$@" -q -DforceStdout)
  SPARK_HADOOP_VERSION=$("$MVN" help:evaluate -Dexpression=hadoop.version "$@" -q -DforceStdout)
fi

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

if [ "$SBT_ENABLED" == "true" ] ; then
  # Store the command as an array because $SBT variable might have spaces in it.
  # Normal quoting tricks don't work.
  # See: http://mywiki.wooledge.org/BashFAQ/050
  BUILD_COMMAND=("$SBT" clean package $@)
else
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
fi

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

# SPARK-53327: Use the modified ResourceImpl.class in spark-catalyst which is compatible with Java 25
if [ -f "$DISTDIR"/jars/datasketches-memory-3.0.2.jar ]; then
  zip -d "$DISTDIR"/jars/datasketches-memory-3.0.2.jar org/apache/datasketches/memory/internal/ResourceImpl.class
fi

# Copy the C header that the libraries of native data sources implement
mkdir -p "$DISTDIR/include"
cp "$SPARK_HOME/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h" \
  "$DISTDIR/include/"

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
# The Rust examples only build in a source tree, against the SDK under sql/native/rust.
rm -rf "$DISTDIR/examples/src/main/rust"

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
  # Delete build artifacts that may be left over from earlier builds so that the build
  # validation logic below doesn't trip on them.
  rm -rf pyspark.egg-info
  rm -f dist/pyspark*.tar.gz
  # Ship the Apache LICENSE and NOTICE inside the PySpark source distributions
  # (see MANIFEST.in). These are removed again after the sdists are built.
  #
  # The classic pyspark sdist bundles the assembly jars (packaging/classic/setup.py
  # builds a deps/jars symlink farm), so it ships the binary LICENSE/NOTICE that
  # enumerate the bundled third-party jars' licenses, mirroring the binary
  # distribution above. The connect and client sdists bundle no jars and ship the
  # plain source LICENSE/NOTICE.
  if [ -e "$SPARK_HOME/LICENSE-binary" ]; then
    cp "$SPARK_HOME/LICENSE-binary" LICENSE
    cp "$SPARK_HOME/NOTICE-binary" NOTICE
  else
    cp "$SPARK_HOME/LICENSE" LICENSE
    cp "$SPARK_HOME/NOTICE" NOTICE
  fi
  python3 packaging/classic/setup.py sdist

  cp "$SPARK_HOME/LICENSE" LICENSE
  cp "$SPARK_HOME/NOTICE" NOTICE
  python3 packaging/connect/setup.py sdist
  python3 packaging/client/setup.py sdist
  rm -f LICENSE NOTICE

  # Guard against regressions: every PySpark sdist must contain LICENSE and NOTICE
  # at the package root. The missing files were only caught by a Spark 4.2.0 RC1
  # vote -1 (SPARK-57393); fail the release build here instead of at vote time.
  for f in dist/pyspark*.tar.gz; do
    listing=$(tar tzf "$f")
    for required in LICENSE NOTICE; do
      grep -qE "^[^/]+/$required\$" <<< "$listing" || \
        { echo "ERROR: $f is missing $required at the package root"; exit 1; }
    done
  done
  popd > /dev/null
else
  echo "Skipping building python distribution package"
fi

# Copy other things
mkdir "$DISTDIR/conf"
cp "$SPARK_HOME"/conf/*.template "$DISTDIR/conf"
cp "$SPARK_HOME/README.md" "$DISTDIR"
cp -r "$SPARK_HOME/bin" "$DISTDIR"
if command -v git && command -v cpio && git rev-parse --git-dir 2>/dev/null; then
  git ls-files -z "$SPARK_HOME/python" | cpio -0pdm "$DISTDIR"
  cp "$SPARK_HOME/python/lib/pyspark.zip" "$DISTDIR/python/lib"
else
  cp -r "$SPARK_HOME/python" "$DISTDIR"
fi

# Remove the python distribution from dist/ if we built it
if [ "$MAKE_PIP" == "true" ]; then
  rm -f "$DISTDIR"/python/dist/pyspark-*.tar.gz
fi

cp -r "$SPARK_HOME/sbin" "$DISTDIR"

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
  if [[ "$MAKE_SPARK_CONNECT" == "true" ]]; then
    TARDIR_NAME=spark-$VERSION-bin-$NAME-connect
    TARDIR="$SPARK_HOME/$TARDIR_NAME"
    rm -rf "$TARDIR"
    cp -r "$DISTDIR" "$TARDIR"
    # Set the Spark Connect system variable in these scripts to enable it by default.
    awk 'NR==1{print; print "export SPARK_CONNECT_BEELINE=${SPARK_CONNECT_BEELINE:-1}"; next} {print}' "$TARDIR/bin/beeline" > tmp && cat tmp > "$TARDIR/bin/beeline"
    awk 'NR==1{print; print "export SPARK_CONNECT_MODE=${SPARK_CONNECT_MODE:-1}"; next} {print}' "$TARDIR/bin/pyspark" > tmp && cat tmp > "$TARDIR/bin/pyspark"
    awk 'NR==1{print; print "export SPARK_CONNECT_MODE=${SPARK_CONNECT_MODE:-1}"; next} {print}' "$TARDIR/bin/spark-shell" > tmp && cat tmp > "$TARDIR/bin/spark-shell"
    awk 'NR==1{print; print "export SPARK_CONNECT_MODE=${SPARK_CONNECT_MODE:-1}"; next} {print}' "$TARDIR/bin/spark-submit" > tmp && cat tmp > "$TARDIR/bin/spark-submit"
    awk 'NR==1{print; print "if [%SPARK_CONNECT_BEELINE%] == [] set SPARK_CONNECT_BEELINE=1"; next} {print}' "$TARDIR/bin/beeline.cmd" > tmp && cat tmp > "$TARDIR/bin/beeline.cmd"
    awk 'NR==1{print; print "if [%SPARK_CONNECT_MODE%] == [] set SPARK_CONNECT_MODE=1"; next} {print}' "$TARDIR/bin/pyspark2.cmd" > tmp && cat tmp > "$TARDIR/bin/pyspark2.cmd"
    awk 'NR==1{print; print "if [%SPARK_CONNECT_MODE%] == [] set SPARK_CONNECT_MODE=1"; next} {print}' "$TARDIR/bin/spark-shell2.cmd" > tmp && cat tmp > "$TARDIR/bin/spark-shell2.cmd"
    awk 'NR==1{print; print "if [%SPARK_CONNECT_MODE%] == [] set SPARK_CONNECT_MODE=1"; next} {print}' "$TARDIR/bin/spark-submit2.cmd" > tmp && cat tmp > "$TARDIR/bin/spark-submit2.cmd"
    rm tmp
    $TAR -czf "$TARDIR_NAME.tgz" -C "$SPARK_HOME" "$TARDIR_NAME"
    rm -rf "$TARDIR"
  fi
fi
