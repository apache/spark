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

set -e

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# TODO: This would be much nicer to do in SBT, once SBT supports Maven-style resolution.

# NOTE: These should match those in the release publishing script
HADOOP2_MODULE_PROFILES="-Phive-thriftserver -Pyarn -Phive"
MVN="build/mvn --force"
HADOOP_PROFILES=(
    hadoop-2.3
    hadoop-2.4
)

# We'll switch the version to a temp. one, publish POMs using that new version, then switch back to
# the old version. We need to do this because the `dependency:build-classpath` task needs to
# resolve Spark's internal submodule dependencies.

# See http://stackoverflow.com/a/3545363 for an explanation of this one-liner:
OLD_VERSION=$(mvn help:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)')
TEMP_VERSION="spark-$(date +%s | tail -c6)"

function reset_version {
  # Delete the temporary POMs that we wrote to the local Maven repo:
  find "$HOME/.m2/" | grep "$TEMP_VERSION" | xargs rm -rf

  # Restore the original version number:
  $MVN -q versions:set -DnewVersion=$OLD_VERSION -DgenerateBackupPoms=false > /dev/null
}
trap reset_version EXIT

$MVN -q versions:set -DnewVersion=$TEMP_VERSION -DgenerateBackupPoms=false > /dev/null

# Generate manifests for each Hadoop profile:
for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  echo "Performing Maven install for $HADOOP_PROFILE"
  $MVN $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE jar:jar install:install -q \
    -pl '!assembly' \
    -pl '!examples' \
    -pl '!external/flume-assembly' \
    -pl '!external/kafka-assembly' \
    -pl '!external/twitter' \
    -pl '!external/flume' \
    -pl '!external/mqtt' \
    -pl '!external/mqtt-assembly' \
    -pl '!external/zeromq' \
    -pl '!external/kafka' \
    -pl '!tags' \
    -DskipTests

  echo "Generating dependency manifest for $HADOOP_PROFILE"
  mkdir -p dev/pr-deps
  $MVN $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE dependency:build-classpath -pl assembly \
    | grep "Building Spark Project Assembly" -A 5 \
    | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
    | grep -v spark > dev/pr-deps/spark-deps-$HADOOP_PROFILE
done

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifests and creating new files at dev/deps"
  rm -rf dev/deps
  mv dev/pr-deps dev/deps
  exit 0
fi

for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  set +e
  dep_diff="$(
    git diff \
    --no-index \
    dev/deps/spark-deps-$HADOOP_PROFILE \
    dev/pr-deps/spark-deps-$HADOOP_PROFILE \
  )"
  set -e
  if [ "$dep_diff" != "" ]; then
    echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
    echo "To update the manifest file, run './dev/test-dependencies.sh --replace-manifest'."
    echo "$dep_diff"
    rm -rf dev/pr-deps
    exit 1
  fi
done
