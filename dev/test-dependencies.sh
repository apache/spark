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

# We'll switch the version to a temp. one, publish POMs using that new version, then switch back to
# the old version. We need to do this because the `dependency:build-classpath` task needs to
# resolve Spark's internal submodule dependencies.

# See http://stackoverflow.com/a/3545363 for an explanation of this one-liner:
OLD_VERSION=$(mvn help:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)')
TEMP_VERSION="spark-$(date +%s | tail -c6)"
$MVN -q versions:set -DnewVersion=$TEMP_VERSION -DgenerateBackupPoms=false > /dev/null

echo "Performing Maven install"
$MVN $HADOOP2_MODULE_PROFILES jar:jar install:install \
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

echo "Generating dependency manifest"
$MVN $HADOOP2_MODULE_PROFILES -Phadoop-2.4 dependency:build-classpath -pl assembly \
  | grep "Building Spark Project Assembly" -A 5 \
  | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
  | grep -v spark > dev/pr-deps-hadoop24

# Restore the original version number:
$MVN -q versions:set -DnewVersion=$OLD_VERSION > /dev/null

# Delete the temporary POMs that we wrote to the local Maven repo:
find "$HOME/.m2/" | grep "$TEMP_VERSION" | xargs rm -rf

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifest and creating new file at dev/spark-deps"
  mv dev/pr-deps-hadoop24 dev/spark-deps-hadoop24
  exit 0
fi

set +e
dep_diff="$(diff dev/pr-deps-hadoop24 dev/spark-deps-hadoop24)"
set -e

if [ "$dep_diff" != "" ]; then
  echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
  echo "To update the manifest file, run './dev/test-dependencies --replace-manifest'."
  echo "$dep_diff"
  exit 1
fi
