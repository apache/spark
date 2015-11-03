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

# TODO: This would be much nicer to do in SBT, once SBT supports Maven-style
# resolution.

MVN="build/mvn --force"
# NOTE: These should match those in the release publishing script
HADOOP2_MODULE_PROFILES="-Phive-thriftserver -Pyarn -Phive"
LOCAL_REPO="mvn-tmp"

if [ -n "$AMPLAB_JENKINS" ]; then
  # To speed up Maven install process we remove source files
  # Maven dependency list only works once installed
  find . -name *.scala | xargs rm
  find . -name *.java | xargs rm
fi

# Use custom version to avoid Maven contention
spark_version="spark-$(date +%s | tail -c6)"
$MVN -q versions:set -DnewVersion=$spark_version > /dev/null

echo "Performing Maven install"
$MVN $HADOOP2_MODULE_PROFILES install -q \
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
  -DskipTests

echo "Generating dependency manifest"

$MVN -Phadoop-1 dependency:build-classpath -pl assembly \
  | grep "Building Spark Project Assembly" -A 5 \
  | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
  | grep -v spark > dev/pr-deps-hadoop1


$MVN $HADOOP2_MODULE_PROFILES -Phadoop-2.4 dependency:build-classpath -pl assembly \
  | grep "Building Spark Project Assembly" -A 5 \
  | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
  | grep -v spark > dev/pr-deps-hadoop24

if [ -n "$AMPLAB_JENKINS" ]; then
  git reset --hard HEAD
fi

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifest and creating new file at dev/spark-deps"
  mv dev/pr-deps-hadoop1 dev/spark-deps-hadoop1
  mv dev/pr-deps-hadoop24 dev/spark-deps-hadoop24
  exit 0
fi

set +e
dep_diff="$(diff dev/pr-deps-hadoop1 dev/spark-deps-hadoop1)"
dep_diff="$(diff dev/pr-deps-hadoop24 dev/spark-deps-hadoop24)"
set -e

if [ "$dep_diff" != "" ]; then
  echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
  echo "To update the manifest file, run './dev/test-dependencies --replace-manifest'."
  echo "$dep_diff"
  exit 1
fi
