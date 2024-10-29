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
set -ex

if [[ $# -gt 1 ]]; then
  echo "Illegal number of parameters."
  echo "Usage: ./dev/protobuf-breaking-changes-check.sh [branch]"
  echo "the default branch is 'master'"
  exit -1
fi

SPARK_HOME="$(cd "`dirname $0`"/..; pwd)"
cd "$SPARK_HOME"

BRANCH="master"
if [[ $# -eq 1 ]]; then
  BRANCH=$1
fi

pushd sql/connect/common/src/main &&
echo "Start protobuf breaking changes checking against $BRANCH" &&
buf breaking --against "https://github.com/apache/spark.git#branch=$BRANCH,subdir=connector/connect/common/src/main" &&
echo "Finsh protobuf breaking changes checking: SUCCESS"

if [[ $? -ne -0 ]]; then
  echo "Buf detected breaking changes for your Pull Request. Please verify."
  echo "Please make sure your branch is current against spark/$BRANCH."
  exit 1
fi

