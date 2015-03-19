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
# This script follows the base format for testing pull requests against
# another branch and returning results to be published. More details can be
# found at dev/run-tests-jenkins.
#
# Arg1: The Github Pull Request Actual Commit
#+ known as `ghprbActualCommit` in `run-tests-jenkins`
# Arg2: The SHA1 hash
#+ known as `sha1` in `run-tests-jenkins`
#

ghprbActualCommit="$1"
sha1="$2"

CURR_CP_FILE="my-classpath.txt"
MASTER_CP_FILE="master-classpath.txt"

curr_results="`./build/mvn dependency:build-classpath 2>&1`"
#./build/mvn dependency:build-classpath | \
#  grep -A 5 "Building Spark Project Assembly" | \
#  tail -n 1 | \
#  tr ":" "\n" | \
#  rev | \
#  cut -d "/" -f 1 | \
#  rev | \
#  sort > ${CURR_CP_FILE}

# Checkout the master branch to compare against
git checkout apache/master

master_results="`./build/mvn dependency:build-classpath 2>&1`"
#./build/mvn dependency:build-classpath | \
#  grep -A 5 "Building Spark Project Assembly" | \
#  tail -n 1 | \
#  tr ":" "\n" | \
#  rev | \
#  cut -d "/" -f 1 | \
#  rev | \
#  sort > ${MASTER_CP_FILE}

echo "CURRENT:${curr_results}\nMASTER:${master_results}"

#DIFF_RESULTS="`diff my-classpath.txt master-classpath.txt`"

#if [ -z "${DIFF_RESULTS}" ]; then
#  echo " * This patch adds no new dependencies"
#else
  # Pretty print the new dependencies
#  new_deps=$(echo ${DIFF_RESULTS} | grep "<" | cut -d" " -f2 | awk '{print "   * "$1}')
#  echo " * This patch **adds the following new dependencies:**\n${new_deps}"
#fi
  
