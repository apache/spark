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
# Arg3: Current PR Commit Hash
#+ the PR hash for the current commit
#

ghprbActualCommit="$1"
sha1="$2"
current_pr_head="$3"

MVN_BIN="build/mvn"
CURR_CP_FILE="my-classpath.txt"
MASTER_CP_FILE="master-classpath.txt"

# First switch over to the master branch
git checkout -f master
# Find and copy all pom.xml files into a *.gate file that we can check
# against through various `git` changes
find -name "pom.xml" -exec cp {} {}.gate \;
# Switch back to the current PR
git checkout -f "${current_pr_head}"

# Check if any *.pom files from the current branch are different from the master
difference_q=""
for p in $(find -name "pom.xml"); do
  [[ -f "${p}" && -f "${p}.gate" ]] && \
    difference_q="${difference_q}$(diff $p.gate $p)"
done

# If no pom files were changed we can easily say no new dependencies were added
if [ -z "${difference_q}" ]; then
  echo " * This patch does not change any dependencies."
else
  # Else we need to manually build spark to determine what, if any, dependencies
  # were added into the Spark assembly jar
  ${MVN_BIN} clean package dependency:build-classpath -DskipTests 2>/dev/null | \
    sed -n -e '/Building Spark Project Assembly/,$p' | \
    grep --context=1 -m 2 "Dependencies classpath:" | \
    head -n 3 | \
    tail -n 1 | \
    tr ":" "\n" | \
    rev | \
    cut -d "/" -f 1 | \
    rev | \
    sort > ${CURR_CP_FILE}

  # Checkout the master branch to compare against
  git checkout -f master

  ${MVN_BIN} clean package dependency:build-classpath -DskipTests 2>/dev/null | \
    sed -n -e '/Building Spark Project Assembly/,$p' | \
    grep --context=1 -m 2 "Dependencies classpath:" | \
    head -n 3 | \
    tail -n 1 | \
    tr ":" "\n" | \
    rev | \
    cut -d "/" -f 1 | \
    rev | \
    sort > ${MASTER_CP_FILE}

  DIFF_RESULTS="`diff ${CURR_CP_FILE} ${MASTER_CP_FILE}`"

  if [ -z "${DIFF_RESULTS}" ]; then
    echo " * This patch does not change any dependencies."
  else
    # Pretty print the new dependencies
    added_deps=$(echo "${DIFF_RESULTS}" | grep "<" | cut -d' ' -f2 | awk '{printf "   * \`"$1"\`\\n"}')
    removed_deps=$(echo "${DIFF_RESULTS}" | grep ">" | cut -d' ' -f2 | awk '{printf "   * \`"$1"\`\\n"}')
    added_deps_text=" * This patch **adds the following new dependencies:**\n${added_deps}"
    removed_deps_text=" * This patch **removes the following dependencies:**\n${removed_deps}"

    # Construct the final returned message with proper 
    return_mssg=""
    [ -n "${added_deps}" ] && return_mssg="${added_deps_text}"
    if [ -n "${removed_deps}" ]; then
      if [ -n "${return_mssg}" ]; then
        return_mssg="${return_mssg}\n${removed_deps_text}"
      else
        return_mssg="${removed_deps_text}"
      fi
    fi
    echo "${return_mssg}"
  fi
  
  # Remove the files we've left over
  [ -f "${CURR_CP_FILE}" ] && rm -f "${CURR_CP_FILE}"
  [ -f "${MASTER_CP_FILE}" ] && rm -f "${MASTER_CP_FILE}"

  # Clean up our mess from the Maven builds just in case
  ${MVN_BIN} clean &>/dev/null
fi
