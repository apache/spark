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
# known as `ghprbActualCommit` in `run-tests-jenkins`
# Arg2: The SHA1 hash
# known as `sha1` in `run-tests-jenkins`
#

ghprbActualCommit="$1"
sha1="$2"

# check PR merge-ability
if [ "${sha1}" == "${ghprbActualCommit}" ]; then
  echo " * This patch **does not merge cleanly**."
else
  echo " * This patch merges cleanly."
fi
