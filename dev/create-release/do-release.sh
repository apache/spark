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

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

while getopts ":b:n" opt; do
  case $opt in
    b) GIT_BRANCH=$OPTARG ;;
    n) DRY_RUN=1 ;;
    \?) error "Invalid option: $OPTARG" ;;
  esac
done

if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  # Inside docker, need to import the GPG key stored in the current directory.
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --import "$SELF/gpg.key"

  # We may need to adjust the path since JAVA_HOME may be overridden by the driver script.
  if [ -n "$JAVA_HOME" ]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  else
    # JAVA_HOME for the openjdk package.
    export JAVA_HOME=/usr
  fi
else
  # Outside docker, need to ask for information about the release.
  get_release_info
fi

function should_build {
  local WHAT=$1
  [ -z "$RELEASE_STEP" ] || [ "$WHAT" = "$RELEASE_STEP" ]
}

if should_build "tag" && [ $SKIP_TAG = 0 ]; then
  run_silent "Creating release tag $RELEASE_TAG..." "tag.log" \
    "$SELF/release-tag.sh"
  echo "It may take some time for the tag to be synchronized to github."
  echo "Press enter when you've verified that the new tag ($RELEASE_TAG) is available."
  read
else
  echo "Skipping tag creation for $RELEASE_TAG."
fi

if should_build "build"; then
  run_silent "Building Spark..." "build.log" \
    "$SELF/release-build.sh" package
else
  echo "Skipping build step."
fi

if should_build "docs"; then
  run_silent "Building documentation..." "docs.log" \
    "$SELF/release-build.sh" docs
else
  echo "Skipping docs step."
fi

if should_build "publish"; then
  run_silent "Publishing release" "publish.log" \
    "$SELF/release-build.sh" publish-release
else
  echo "Skipping publish step."
fi

if [ ! -z "$RELEASE_STEP" ] && [ "$RELEASE_STEP" = "finalize" ]; then
  run_silent "Finalizing release" "finalize.log" \
    "$SELF/release-build.sh" finalize
fi
