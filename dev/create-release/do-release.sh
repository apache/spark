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

while getopts "bn" opt; do
  case $opt in
    b) GIT_BRANCH=$OPTARG ;;
    n) DRY_RUN=1 ;;
    ?) error "Invalid option: $OPTARG" ;;
  esac
done

set -e

if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  # Inside docker, need to import the GPG key stored in the current directory.
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 "$SELF/gpg.key"
else
  # Outside docker, need to ask for information about the release.
  get_release_info
fi

if [ $SKIP_TAG = 0 ]; then
  run_silent "Creating release tag $RELEASE_TAG..." "tag.log" \
    "$SELF/release-tag.sh"
else
  echo "Skipping tag creation for $RELEASE_TAG."
fi

run_silent "Building Spark..." "build.log" \
  "$SELF/release-build.sh" package
run_silent "Building documentation..." "docs.log" \
  "$SELF/release-build.sh" docs
run_silent "Publishing release" "publish.log" \
  "$SELF/release-build.sh" publish-release
