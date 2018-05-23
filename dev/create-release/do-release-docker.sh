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
SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

WORKDIR=
IMGTAG=latest
while getopts "d:n:t:" opt; do
  case $opt in
    d) WORKDIR="$OPTARG" ;;
    n) DRY_RUN=1 ;;
    t) IMGTAG="$OPTARG" ;;
    ?) error "Invalid option: $OPTARG" ;;
  esac
done

if [ -z "$WORKDIR" ] || [ ! -d "$WORKDIR" ]; then
  error "Work directory (-d) must be defined and exist."
fi

if [ -d "$WORKDIR/output" ]; then
  read -p "Output directory already exists. Overwrite and continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    error "Exiting."
  fi
fi

cd "$WORKDIR"
rm -rf "$WORKDIR/output"
mkdir "$WORKDIR/output"

get_release_info

# Place all RM scripts and necessary data in a local directory that must be defined in the command
# line. This directory is mounted into the image.
for f in "$SELF"/*; do
  if [ -f "$f" ]; then
    cp "$f" "$WORKDIR"
  fi
done
GPG_KEY_FILE="$WORKDIR/gpg.key"
fcreate_secure "$GPG_KEY_FILE"
$GPG --export-secret-key --armor "$GPG_KEY" > "$GPG_KEY_FILE"

run_silent "Building spark-rm image with tag $IMGTAG..." "docker-build.log" \
  docker build -t "spark-rm:$IMGTAG" --build-arg UID=$UID "$SELF/spark-rm"

# Write the release information to a file with environment variables to be used when running the
# image.
ENVFILE="$WORKDIR/env.list"
fcreate_secure "$ENVFILE"

function cleanup {
  rm -f "$ENVFILE"
  rm -f "$GPG_KEY_FILE"
}

trap cleanup EXIT

cat > $ENVFILE <<EOF
DRY_RUN=$DRY_RUN
SKIP_TAG=$SKIP_TAG
RUNNING_IN_DOCKER=1
GIT_BRANCH=$GIT_BRANCH
NEXT_VERSION=$NEXT_VERSION
RELEASE_VERSION=$RELEASE_VERSION
RELEASE_TAG=$RELEASE_TAG
GIT_REF=$GIT_REF
SPARK_PACKAGE_VERSION=$SPARK_PACKAGE_VERSION
ASF_USERNAME=$ASF_USERNAME
GIT_NAME=$GIT_NAME
GIT_EMAIL=$GIT_EMAIL
GPG_KEY=$GPG_KEY
ASF_PASSWORD=$ASF_PASSWORD
GPG_PASSPHRASE=$GPG_PASSPHRASE
EOF

echo "Building $RELEASE_TAG; output will be at $WORKDIR/output"
docker run \
  --env-file "$ENVFILE" \
  --volume "$WORKDIR":/opt/spark-rm \
  "spark-rm:$IMGTAG"
