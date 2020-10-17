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
# Creates a Spark release candidate. The script will update versions, tag the branch,
# build Spark binary packages and documentation, and upload maven artifacts to a staging
# repository. There is also a dry run mode where only local builds are performed, and
# nothing is uploaded to the ASF repos.
#
# Run with "-h" for options.
#

set -e
SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function usage {
  local NAME=$(basename $0)
  cat <<EOF
Usage: $NAME [options]

This script runs the release scripts inside a docker image. The image is hardcoded to be called
"spark-rm" and will be re-generated (as needed) on every invocation of this script.

Options are:

  -d [path]   : required: working directory (output will be written to an "output" directory in
                the working directory).
  -n          : dry run mode. Performs checks and local builds, but do not upload anything.
  -t [tag]    : tag for the spark-rm docker image to use for building (default: "latest").
  -j [path]   : path to local JDK installation to use for building. By default the script will
                use openjdk8 installed in the docker image.
  -s [step]   : runs a single step of the process; valid steps are: tag, build, docs, publish
EOF
}

WORKDIR=
IMGTAG=latest
JAVA=
RELEASE_STEP=
while getopts "d:hj:ns:t:" opt; do
  case $opt in
    d) WORKDIR="$OPTARG" ;;
    n) DRY_RUN=1 ;;
    t) IMGTAG="$OPTARG" ;;
    j) JAVA="$OPTARG" ;;
    s) RELEASE_STEP="$OPTARG" ;;
    h) usage ;;
    ?) error "Invalid option. Run with -h for help." ;;
  esac
done

if [ -z "$WORKDIR" ] || [ ! -d "$WORKDIR" ]; then
  error "Work directory (-d) must be defined and exist. Run with -h for help."
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
RELEASE_STEP=$RELEASE_STEP
USER=$USER
ZINC_OPTS=${RELEASE_ZINC_OPTS:-"-Xmx4g -XX:ReservedCodeCacheSize=2g"}
EOF

JAVA_VOL=
if [ -n "$JAVA" ]; then
  echo "JAVA_HOME=/opt/spark-java" >> $ENVFILE
  JAVA_VOL="--volume $JAVA:/opt/spark-java"
fi

# SPARK-24530: Sphinx must work with python 3 to generate doc correctly.
echo "SPHINXPYTHON=/opt/p35/bin/python" >> $ENVFILE

echo "Building $RELEASE_TAG; output will be at $WORKDIR/output"
docker run -ti \
  --env-file "$ENVFILE" \
  --volume "$WORKDIR:/opt/spark-rm" \
  $JAVA_VOL \
  "spark-rm:$IMGTAG"
