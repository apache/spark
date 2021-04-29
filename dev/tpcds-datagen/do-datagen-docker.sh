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
# Generate TPC-DS data for TPCDSQUeryTestSuite.
# Run with "-h" for options.
#

set -e
SELF=$(cd $(dirname $0) && pwd)

# Re-uses helper funcs for the release scripts
. "$SELF/../create-release/release-util.sh"

function usage {
  local NAME=$(basename $0)
  cat <<EOF
Usage: $NAME [options]

This script generates TPC-DS data for TPCDSQUeryTestSuite inside a docker image. The image is hardcoded to be called
"spark-tpcds" and will be re-generated (as needed) on every invocation of this script.

Options are:

  -d [path]   : required: working directory (output will be written to an "output" directory in
                the working directory).
EOF
}

WORKDIR=
IMGTAG=latest
while getopts ":d:h" opt; do
  case $opt in
    d) WORKDIR="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) error "Invalid option. Run with -h for help." ;;
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

# Place all scripts in a local directory that must be defined in the command
# line. This directory is mounted into the image.
for f in "$SELF"/*; do
  if [ -f "$f" ]; then
    cp "$f" "$WORKDIR"
  fi
done

# Place `release-util.sh` for reuse
cp "$SELF/../create-release/release-util.sh" "$WORKDIR"

run_silent "Building spark-tpcds image with tag $IMGTAG..." "docker-build.log" \
  docker build -t "spark-tpcds:$IMGTAG" --build-arg UID=$UID "$SELF/spark-tpcds"

# Write the release information to a file with environment variables to be used when running the
# image.
ENVFILE="$WORKDIR/env.list"
fcreate_secure "$ENVFILE"

function cleanup {
  rm -f "$ENVFILE"
}

trap cleanup EXIT

cat > $ENVFILE <<EOF
RUNNING_IN_DOCKER=1
EOF

echo "Building Spark to generate TPC-DS data; output will be at $WORKDIR/output/tpcds-data"
docker run -ti \
  --env-file "$ENVFILE" \
  --volume "$WORKDIR:/opt/spark-tpcds" \
  "spark-tpcds:$IMGTAG"
