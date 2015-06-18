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

usage() {
  echo "Usage: $(basename $0) <from-version> <to-version>" 1>&2
  exit 1
}

if [ $# -ne 2 ]; then
  echo "Wrong number of arguments" 1>&2
  usage
fi

FROM_VERSION=$1
TO_VERSION=$2

VALID_VERSIONS=( 2.10 2.11 )

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$FROM_VERSION"
check_scala_version "$TO_VERSION"

test_sed() {
  [ ! -z "$($1 --version 2>&1 | head -n 1 | grep 'GNU sed')" ]
}

# Find GNU sed. On OS X with MacPorts you can install gsed with "sudo port install gsed"
if test_sed sed; then
  SED=sed
elif test_sed gsed; then
  SED=gsed
else
  echo "Could not find GNU sed. Tried \"sed\" and \"gsed\"" 1>&2
  exit 1
fi

BASEDIR=$(dirname $0)/..
find $BASEDIR -name 'pom.xml' | grep -v target \
  | xargs -I {} $SED -i -e 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' {}

# Also update <scala.binary.version> in parent POM
$SED -i -e '0,/<scala\.binary\.version>'$FROM_VERSION'</s//<scala.binary.version>'$TO_VERSION'</' $BASEDIR/pom.xml

# Update source of scaladocs
$SED -i -e 's/scala\-'$FROM_VERSION'/scala\-'$TO_VERSION'/' $BASEDIR/docs/_plugins/copy_api_dirs.rb
