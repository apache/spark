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

VALID_VERSIONS=( 2.13 3 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.13" ]; then
  FROM_VERSION="3"
else
  FROM_VERSION="2.13"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

BASEDIR=$(dirname $0)/..
for f in $(find "$BASEDIR" -name 'pom.xml' -not -path '*target*'); do
  echo $f
  # Anchor to the <artifactId> element: a looser match also corrupts property names
  # ending in "artifactId".
  sed_i 's|\(<artifactId>[^<]*\)_'$FROM_VERSION'<|\1_'$TO_VERSION'<|g' $f
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$TO_VERSION' -->\)<!--/\1/' $f
  sed_i 's/^\([[:space:]]*\)-->\(<!-- #endif scala-'$TO_VERSION' -->\)/\1\2/' $f
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$FROM_VERSION' -->\)$/\1<!--/' $f
  sed_i 's/^\([[:space:]]*\)\(<!-- #endif scala-'$FROM_VERSION' -->\)/\1-->\2/' $f
done

# dependency:get is workaround for SPARK-34762 to download the JAR file of commons-cli.
# Without this, build with Scala 2.13 using SBT will fail because the help plugin used below downloads only the POM file.
COMMONS_CLI_VERSION=`build/mvn help:evaluate -Dexpression=commons-cli.version -q -DforceStdout`
build/mvn dependency:get -Dartifact=commons-cli:commons-cli:${COMMONS_CLI_VERSION} -q

# Swap scala.version to the canonical property reference (not a literal), so each full
# version is defined once and the switch does not depend on a profile being evaluable.
if [ $TO_VERSION = "3" ]; then
  SCALA_VERSION_REF='${scala3.version}'
else
  SCALA_VERSION_REF='${scala213.version}'
fi
sed_i '1,/<scala\.version>/s|<scala\.version>[^<]*</scala\.version>|<scala.version>'"$SCALA_VERSION_REF"'</scala.version>|' \
  "$BASEDIR/pom.xml"

# Also update <scala.binary.version>. The pattern must match a dotless version too:
# Scala 3 artifacts are suffixed _3, not _3.3.
sed_i '1,/<scala\.binary\.version>[0-9][0-9.]*</s/<scala\.binary\.version>[0-9][0-9.]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"

# Update source of scaladocs. 2.13 is the default and needs no flag; only Scala 3 is
# selected with -Pscala-3, so this tracks the non-default version rather than the target.
echo "$BASEDIR/docs/_plugins/build_api_docs.rb"
if [ $TO_VERSION = "3" ]; then
  sed_i '/\-Pscala-'$TO_VERSION'/!s:build/sbt:build/sbt \-Pscala\-'$TO_VERSION':' "$BASEDIR/docs/_plugins/build_api_docs.rb"
else
  sed_i 's:build/sbt \-Pscala\-'$FROM_VERSION':build/sbt:' "$BASEDIR/docs/_plugins/build_api_docs.rb"
fi
sed_i 's/scala\-'$FROM_VERSION'/scala\-'$TO_VERSION'/' "$BASEDIR/docs/_plugins/build_api_docs.rb"

echo "$BASEDIR/dev/mima"
if [ $TO_VERSION = "3" ]; then
  sed_i '/\-Pscala-'$TO_VERSION'/!s:build/sbt:build/sbt \-Pscala\-'$TO_VERSION':' "$BASEDIR/dev/mima"
  sed_i '/\-Pscala-'$TO_VERSION'/!s;SPARK_PROFILES=\${1:\-";SPARK_PROFILES=\${1:\-"\-Pscala\-'$TO_VERSION' ;' \
    "$BASEDIR/dev/mima"
else
  sed_i 's/\-Pscala\-'$FROM_VERSION' //' "$BASEDIR/dev/mima"
fi
chmod 775 "$BASEDIR/dev/mima"
