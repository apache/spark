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

function exit_with_usage {
  local NAME=$(basename $0)
  cat << EOF
usage: $NAME
Tags a Spark release on a particular branch.

Inputs are specified with the following environment variables:
ASF_USERNAME - Apache Username
ASF_PASSWORD - Apache Password
GIT_NAME - Name to use with git
GIT_EMAIL - E-mail address to use with git
GIT_BRANCH - Git branch on which to make release
RELEASE_VERSION - Version used in pom files for release
RELEASE_TAG - Name of release tag
NEXT_VERSION - Development version after release
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

for env in ASF_USERNAME ASF_PASSWORD RELEASE_VERSION RELEASE_TAG NEXT_VERSION GIT_EMAIL GIT_NAME GIT_BRANCH; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done

init_java
init_maven_sbt

function uriencode { jq -nSRr --arg v "$1" '$v|@uri'; }

declare -r ENCODED_ASF_PASSWORD=$(uriencode "$ASF_PASSWORD")

rm -rf spark
git clone "https://$ASF_USERNAME:$ENCODED_ASF_PASSWORD@$ASF_SPARK_REPO" -b $GIT_BRANCH
cd spark

git config user.name "$GIT_NAME"
git config user.email "$GIT_EMAIL"

# Remove test jars and classes that do not belong to source releases.
rm $(<dev/test-jars.txt)
:> dev/test-jars.txt
rm $(<dev/test-classes.txt)
:> dev/test-classes.txt
git commit -a -m "Removing test jars and class files"
JAR_RM_REF=$(git rev-parse HEAD)

# Create release version
$MVN versions:set -DnewVersion=$RELEASE_VERSION | grep -v "no value" # silence logs
if [[ $RELEASE_VERSION != *"preview"* ]]; then
  # Set the release version in R/pkg/DESCRIPTION
  sed -i".tmp1" 's/Version.*$/Version: '"$RELEASE_VERSION"'/g' R/pkg/DESCRIPTION
else
  sed -i".tmp1" 's/-SNAPSHOT/'"-$(cut -d "-" -f 2 <<< $RELEASE_VERSION)"'/g' R/pkg/R/sparkR.R
fi
# Set the release version in docs
sed -i".tmp1" 's/SPARK_VERSION:.*$/SPARK_VERSION: '"$RELEASE_VERSION"'/g' docs/_config.yml
sed -i".tmp2" 's/SPARK_VERSION_SHORT:.*$/SPARK_VERSION_SHORT: '"$RELEASE_VERSION"'/g' docs/_config.yml
sed -i".tmp3" "s/'facetFilters':.*$/'facetFilters': [\"version:$RELEASE_VERSION\"]/g" docs/_config.yml
sed -i".tmp4" 's/__version__: str = .*$/__version__: str = "'"$RELEASE_VERSION"'"/' python/pyspark/version.py

git commit -a -m "Preparing Spark release $RELEASE_TAG"
echo "Creating tag $RELEASE_TAG at the head of $GIT_BRANCH"
git tag $RELEASE_TAG

# Restore test jars for dev.
git revert --no-edit $JAR_RM_REF

# Create next version
$MVN versions:set -DnewVersion=$NEXT_VERSION | grep -v "no value" # silence logs
# Remove -SNAPSHOT before setting the R version as R expects version strings to only have numbers
R_NEXT_VERSION=`echo $NEXT_VERSION | sed 's/-SNAPSHOT//g'`
sed -i".tmp5" 's/Version.*$/Version: '"$R_NEXT_VERSION"'/g' R/pkg/DESCRIPTION
# Write out the R_NEXT_VERSION to PySpark version info we use dev0 instead of SNAPSHOT to be closer
# to PEP440.
sed -i".tmp6" 's/__version__: str = .*$/__version__: str = "'"$R_NEXT_VERSION.dev0"'"/' python/pyspark/version.py

# Update docs with next version
sed -i".tmp7" 's/SPARK_VERSION:.*$/SPARK_VERSION: '"$NEXT_VERSION"'/g' docs/_config.yml
# Use R version for short version
sed -i".tmp8" 's/SPARK_VERSION_SHORT:.*$/SPARK_VERSION_SHORT: '"$R_NEXT_VERSION"'/g' docs/_config.yml
# Update the version index of DocSearch as the short version
sed -i".tmp9" "s/'facetFilters':.*$/'facetFilters': [\"version:$R_NEXT_VERSION\"]/g" docs/_config.yml

git commit -a -m "Preparing development version $NEXT_VERSION"

if ! is_dry_run; then
  # Push changes
  git push origin $RELEASE_TAG
  if [[ $RELEASE_VERSION != *"preview"* ]]; then
    git push origin HEAD:$GIT_BRANCH
    if git branch -r --contains tags/$RELEASE_TAG | grep origin; then
      echo "Pushed $RELEASE_TAG to $GIT_BRANCH."
    else
      echo "Failed to push $RELEASE_TAG to $GIT_BRANCH. Please start over."
      exit 1
    fi
  else
    echo "It's preview release. We only push $RELEASE_TAG to remote."
  fi

  cd ..
  rm -rf spark
else
  cd ..
  mv spark spark.tag
  echo "Clone with version changes and tag available as spark.tag in the output directory."
fi
