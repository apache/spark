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

# Quick-and-dirty automation of making maven and binary releases. Not robust at all.
# Publishes releases to Maven and packages/copies binary release artifacts.
# Expects to be run in a totally empty directory.
#
# Options:
#  --package-only   only packages an existing release candidate
#
# Would be nice to add:
#  - Send output to stderr and have useful logging in stdout

# Note: The following variables must be set before use!
ASF_USERNAME=${ASF_USERNAME:-pwendell}
ASF_PASSWORD=${ASF_PASSWORD:-XXX}
GPG_PASSPHRASE=${GPG_PASSPHRASE:-XXX}
GIT_BRANCH=${GIT_BRANCH:-branch-1.0}
RELEASE_VERSION=${RELEASE_VERSION:-1.2.0}
NEXT_VERSION=${NEXT_VERSION:-1.2.1}
RC_NAME=${RC_NAME:-rc2}

M2_REPO=~/.m2/repository
SPARK_REPO=$M2_REPO/org/apache/spark
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_UPLOAD=$NEXUS_ROOT/deploy/maven2
NEXUS_PROFILE=d63f592e7eac0 # Profile for Spark staging uploads

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi
JAVA_7_HOME=${JAVA_7_HOME:-$JAVA_HOME}

set -e

GIT_TAG=v$RELEASE_VERSION-$RC_NAME

if [[ ! "$@" =~ --package-only ]]; then
  echo "Creating release commit and publishing to Apache repository"
  # Artifact publishing
  git clone https://$ASF_USERNAME:$ASF_PASSWORD@git-wip-us.apache.org/repos/asf/spark.git \
    -b $GIT_BRANCH
  pushd spark
  export MAVEN_OPTS="-Xmx3g -XX:MaxPermSize=1g -XX:ReservedCodeCacheSize=1g"

  # Create release commits and push them to github
  # NOTE: This is done "eagerly" i.e. we don't check if we can succesfully build
  # or before we coin the release commit. This helps avoid races where
  # other people add commits to this branch while we are in the middle of building.
  old="  <version>${RELEASE_VERSION}-SNAPSHOT<\/version>"
  new="  <version>${RELEASE_VERSION}<\/version>"
  find . -name pom.xml -o -name package.scala | grep -v dev | xargs -I {} sed -i \
    -e "s/$old/$new/" {}
  git commit -a -m "Preparing Spark release $GIT_TAG"
  echo "Creating tag $GIT_TAG at the head of $GIT_BRANCH"
  git tag $GIT_TAG

  old="  <version>${RELEASE_VERSION}<\/version>"
  new="  <version>${NEXT_VERSION}-SNAPSHOT<\/version>"
  find . -name pom.xml -o -name package.scala | grep -v dev | xargs -I {} sed -i \
    -e "s/$old/$new/" {}
  git commit -a -m "Preparing development version ${NEXT_VERSION}-SNAPSHOT"
  git push origin $GIT_TAG
  git push origin HEAD:$GIT_BRANCH
  git checkout -f $GIT_TAG 
  
  # Using Nexus API documented here:
  # https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API
  echo "Creating Nexus staging repository"
  repo_request="<promoteRequest><data><description>Apache Spark $GIT_TAG</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
  staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachespark-[0-9]\{4\}\).*/\1/")
  echo "Created Nexus staging repository: $staged_repo_id"

  rm -rf $SPARK_REPO

  mvn -DskipTests -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 \
    -Pyarn -Phive -Phadoop-2.2 -Pspark-ganglia-lgpl -Pkinesis-asl \
    clean install

  ./dev/change-version-to-2.11.sh
  
  mvn -DskipTests -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 \
    -Dscala-2.11 -Pyarn -Phive -Phadoop-2.2 -Pspark-ganglia-lgpl -Pkinesis-asl \
    clean install

  ./dev/change-version-to-2.10.sh

  pushd $SPARK_REPO

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

  echo "Creating hash and signature files"
  for file in $(find . -type f)
  do
    echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --output $file.asc --detach-sig --armour $file;
    gpg --print-md MD5 $file > $file.md5;
    gpg --print-md SHA1 $file > $file.sha1
  done

  echo "Uplading files to $NEXUS_UPLOAD"
  for file in $(find . -type f)
  do
    # strip leading ./
    file_short=$(echo $file | sed -e "s/\.\///")
    dest_url="$NEXUS_UPLOAD/org/apache/spark/$file_short"
    echo "  Uploading $file_short"
    curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
  done

  echo "Closing nexus staging repository"
  repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Spark $GIT_TAG</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
  echo "Closed Nexus staging repository: $staged_repo_id"

  popd
  popd
  rm -rf spark
fi

# Source and binary tarballs
echo "Packaging release tarballs"
git clone https://git-wip-us.apache.org/repos/asf/spark.git
cd spark
git checkout --force $GIT_TAG
release_hash=`git rev-parse HEAD`

rm .gitignore
rm -rf .git
cd ..

cp -r spark spark-$RELEASE_VERSION
tar cvzf spark-$RELEASE_VERSION.tgz spark-$RELEASE_VERSION
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --armour --output spark-$RELEASE_VERSION.tgz.asc \
  --detach-sig spark-$RELEASE_VERSION.tgz
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md MD5 spark-$RELEASE_VERSION.tgz > \
  spark-$RELEASE_VERSION.tgz.md5
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md SHA512 spark-$RELEASE_VERSION.tgz > \
  spark-$RELEASE_VERSION.tgz.sha
rm -rf spark-$RELEASE_VERSION

make_binary_release() {
  NAME=$1
  FLAGS=$2
  cp -r spark spark-$RELEASE_VERSION-bin-$NAME
  
  cd spark-$RELEASE_VERSION-bin-$NAME

  # TODO There should probably be a flag to make-distribution to allow 2.11 support
  if [[ $FLAGS == *scala-2.11* ]]; then
    ./dev/change-version-to-2.11.sh
  fi

  ./make-distribution.sh --name $NAME --tgz $FLAGS 2>&1 | tee ../binary-release-$NAME.log
  cd ..
  cp spark-$RELEASE_VERSION-bin-$NAME/spark-$RELEASE_VERSION-bin-$NAME.tgz .
  rm -rf spark-$RELEASE_VERSION-bin-$NAME

  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --armour \
    --output spark-$RELEASE_VERSION-bin-$NAME.tgz.asc \
    --detach-sig spark-$RELEASE_VERSION-bin-$NAME.tgz
  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md \
    MD5 spark-$RELEASE_VERSION-bin-$NAME.tgz > \
    spark-$RELEASE_VERSION-bin-$NAME.tgz.md5
  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md \
    SHA512 spark-$RELEASE_VERSION-bin-$NAME.tgz > \
    spark-$RELEASE_VERSION-bin-$NAME.tgz.sha
}


make_binary_release "hadoop1" "-Phive -Phive-thriftserver -Dhadoop.version=1.0.4" &
make_binary_release "hadoop1-scala2.11" "-Phive -Dscala-2.11" &
make_binary_release "cdh4" "-Phive -Phive-thriftserver -Dhadoop.version=2.0.0-mr1-cdh4.2.0" &
make_binary_release "hadoop2.3" "-Phadoop-2.3 -Phive -Phive-thriftserver -Pyarn" &
make_binary_release "hadoop2.4" "-Phadoop-2.4 -Phive -Phive-thriftserver -Pyarn" &
make_binary_release "mapr3" "-Pmapr3 -Phive -Phive-thriftserver" &
make_binary_release "mapr4" "-Pmapr4 -Pyarn -Phive -Phive-thriftserver" &
make_binary_release "hadoop2.4-without-hive" "-Phadoop-2.4 -Pyarn" &
wait

# Copy data
echo "Copying release tarballs"
rc_folder=spark-$RELEASE_VERSION-$RC_NAME
ssh $ASF_USERNAME@people.apache.org \
  mkdir /home/$ASF_USERNAME/public_html/$rc_folder
scp spark-* \
  $ASF_USERNAME@people.apache.org:/home/$ASF_USERNAME/public_html/$rc_folder/

# Docs
cd spark
sbt/sbt clean
cd docs
# Compile docs with Java 7 to use nicer format
JAVA_HOME=$JAVA_7_HOME PRODUCTION=1 jekyll build
echo "Copying release documentation"
rc_docs_folder=${rc_folder}-docs
ssh $ASF_USERNAME@people.apache.org \
  mkdir /home/$ASF_USERNAME/public_html/$rc_docs_folder
rsync -r _site/* $ASF_USERNAME@people.apache.org:/home/$ASF_USERNAME/public_html/$rc_docs_folder

echo "Release $RELEASE_VERSION completed:"
echo "Git tag:\t $GIT_TAG"
echo "Release commit:\t $release_hash"
echo "Binary location:\t http://people.apache.org/~$ASF_USERNAME/$rc_folder"
echo "Doc location:\t http://people.apache.org/~$ASF_USERNAME/$rc_docs_folder"
