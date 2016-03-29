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

function exit_with_usage {
  cat << EOF
usage: release-build.sh <package|docs|publish-snapshot|publish-release>
Creates build deliverables from a Spark commit.

Top level targets are
  package: Create binary packages and copy them to home.apache
  docs: Build docs and copy them to home.apache
  publish-snapshot: Publish snapshot release to Apache snapshots
  publish-release: Publish a release to Apache release repo

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
SPARK_VERSION - Release identifier used when publishing
SPARK_PACKAGE_VERSION - Release identifier in top level package directory
REMOTE_PARENT_DIR - Parent in which to create doc or release builds.
REMOTE_PARENT_MAX_LENGTH - If set, parent directory will be cleaned to only
 have this number of subdirectories (by deleting old ones). WARNING: This deletes data.

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account
ASF_RSA_KEY - RSA private key file for ASF committer account

GPG_KEY - GPG key used to sign release artifacts
GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

for env in ASF_USERNAME ASF_RSA_KEY GPG_PASSPHRASE GPG_KEY; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

# Destination directory parent on remote server
REMOTE_PARENT_DIR=${REMOTE_PARENT_DIR:-/home/$ASF_USERNAME/public_html}

GPG="gpg --no-tty --batch"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=d63f592e7eac0 # Profile for Spark staging uploads
BASE_DIR=$(pwd)

MVN="build/mvn --force"
PUBLISH_PROFILES="-Pyarn -Phive -Phadoop-2.2"
PUBLISH_PROFILES="$PUBLISH_PROFILES -Pspark-ganglia-lgpl -Pkinesis-asl"

rm -rf spark
git clone https://git-wip-us.apache.org/repos/asf/spark.git
cd spark
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
echo "Checked out Spark git hash $git_hash"

if [ -z "$SPARK_VERSION" ]; then
  SPARK_VERSION=$($MVN help:evaluate -Dexpression=project.version \
    | grep -v INFO | grep -v WARNING | grep -v Download)
fi

if [ -z "$SPARK_PACKAGE_VERSION" ]; then
  SPARK_PACKAGE_VERSION="${SPARK_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

DEST_DIR_NAME="spark-$SPARK_PACKAGE_VERSION"

function LFTP {
  SSH="ssh -o ConnectTimeout=300 -o StrictHostKeyChecking=no -i $ASF_RSA_KEY"
  COMMANDS=$(cat <<EOF
     set net:max-retries 1 &&
     set sftp:connect-program $SSH &&
     connect -u $ASF_USERNAME,p sftp://home.apache.org &&
     $@
EOF
)
  lftp --norc -c "$COMMANDS"
}
export -f LFTP


git clean -d -f -x
rm .gitignore
rm -rf .git
cd ..

if [ -n "$REMOTE_PARENT_MAX_LENGTH" ]; then
  old_dirs=$(
    LFTP nlist $REMOTE_PARENT_DIR \
        | grep -v "^\." \
        | sort -r \
        | tail -n +$REMOTE_PARENT_MAX_LENGTH)
  for old_dir in $old_dirs; do
    echo "Removing directory: $old_dir"
    LFTP "rm -rf $REMOTE_PARENT_DIR/$old_dir && exit 0"
  done
fi

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging release tarballs"
  cp -r spark spark-$SPARK_VERSION
  tar cvzf spark-$SPARK_VERSION.tgz spark-$SPARK_VERSION
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour --output spark-$SPARK_VERSION.tgz.asc \
    --detach-sig spark-$SPARK_VERSION.tgz
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md MD5 spark-$SPARK_VERSION.tgz > \
    spark-$SPARK_VERSION.tgz.md5
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
    SHA512 spark-$SPARK_VERSION.tgz > spark-$SPARK_VERSION.tgz.sha
  rm -rf spark-$SPARK_VERSION

  # Updated for each binary build
  make_binary_release() {
    NAME=$1
    FLAGS=$2
    ZINC_PORT=$3
    cp -r spark spark-$SPARK_VERSION-bin-$NAME

    cd spark-$SPARK_VERSION-bin-$NAME

    # TODO There should probably be a flag to make-distribution to allow 2.11 support
    if [[ $FLAGS == *scala-2.11* ]]; then
      ./dev/change-scala-version.sh 2.11
    fi

    export ZINC_PORT=$ZINC_PORT
    echo "Creating distribution: $NAME ($FLAGS)"

    # Get maven home set by MVN
    MVN_HOME=`$MVN -version 2>&1 | grep 'Maven home' | awk '{print $NF}'`

    ./make-distribution.sh --name $NAME --mvn $MVN_HOME/bin/mvn --tgz $FLAGS \
      -DzincPort=$ZINC_PORT 2>&1 >  ../binary-release-$NAME.log
    cd ..
    cp spark-$SPARK_VERSION-bin-$NAME/spark-$SPARK_VERSION-bin-$NAME.tgz .

    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output spark-$SPARK_VERSION-bin-$NAME.tgz.asc \
      --detach-sig spark-$SPARK_VERSION-bin-$NAME.tgz
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
      MD5 spark-$SPARK_VERSION-bin-$NAME.tgz > \
      spark-$SPARK_VERSION-bin-$NAME.tgz.md5
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
      SHA512 spark-$SPARK_VERSION-bin-$NAME.tgz > \
      spark-$SPARK_VERSION-bin-$NAME.tgz.sha
  }

  # TODO: Check exit codes of children here:
  # http://stackoverflow.com/questions/1570262/shell-get-exit-code-of-background-process

  # We increment the Zinc port each time to avoid OOM's and other craziness if multiple builds
  # share the same Zinc server.
  make_binary_release "hadoop1" "-Psparkr -Phadoop-1 -Phive -Phive-thriftserver" "3030" &
  make_binary_release "hadoop1-scala2.11" "-Psparkr -Phadoop-1 -Phive -Dscala-2.11" "3031" &
  make_binary_release "cdh4" "-Psparkr -Phadoop-1 -Phive -Phive-thriftserver -Dhadoop.version=2.0.0-mr1-cdh4.2.0" "3032" &
  make_binary_release "hadoop2.3" "-Psparkr -Phadoop-2.3 -Phive -Phive-thriftserver -Pyarn" "3033" &
  make_binary_release "hadoop2.4" "-Psparkr -Phadoop-2.4 -Phive -Phive-thriftserver -Pyarn" "3034" &
  make_binary_release "hadoop2.6" "-Psparkr -Phadoop-2.6 -Phive -Phive-thriftserver -Pyarn" "3034" &
  make_binary_release "hadoop2.4-without-hive" "-Psparkr -Phadoop-2.4 -Pyarn" "3037" &
  make_binary_release "without-hadoop" "-Psparkr -Phadoop-provided -Pyarn" "3038" &
  wait
  rm -rf spark-$SPARK_VERSION-bin-*/

  # Copy data
  dest_dir="$REMOTE_PARENT_DIR/${DEST_DIR_NAME}-bin"
  echo "Copying release tarballs to $dest_dir"
  # Put to new directory:
  LFTP mkdir -p $dest_dir
  LFTP mput -O $dest_dir 'spark-*'
  # Delete /latest directory and rename new upload to /latest
  LFTP "rm -r -f $REMOTE_PARENT_DIR/latest || exit 0"
  LFTP mv $dest_dir "$REMOTE_PARENT_DIR/latest"
  # Re-upload a second time and leave the files in the timestamped upload directory:
  LFTP mkdir -p $dest_dir
  LFTP mput -O $dest_dir 'spark-*'
  exit 0
fi

if [[ "$1" == "docs" ]]; then
  # Documentation
  cd spark
  echo "Building Spark docs"
  dest_dir="$REMOTE_PARENT_DIR/${DEST_DIR_NAME}-docs"
  cd docs
  # Compile docs with Java 7 to use nicer format
  # TODO: Make configurable to add this: PRODUCTION=1
  PRODUCTION=1 RELEASE_VERSION="$SPARK_VERSION" jekyll build
  echo "Copying release documentation to $dest_dir"
  # Put to new directory:
  LFTP mkdir -p $dest_dir
  LFTP mirror -R _site $dest_dir
  # Delete /latest directory and rename new upload to /latest
  LFTP "rm -r -f $REMOTE_PARENT_DIR/latest || exit 0"
  LFTP mv $dest_dir "$REMOTE_PARENT_DIR/latest"
  # Re-upload a second time and leave the files in the timestamped upload directory:
  LFTP mkdir -p $dest_dir
  LFTP mirror -R _site $dest_dir
  cd ..
  exit 0
fi

if [[ "$1" == "publish-snapshot" ]]; then
  cd spark
  # Publish Spark to Maven release repo
  echo "Deploying Spark SNAPSHOT at '$GIT_REF' ($git_hash)"
  echo "Publish version is $SPARK_VERSION"
  if [[ ! $SPARK_VERSION == *"SNAPSHOT"* ]]; then
    echo "ERROR: Snapshots must have a version containing SNAPSHOT"
    echo "ERROR: You gave version '$SPARK_VERSION'"
    exit 1
  fi
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$SPARK_VERSION
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>apache.snapshots.https</id><username>$ASF_USERNAME</username>" >> $tmp_settings
  echo "<password>$ASF_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  # Generate random point for Zinc
  export ZINC_PORT=$(python -S -c "import random; print random.randrange(3030,4030)")

  $MVN -DzincPort=$ZINC_PORT --settings $tmp_settings -DskipTests $PUBLISH_PROFILES \
    -Phive-thriftserver deploy
  ./dev/change-scala-version.sh 2.11
  $MVN -DzincPort=$ZINC_PORT -Dscala-2.11 --settings $tmp_settings \
    -DskipTests $PUBLISH_PROFILES clean deploy

  # Clean-up Zinc nailgun process
  /usr/sbin/lsof -P |grep $ZINC_PORT | grep LISTEN | awk '{ print $2; }' | xargs kill

  rm $tmp_settings
  cd ..
  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  cd spark
  # Publish Spark to Maven release repo
  echo "Publishing Spark checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $SPARK_VERSION"
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$SPARK_VERSION

  # Using Nexus API documented here:
  # https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API
  echo "Creating Nexus staging repository"
  repo_request="<promoteRequest><data><description>Apache Spark $SPARK_VERSION (commit $git_hash)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
  staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachespark-[0-9]\{4\}\).*/\1/")
  echo "Created Nexus staging repository: $staged_repo_id"

  tmp_repo=$(mktemp -d spark-repo-XXXXX)

  # Generate random point for Zinc
  export ZINC_PORT=$(python -S -c "import random; print random.randrange(3030,4030)")

  $MVN -DzincPort=$ZINC_PORT -Dmaven.repo.local=$tmp_repo -DskipTests $PUBLISH_PROFILES \
    -Phive-thriftserver clean install

  ./dev/change-scala-version.sh 2.11

  $MVN -DzincPort=$ZINC_PORT -Dmaven.repo.local=$tmp_repo -Dscala-2.11 \
    -DskipTests $PUBLISH_PROFILES clean install

  # Clean-up Zinc nailgun process
  /usr/sbin/lsof -P |grep $ZINC_PORT | grep LISTEN | awk '{ print $2; }' | xargs kill

  ./dev/change-version-to-2.10.sh

  pushd $tmp_repo/org/apache/spark

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

  echo "Creating hash and signature files"
  for file in $(find . -type f)
  do
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
      --detach-sig --armour $file;
    if [ $(command -v md5) ]; then
      # Available on OS X; -q to keep only hash
      md5 -q $file > $file.md5
    else
      # Available on Linux; cut to keep only hash
      md5sum $file | cut -f1 -d' ' > $file.md5
    fi
    sha1sum $file | cut -f1 -d' ' > $file.sha1
  done

  nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
  echo "Uplading files to $nexus_upload"
  for file in $(find . -type f)
  do
    # strip leading ./
    file_short=$(echo $file | sed -e "s/\.\///")
    dest_url="$nexus_upload/org/apache/spark/$file_short"
    echo "  Uploading $file_short"
    curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
  done

  echo "Closing nexus staging repository"
  repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Spark $SPARK_VERSION (commit $git_hash)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
  echo "Closed Nexus staging repository: $staged_repo_id"
  popd
  rm -rf $tmp_repo
  cd ..
  exit 0
fi

cd ..
rm -rf spark
echo "ERROR: expects to be called with 'package', 'docs', 'publish-release' or 'publish-snapshot'"
