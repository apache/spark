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
  cat << EOF
usage: release-build.sh <package|docs|publish-snapshot|publish-release|finalize>
Creates build deliverables from a Spark commit.

Top level targets are
  package: Create binary packages and commit them to dist.apache.org/repos/dist/dev/spark/
  docs: Build docs and commit them to dist.apache.org/repos/dist/dev/spark/
  publish-snapshot: Publish snapshot release to Apache snapshots
  publish-release: Publish a release to Apache release repo
  finalize: Finalize the release after an RC passes vote

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
SPARK_PACKAGE_VERSION - Release identifier in top level package directory (e.g. 2.1.2-rc1)
SPARK_VERSION - (optional) Version of Spark being built (e.g. 2.1.2)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

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

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

if [[ -z "$GPG_PASSPHRASE" ]]; then
  echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
  echo 'unlock the GPG signing key that will be used to sign the release!'
  echo
  stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
fi

for env in ASF_USERNAME GPG_PASSPHRASE GPG_KEY; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/spark"
RELEASE_LOCATION="https://dist.apache.org/repos/dist/release/spark"

GPG="gpg -u $GPG_KEY --no-tty --batch --pinentry-mode loopback"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=d63f592e7eac0 # Profile for Spark staging uploads
BASE_DIR=$(pwd)

init_java
init_maven_sbt

if [[ "$1" == "finalize" ]]; then
  if [[ -z "$PYPI_API_TOKEN" ]]; then
    error 'The environment variable PYPI_API_TOKEN is not set. Exiting.'
  fi

  git config --global user.name "$GIT_NAME"
  git config --global user.email "$GIT_EMAIL"

  # Create the git tag for the new release
  echo "Creating the git tag for the new release"
  if check_for_tag "v$RELEASE_VERSION"; then
    echo "v$RELEASE_VERSION already exists. Skip creating it."
  else
    rm -rf spark
    git clone "https://$ASF_USERNAME:$ASF_PASSWORD@$ASF_SPARK_REPO" -b master
    cd spark
    git tag "v$RELEASE_VERSION" "$RELEASE_TAG"
    git push origin "v$RELEASE_VERSION"
    cd ..
    rm -rf spark
    echo "git tag v$RELEASE_VERSION created"
  fi

  # download PySpark binary from the dev directory and upload to PyPi.
  echo "Uploading PySpark to PyPi"
  svn co --depth=empty "$RELEASE_STAGING_LOCATION/$RELEASE_TAG-bin" svn-spark
  cd svn-spark
  PYSPARK_VERSION=`echo "$RELEASE_VERSION" |  sed -e "s/-/./" -e "s/preview/dev/"`
  svn update "pyspark-$PYSPARK_VERSION.tar.gz"
  svn update "pyspark-$PYSPARK_VERSION.tar.gz.asc"
  twine upload -u __token__  -p $PYPI_API_TOKEN \
    --repository-url https://upload.pypi.org/legacy/ \
    "pyspark-$RELEASE_VERSION.tar.gz" \
    "pyspark-$RELEASE_VERSION.tar.gz.asc"
  cd ..
  rm -rf svn-spark
  echo "PySpark uploaded"

  # download the docs from the dev directory and upload it to spark-website
  echo "Uploading docs to spark-website"
  svn co "$RELEASE_STAGING_LOCATION/$RELEASE_TAG-docs" docs
  git clone "https://$ASF_USERNAME:$ASF_PASSWORD@gitbox.apache.org/repos/asf/spark-website.git" -b asf-site
  mv docs/_site "spark-website/site/docs/$RELEASE_VERSION"
  cd spark-website
  git add site/docs/$RELEASE_VERSION
  git commit -m "Add docs for Apache Spark $RELEASE_VERSION"
  git push origin HEAD:asf-site
  cd ..
  rm -rf spark-website
  svn rm --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Remove RC artifacts" --no-auth-cache \
    "$RELEASE_STAGING_LOCATION/$RELEASE_TAG-docs"
  echo "docs uploaded"

  # Moves the binaries from dev directory to release directory.
  echo "Moving Spark binaries to the release directory"
  svn mv --username "$ASF_USERNAME" --password "$ASF_PASSWORD" -m"Apache Spark $RELEASE_VERSION" \
    --no-auth-cache "$RELEASE_STAGING_LOCATION/$RELEASE_TAG-bin" "$RELEASE_LOCATION/spark-$RELEASE_VERSION"
  echo "Spark binaries moved"

  # Update the KEYS file.
  echo "Sync'ing KEYS"
  svn co --depth=files "$RELEASE_LOCATION" svn-spark
  curl "$RELEASE_STAGING_LOCATION/KEYS" > svn-spark/KEYS
  (cd svn-spark && svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS")
  echo "KEYS sync'ed"
  rm -rf svn-spark

  exit 0
fi

rm -rf spark
git clone "$ASF_REPO"
cd spark
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
export GIT_HASH=$git_hash
echo "Checked out Spark git hash $git_hash"

if [ -z "$SPARK_VERSION" ]; then
  # Run $MVN in a separate command so that 'set -e' does the right thing.
  TMP=$(mktemp)
  $MVN help:evaluate -Dexpression=project.version > $TMP
  SPARK_VERSION=$(cat $TMP | grep -v INFO | grep -v WARNING | grep -vi Download)
  rm $TMP
fi

# Depending on the version being built, certain extra profiles need to be activated, and
# different versions of Scala are supported.
BASE_PROFILES="-Pmesos -Pyarn -Pkubernetes"

PUBLISH_SCALA_2_13=1
SCALA_2_13_PROFILES="-Pscala-2.13"
if [[ $SPARK_VERSION < "3.2" ]]; then
  PUBLISH_SCALA_2_13=0
fi

PUBLISH_SCALA_2_12=1
SCALA_2_12_PROFILES="-Pscala-2.12"

# Hive-specific profiles for some builds
HIVE_PROFILES="-Phive -Phive-thriftserver"
# Profiles for publishing snapshots and release to Maven Central
# We use Apache Hive 2.3 for publishing
PUBLISH_PROFILES="$BASE_PROFILES $HIVE_PROFILES -Pspark-ganglia-lgpl -Pkinesis-asl -Phadoop-cloud"
# Profiles for building binary releases
BASE_RELEASE_PROFILES="$BASE_PROFILES -Psparkr"

if [[ $JAVA_VERSION < "1.8." ]]; then
  echo "Java version $JAVA_VERSION is less than required 1.8 for 2.2+"
  echo "Please set JAVA_HOME correctly."
  exit 1
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof. Please see SPARK-22377 and the discussion
# in its pull request.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

if [ -z "$SPARK_PACKAGE_VERSION" ]; then
  SPARK_PACKAGE_VERSION="${SPARK_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

DEST_DIR_NAME="$SPARK_PACKAGE_VERSION"

git clean -d -f -x
rm -f .gitignore
cd ..

export MAVEN_OPTS="-Xss128m -Xmx12g"

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging release source tarballs"
  cp -r spark spark-$SPARK_VERSION

  rm -f spark-$SPARK_VERSION/LICENSE-binary
  rm -f spark-$SPARK_VERSION/NOTICE-binary
  rm -rf spark-$SPARK_VERSION/licenses-binary

  tar cvzf spark-$SPARK_VERSION.tgz --exclude spark-$SPARK_VERSION/.git spark-$SPARK_VERSION
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour --output spark-$SPARK_VERSION.tgz.asc \
    --detach-sig spark-$SPARK_VERSION.tgz
  shasum -a 512 spark-$SPARK_VERSION.tgz > spark-$SPARK_VERSION.tgz.sha512
  rm -rf spark-$SPARK_VERSION

  # Updated for each binary build
  make_binary_release() {
    NAME=$1
    FLAGS="$MVN_EXTRA_OPTS -B $BASE_RELEASE_PROFILES $2"
    # BUILD_PACKAGE can be "withpip", "withr", or both as "withpip,withr"
    BUILD_PACKAGE=$3
    SCALA_VERSION=$4

    PIP_FLAG=""
    if [[ $BUILD_PACKAGE == *"withpip"* ]]; then
      PIP_FLAG="--pip"
    fi
    R_FLAG=""
    if [[ $BUILD_PACKAGE == *"withr"* ]]; then
      R_FLAG="--r"
    fi

    echo "Building binary dist $NAME"
    cp -r spark spark-$SPARK_VERSION-bin-$NAME
    cd spark-$SPARK_VERSION-bin-$NAME

    ./dev/change-scala-version.sh $SCALA_VERSION

    echo "Creating distribution: $NAME ($FLAGS)"

    # Write out the VERSION to PySpark version info we rewrite the - into a . and SNAPSHOT
    # to dev0 to be closer to PEP440.
    PYSPARK_VERSION=`echo "$SPARK_VERSION" |  sed -e "s/-/./" -e "s/SNAPSHOT/dev0/" -e "s/preview/dev/"`

    if [[ $SPARK_VERSION == 3.0* ]] || [[ $SPARK_VERSION == 3.1* ]] || [[ $SPARK_VERSION == 3.2* ]]; then
      echo "__version__ = '$PYSPARK_VERSION'" > python/pyspark/version.py
    else
      echo "__version__: str = '$PYSPARK_VERSION'" > python/pyspark/version.py
    fi

    # Get maven home set by MVN
    MVN_HOME=`$MVN -version 2>&1 | grep 'Maven home' | awk '{print $NF}'`

    echo "Creating distribution"
    ./dev/make-distribution.sh --name $NAME --mvn $MVN_HOME/bin/mvn --tgz \
      $PIP_FLAG $R_FLAG $FLAGS 2>&1 >  ../binary-release-$NAME.log
    cd ..

    if [[ -n $R_FLAG ]]; then
      echo "Copying and signing R source package"
      R_DIST_NAME=SparkR_$SPARK_VERSION.tar.gz
      cp spark-$SPARK_VERSION-bin-$NAME/R/$R_DIST_NAME .

      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
        --output $R_DIST_NAME.asc \
        --detach-sig $R_DIST_NAME
      shasum -a 512 $R_DIST_NAME > $R_DIST_NAME.sha512
    fi

    if [[ -n $PIP_FLAG ]]; then
      echo "Copying and signing python distribution"
      PYTHON_DIST_NAME=pyspark-$PYSPARK_VERSION.tar.gz
      cp spark-$SPARK_VERSION-bin-$NAME/python/dist/$PYTHON_DIST_NAME .

      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
        --output $PYTHON_DIST_NAME.asc \
        --detach-sig $PYTHON_DIST_NAME
      shasum -a 512 $PYTHON_DIST_NAME > $PYTHON_DIST_NAME.sha512
    fi

    echo "Copying and signing regular binary distribution"
    cp spark-$SPARK_VERSION-bin-$NAME/spark-$SPARK_VERSION-bin-$NAME.tgz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output spark-$SPARK_VERSION-bin-$NAME.tgz.asc \
      --detach-sig spark-$SPARK_VERSION-bin-$NAME.tgz
    shasum -a 512 spark-$SPARK_VERSION-bin-$NAME.tgz > spark-$SPARK_VERSION-bin-$NAME.tgz.sha512
  }

  # List of binary packages built. Populates two associative arrays, where the key is the "name" of
  # the package being built, and the values are respectively the needed maven arguments for building
  # the package, and any extra package needed for that particular combination.
  #
  # In dry run mode, only build the first one. The keys in BINARY_PKGS_ARGS are used as the
  # list of packages to be built, so it's ok for things to be missing in BINARY_PKGS_EXTRA.

  # NOTE: Don't forget to update the valid combinations of distributions at
  #   'python/pyspark/install.py' and 'python/docs/source/getting_started/install.rst'
  #   if you're changing them.
  declare -A BINARY_PKGS_ARGS
  BINARY_PKGS_ARGS["hadoop3"]="-Phadoop-3 $HIVE_PROFILES"
  if ! is_dry_run; then
    BINARY_PKGS_ARGS["without-hadoop"]="-Phadoop-provided"
  fi

  declare -A BINARY_PKGS_EXTRA
  BINARY_PKGS_EXTRA["hadoop3"]="withpip,withr"

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    key="hadoop3-scala2.13"
    args="-Phadoop-3 $HIVE_PROFILES"
    extra=""
    if ! make_binary_release "$key" "$SCALA_2_13_PROFILES $args" "$extra" "2.13"; then
      error "Failed to build $key package. Check logs for details."
    fi
  fi

  if [[ $PUBLISH_SCALA_2_12 = 1 ]]; then
    echo "Packages to build: ${!BINARY_PKGS_ARGS[@]}"
    for key in ${!BINARY_PKGS_ARGS[@]}; do
      args=${BINARY_PKGS_ARGS[$key]}
      extra=${BINARY_PKGS_EXTRA[$key]}
      if ! make_binary_release "$key" "$SCALA_2_12_PROFILES $args" "$extra" "2.12"; then
        error "Failed to build $key package. Check logs for details."
      fi
    done
  fi

  rm -rf spark-$SPARK_VERSION-bin-*/

  if ! is_dry_run; then
    svn co --depth=empty $RELEASE_STAGING_LOCATION svn-spark
    rm -rf "svn-spark/${DEST_DIR_NAME}-bin"
    mkdir -p "svn-spark/${DEST_DIR_NAME}-bin"

    echo "Copying release tarballs"
    cp spark-* "svn-spark/${DEST_DIR_NAME}-bin/"
    cp pyspark-* "svn-spark/${DEST_DIR_NAME}-bin/"
    cp SparkR_* "svn-spark/${DEST_DIR_NAME}-bin/"
    svn add "svn-spark/${DEST_DIR_NAME}-bin"

    cd svn-spark
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache Spark $SPARK_PACKAGE_VERSION" --no-auth-cache
    cd ..
    rm -rf svn-spark
  fi

  exit 0
fi

if [[ "$1" == "docs" ]]; then
  # Documentation
  cd spark
  echo "Building Spark docs"
  cd docs
  # TODO: Make configurable to add this: PRODUCTION=1
  if [ ! -f "Gemfile" ]; then
    cp "$SELF/Gemfile" .
    cp "$SELF/Gemfile.lock" .
    cp -r "$SELF/.bundle" .
  fi
  bundle install
  PRODUCTION=1 RELEASE_VERSION="$SPARK_VERSION" bundle exec jekyll build
  cd ..
  cd ..

  if ! is_dry_run; then
    svn co --depth=empty $RELEASE_STAGING_LOCATION svn-spark
    rm -rf "svn-spark/${DEST_DIR_NAME}-docs"
    mkdir -p "svn-spark/${DEST_DIR_NAME}-docs"

    echo "Copying release documentation"
    cp -R "spark/docs/_site" "svn-spark/${DEST_DIR_NAME}-docs/"
    svn add "svn-spark/${DEST_DIR_NAME}-docs"

    cd svn-spark
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache Spark $SPARK_PACKAGE_VERSION docs" --no-auth-cache
    cd ..
    rm -rf svn-spark
  fi

  mv "spark/docs/_site" docs/
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

  $MVN --settings $tmp_settings -DskipTests $SCALA_2_12_PROFILES $PUBLISH_PROFILES clean deploy

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    ./dev/change-scala-version.sh 2.13
    $MVN --settings $tmp_settings -DskipTests $SCALA_2_13_PROFILES $PUBLISH_PROFILES clean deploy
  fi

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
  if ! is_dry_run; then
    echo "Creating Nexus staging repository"
    repo_request="<promoteRequest><data><description>Apache Spark $SPARK_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
    staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachespark-[0-9]\{4\}\).*/\1/")
    echo "Created Nexus staging repository: $staged_repo_id"
  fi

  tmp_repo=$(mktemp -d spark-repo-XXXXX)

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    ./dev/change-scala-version.sh 2.13
    $MVN -Dmaven.repo.local=$tmp_repo -DskipTests \
      $SCALA_2_13_PROFILES $PUBLISH_PROFILES clean install
  fi

  if [[ $PUBLISH_SCALA_2_12 = 1 ]]; then
    ./dev/change-scala-version.sh 2.12
    $MVN -Dmaven.repo.local=$tmp_repo -DskipTests \
      $SCALA_2_12_PROFILES $PUBLISH_PROFILES clean install
  fi

  pushd $tmp_repo/org/apache/spark

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom |grep -v cyclonedx | xargs rm

  echo "Creating hash and signature files"
  # this must have .asc, .md5 and .sha1 - it really doesn't like anything else there
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

  if ! is_dry_run; then
    nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
    echo "Uploading files to $nexus_upload"
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
  fi

  popd
  rm -rf $tmp_repo
  cd ..
  exit 0
fi

cd ..
rm -rf spark
echo "ERROR: expects to be called with 'package', 'docs', 'publish-release', 'publish-snapshot' or 'finalize'"
