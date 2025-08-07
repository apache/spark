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

export PYSPARK_PYTHON=/usr/local/bin/python
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

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
    "pyspark-$PYSPARK_VERSION.tar.gz" \
    "pyspark-$PYSPARK_VERSION.tar.gz.asc"
  svn update "pyspark_connect-$PYSPARK_VERSION.tar.gz"
  svn update "pyspark_connect-$PYSPARK_VERSION.tar.gz.asc"
  twine upload -u __token__  -p $PYPI_API_TOKEN \
    --repository-url https://upload.pypi.org/legacy/ \
    "pyspark_connect-$PYSPARK_VERSION.tar.gz" \
    "pyspark_connect-$PYSPARK_VERSION.tar.gz.asc"
  svn update "pyspark_client-$PYSPARK_VERSION.tar.gz"
  svn update "pyspark_client-$PYSPARK_VERSION.tar.gz.asc"
  twine upload -u __token__ -p $PYPI_API_TOKEN \
    --repository-url https://upload.pypi.org/legacy/ \
    "pyspark_client-$PYSPARK_VERSION.tar.gz" \
    "pyspark_client-$PYSPARK_VERSION.tar.gz.asc"
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
  echo "docs uploaded"

  echo "Uploading release docs to spark-website"
  cd spark-website

  # TODO: Test it in the actual release
  # 1. Add download link to documentation.md
  python3 <<EOF
import re

release_version = "${RELEASE_VERSION}"
is_preview = bool(re.search(r'-preview\d*$', release_version))
base_version = re.sub(r'-preview\d*$', '', release_version)

stable_newline = f'  <li><a href="{{{{site.baseurl}}}}/docs/{release_version}/">Spark {release_version}</a></li>'
preview_newline = f'  <li><a href="{{{{site.baseurl}}}}/docs/{release_version}/">Spark {release_version} preview</a></li>'

inserted = False

def parse_version(v):
    return [int(p) for p in v.strip().split(".")]

def vercmp(v1, v2):
    a = parse_version(v1)
    b = parse_version(v2)
    return (a > b) - (a < b)

with open("documentation.md") as f:
    lines = f.readlines()

with open("documentation.md", "w") as f:
    if is_preview:
        in_preview_section = False
        for i, line in enumerate(lines):
            if '<p>Documentation for preview releases:</p>' in line:
                in_preview_section = True
                f.write(line)
                continue

            if in_preview_section and re.search(r'docs/\d+\.\d+\.\d+-preview\d*/', line):
                existing_version = re.search(r'docs/(\d+\.\d+\.\d+-preview\d*)/', line).group(1)

                if existing_version == release_version:
                    inserted = True  # Already exists, don't add
                elif not inserted:
                    base_existing = re.sub(r'-preview\d*$', '', existing_version)
                    preview_num_existing = int(re.search(r'preview(\d*)', existing_version).group(1) or "0")
                    preview_num_new = int(re.search(r'preview(\d*)', release_version).group(1) or "0")

                    if (vercmp(base_version, base_existing) > 0) or \
                       (vercmp(base_version, base_existing) == 0 and preview_num_new >= preview_num_existing):
                        f.write(preview_newline + "\n")
                        inserted = True

                f.write(line)
                continue

            if in_preview_section and "</ul>" in line and not inserted:
                f.write(preview_newline + "\n")
                inserted = True
            f.write(line)
    else:
        for line in lines:
            match = re.search(r'docs/(\d+\.\d+\.\d+)/', line)
            if not inserted and match:
                existing_version = match.group(1)
                if vercmp(release_version, existing_version) >= 0:
                    f.write(stable_newline + "\n")
                    inserted = True
            f.write(line)
        if not inserted:
            f.write(stable_newline + "\n")
EOF

  echo "Edited documentation.md"

  # 2. Add download link to js/downloads.js
  if [[ "$RELEASE_VERSION" =~ -preview[0-9]*$ ]]; then
    echo "Skipping js/downloads.js for preview release: $RELEASE_VERSION"
  else
    RELEASE_DATE=$(TZ=America/Los_Angeles date +"%m/%d/%Y")
    IFS='.' read -r rel_maj rel_min rel_patch <<< "$RELEASE_VERSION"
    NEW_PACKAGES="packagesV14"
    if [[ "$rel_maj" -ge 4 ]]; then
      NEW_PACKAGES="packagesV15"
    fi

    python3 <<EOF
import re

release_version = "${RELEASE_VERSION}"
release_date = "${RELEASE_DATE}"
new_packages = "${NEW_PACKAGES}"
newline = f'addRelease("{release_version}", new Date("{release_date}"), {new_packages}, true);'

new_major, new_minor, new_patch = [int(p) for p in release_version.split(".")]

def parse_version(v):
    return [int(p) for p in v.strip().split(".")]

def vercmp(v1, v2):
    a = parse_version(v1)
    b = parse_version(v2)
    return (a > b) - (a < b)

inserted = replaced = False

with open("js/downloads.js") as f:
    lines = f.readlines()

with open("js/downloads.js", "w") as f:
    for line in lines:
        m = re.search(r'addRelease\("(\d+\.\d+\.\d+)"', line)
        if m:
            existing_version = m.group(1)
            cmp_result = vercmp(release_version, existing_version)
            ex_major, ex_minor, ex_patch = parse_version(existing_version)

            if cmp_result == 0:
                f.write(newline + "\n")
                replaced = True
            elif not replaced and ex_major == new_major and ex_minor == new_minor:
                f.write(newline + "\n")
                replaced = True
            elif not replaced and not inserted and cmp_result > 0:
                f.write(newline + "\n")
                f.write(line)
                inserted = True
            else:
                f.write(line)
        else:
            f.write(line)
    if not replaced and not inserted:
        f.write(newline + "\n")
EOF

    echo "Edited js/downloads.js"
  fi

  # 3. Add news post
  RELEASE_DATE=$(TZ=America/Los_Angeles date +"%Y-%m-%d")
  FILENAME="news/_posts/${RELEASE_DATE}-spark-${RELEASE_VERSION//./-}-released.md"
  mkdir -p news/_posts

  if [[ "$RELEASE_VERSION" =~ -preview[0-9]*$ ]]; then
    BASE_VERSION="${RELEASE_VERSION%%-preview*}"
    cat > "$FILENAME" <<EOF
---
layout: post
title: Preview release of Spark ${BASE_VERSION}
categories:
- News
tags: []
status: publish
type: post
published: true
meta:
  _edit_last: '4'
  _wpas_done_all: '1'
---
To enable wide-scale community testing of the upcoming Spark ${BASE_VERSION} release, the Apache Spark community has posted a
<a href="https://archive.apache.org/dist/spark/spark-${RELEASE_VERSION}/">Spark ${RELEASE_VERSION} release</a>.
This preview is not a stable release in terms of either API or functionality, but it is meant to give the community early
access to try the code that will become Spark ${BASE_VERSION}. If you would like to test the release,
please <a href="https://archive.apache.org/dist/spark/spark-${RELEASE_VERSION}/">download</a> it, and send feedback using either
<a href="https://spark.apache.org/community.html">mailing lists</a> or
<a href="https://issues.apache.org/jira/browse/SPARK/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel">JIRA</a>.
The documentation is available at the <a href="https://spark.apache.org/docs/${RELEASE_VERSION}/">link</a>.

We'd like to thank our contributors and users for their contributions and early feedback to this release. This release would not have been possible without you.
EOF

  else
    cat > "$FILENAME" <<EOF
---
layout: post
title: Spark ${RELEASE_VERSION} released
categories:
- News
tags: []
status: publish
type: post
published: true
meta:
  _edit_last: '4'
  _wpas_done_all: '1'
---
We are happy to announce the availability of <a href="{{site.baseurl}}/releases/spark-release-${RELEASE_VERSION}.html" title="Spark Release ${RELEASE_VERSION}">Apache Spark ${RELEASE_VERSION}</a>! Visit the <a href="{{site.baseurl}}/releases/spark-release-${RELEASE_VERSION}.html" title="Spark Release ${RELEASE_VERSION}">release notes</a> to read about the new features, or <a href="{{site.baseurl}}/downloads.html">download</a> the release today.
EOF
  fi

  echo "Created $FILENAME"

  # 4. Add release notes with Python to extract JIRA version ID
  if [[ "$RELEASE_VERSION" =~ -preview[0-9]*$ ]]; then
    echo "Skipping JIRA release notes for preview release: $RELEASE_VERSION"
  else
    RELEASE_DATE=$(TZ=America/Los_Angeles date +"%Y-%m-%d")
    JIRA_PROJECT_ID=12315420
    JIRA_URL="https://issues.apache.org/jira/rest/api/2/project/SPARK/versions"
    JSON=$(curl -s "$JIRA_URL")

    VERSION_ID=$(python3 - <<EOF
import sys, json

release_version = "${RELEASE_VERSION}"
json_str = """$JSON"""

try:
    versions = json.loads(json_str)
except Exception as e:
    print(f"Error parsing JSON: {e}", file=sys.stderr)
    sys.exit(1)

version_id = ""
for v in versions:
    if v.get("name") == release_version:
        version_id = v.get("id", "")
        break

print(version_id)
EOF
    )

    if [[ -z "$VERSION_ID" ]]; then
      echo "Error: Couldn't find JIRA version ID for $RELEASE_VERSION" >&2
    fi

    JIRA_LINK="https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=${JIRA_PROJECT_ID}&version=${VERSION_ID}"

    IFS='.' read -r rel_maj rel_min rel_patch <<< "$RELEASE_VERSION"
    if [[ "$rel_patch" -eq 0 ]]; then
      ACKNOWLEDGE="patches and features to this release."
      BODY="Apache Spark ${RELEASE_VERSION} is a new feature release. It introduces new functionality and improvements. We encourage users to try it and provide feedback."
    else
      ACKNOWLEDGE="patches to this release."
      BODY="Apache Spark ${RELEASE_VERSION} is a maintenance release containing security and correctness fixes. This release is based on the branch-${rel_maj}.${rel_min} maintenance branch of Spark. We strongly recommend all ${rel_maj}.${rel_min} users to upgrade to this stable release."
    fi

    BODY+="

You can find the list of resolved issues and detailed changes in the [JIRA release notes](${JIRA_LINK}).

We would like to acknowledge all community members for contributing ${ACKNOWLEDGE}"

    FILENAME="releases/_posts/${RELEASE_DATE}-spark-release-${RELEASE_VERSION}.md"
    mkdir -p releases/_posts
    cat > "$FILENAME" <<EOF
---
layout: post
title: Spark Release ${RELEASE_VERSION}
categories: []
tags: []
status: publish
type: post
published: true
meta:
  _edit_last: '4'
  _wpas_done_all: '1'
---

${BODY}
EOF

    echo "Created $FILENAME"
  fi

  # 5. Build the website
  bundle install
  bundle exec jekyll build

  # 6. Update latest or preview symlink
  IFS='.' read -r rel_maj rel_min rel_patch <<< "$RELEASE_VERSION"

  if [[ "$RELEASE_VERSION" =~ -preview[0-9]*$ ]]; then
    LINK_PATH="site/docs/preview"

    ln -sfn "$RELEASE_VERSION" "$LINK_PATH"
    echo "Updated symlink $LINK_PATH -> $RELEASE_VERSION (preview release)"

  else
    LINK_PATH="site/docs/latest"

    if [[ "$rel_patch" -eq 0 ]]; then
      if [[ -L "$LINK_PATH" ]]; then
        CURRENT_TARGET=$(readlink "$LINK_PATH")
      else
        CURRENT_TARGET=""
      fi

      if [[ "$CURRENT_TARGET" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        IFS='.' read -r cur_maj cur_min cur_patch <<< "$CURRENT_TARGET"

        if [[ "$rel_maj" -gt "$cur_maj" ]]; then
          ln -sfn "$RELEASE_VERSION" "$LINK_PATH"
          echo "Updated symlink $LINK_PATH -> $RELEASE_VERSION (major version increased)"
        elif [[ "$rel_maj" -eq "$cur_maj" && "$rel_min" -gt "$cur_min" ]]; then
          ln -sfn "$RELEASE_VERSION" "$LINK_PATH"
          echo "Updated symlink $LINK_PATH -> $RELEASE_VERSION (minor version increased)"
        else
          echo "Symlink $LINK_PATH points to $CURRENT_TARGET with equal or newer major.minor, no change"
        fi
      else
        echo "No valid existing version target."
      fi
    else
      echo "Patch release detected ($RELEASE_VERSION), not updating symlink"
    fi
  fi

  git add .
  git commit -m "Add release docs for Apache Spark $RELEASE_VERSION"
  git push origin HEAD:asf-site
  cd ..
  echo "release docs uploaded"
  rm -rf spark-website

  # Moves the docs from dev directory to release directory.
  echo "Moving Spark docs to the release directory"
  svn mv --username "$ASF_USERNAME" --password "$ASF_PASSWORD" -m"Apache Spark $RELEASE_VERSION" \
    --no-auth-cache "$RELEASE_STAGING_LOCATION/$RELEASE_TAG-docs/_site" "$RELEASE_LOCATION/docs/$RELEASE_VERSION"
  echo "Spark docs moved"

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

  # TODO: Test it in the actual release
  # Release artifacts in the Nexus repository
  # Find latest orgapachespark-* repo for this release version
  REPO_ID=$(curl --retry 10 --retry-all-errors -s -u "$ASF_USERNAME:$ASF_PASSWORD" \
    https://repository.apache.org/service/local/staging/profile_repositories | \
    grep -A 5 "<repositoryId>orgapachespark-" | \
    awk '/<repositoryId>/ { id = $0 } /<description>/ && $0 ~ /Apache Spark '"$RELEASE_VERSION"'/ { print id }' | \
    grep -oP '(?<=<repositoryId>)orgapachespark-[0-9]+(?=</repositoryId>)' | \
    sort -V | tail -n 1)

  if [[ -z "$REPO_ID" ]]; then
    echo "No matching staging repository found for Apache Spark $RELEASE_VERSION"
    exit 1
  fi

  echo "Using repository ID: $REPO_ID"

  # Release the repository
  curl --retry 10 --retry-all-errors -s -u "$APACHE_USERNAME:$APACHE_PASSWORD" \
    -H "Content-Type: application/json" \
    -X POST https://repository.apache.org/service/local/staging/bulk/promote \
    -d "{\"data\": {\"stagedRepositoryIds\": [\"$REPO_ID\"], \"description\": \"Apache Spark $RELEASE_VERSION\"}}"

  # Wait for release to complete
  echo "Waiting for release to complete..."
  while true; do
    STATUS=$(curl --retry 10 --retry-all-errors -s -u "$APACHE_USERNAME:$APACHE_PASSWORD" \
      https://repository.apache.org/service/local/staging/repository/$REPO_ID | \
      grep -oPm1 "(?<=<type>)[^<]+")
    echo "Current state: $STATUS"
    if [[ "$STATUS" == "released" ]]; then
      echo "Release complete."
      break
    elif [[ "$STATUS" == "release_failed" || "$STATUS" == "error" ]]; then
      echo "Release failed."
      exit 1
    elif [[ "$STATUS" == "open" ]]; then
      echo "Repository is still open. Cannot release. Please close it first."
      exit 1
    fi
    sleep 10
  done

  # Drop the repository after release
  curl --retry 10 --retry-all-errors -s -u "$APACHE_USERNAME:$APACHE_PASSWORD" \
    -H "Content-Type: application/json" \
    -X POST https://repository.apache.org/service/local/staging/bulk/drop \
    -d "{\"data\": {\"stagedRepositoryIds\": [\"$REPO_ID\"], \"description\": \"Dropped after release\"}}"

  echo "Done."

  # TODO: Test it in the actual official release
  # Remove old releases from the mirror
  # Extract major.minor prefix
  RELEASE_SERIES=$(echo "$RELEASE_VERSION" | cut -d. -f1-2)
  
  # Fetch existing dist URLs
  OLD_VERSION=$(svn ls https://dist.apache.org/repos/dist/release/spark/ | \
    grep "^spark-$RELEASE_SERIES" | \
    grep -v "^spark-$RELEASE_VERSION/" | \
    sed 's#/##' | sed 's/^spark-//' | \
    sort -V | tail -n 1)
  
  if [[ -n "$OLD_VERSION" ]]; then
    echo "Removing old version: spark-$OLD_VERSION"
    svn rm "https://dist.apache.org/repos/dist/release/spark/spark-$OLD_VERSION" -m "Remove older $RELEASE_SERIES release after $RELEASE_VERSION"
  else
    echo "No previous $RELEASE_SERIES version found to remove. Manually remove it if there is."
  fi

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
BASE_PROFILES="-Pyarn -Pkubernetes"

PUBLISH_SCALA_2_13=1
SCALA_2_13_PROFILES="-Pscala-2.13"
if [[ $SPARK_VERSION < "3.2" ]]; then
  PUBLISH_SCALA_2_13=0
fi

PUBLISH_SCALA_2_12=1
if [[ $SPARK_VERSION > "3.5.99" ]]; then
  PUBLISH_SCALA_2_12=0
  # There is no longer scala-2.13 profile since 4.0.0
  SCALA_2_13_PROFILES=""
fi
SCALA_2_12_PROFILES="-Pscala-2.12"

# Hive-specific profiles for some builds
HIVE_PROFILES="-Phive -Phive-thriftserver"
# Profiles for publishing snapshots and release to Maven Central
# We use Apache Hive 2.3 for publishing
PUBLISH_PROFILES="$BASE_PROFILES $HIVE_PROFILES -Pspark-ganglia-lgpl -Pkinesis-asl -Phadoop-cloud -Pjvm-profiler"
# Profiles for building binary releases
BASE_RELEASE_PROFILES="$BASE_PROFILES -Psparkr"

if [[ $JAVA_VERSION < "1.8." ]] && [[ $SPARK_VERSION < "4.0" ]]; then
  echo "Java version $JAVA_VERSION is less than required 1.8 for 2.2+"
  echo "Please set JAVA_HOME correctly."
  exit 1
elif [[ $JAVA_VERSION < "17.0." ]] && [[ $SPARK_VERSION > "3.5.99" ]]; then
  echo "Java version $JAVA_VERSION is less than required 17 for 4.0+"
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

export MAVEN_OPTS="-Xss128m -Xmx${MAVEN_MXM_OPT:-12g} -XX:ReservedCodeCacheSize=1g"

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
    SPARK_CONNECT_FLAG=""
    if [[ $BUILD_PACKAGE == *"withconnect"* ]]; then
      SPARK_CONNECT_FLAG="--connect"
    fi

    echo "Building binary dist $NAME"
    cp -r spark spark-$SPARK_VERSION-bin-$NAME
    cd spark-$SPARK_VERSION-bin-$NAME

    ./dev/change-scala-version.sh $SCALA_VERSION

    echo "Creating distribution: $NAME ($FLAGS)"

    # Write out the VERSION to PySpark version info we rewrite the - into a . and SNAPSHOT
    # to dev0 to be closer to PEP440.
    PYSPARK_VERSION=`echo "$SPARK_VERSION" |  sed -e "s/-/./" -e "s/SNAPSHOT/dev0/" -e "s/preview/dev/"`
    echo "__version__: str = '$PYSPARK_VERSION'" > python/pyspark/version.py

    # Get maven home set by MVN
    MVN_HOME=`$MVN -version 2>&1 | grep 'Maven home' | awk '{print $NF}'`

    echo "Creating distribution"
    ./dev/make-distribution.sh --name $NAME --mvn $MVN_HOME/bin/mvn --tgz \
      $PIP_FLAG $R_FLAG $SPARK_CONNECT_FLAG $FLAGS 2>&1 >  ../binary-release-$NAME.log
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

      PYTHON_CONNECT_DIST_NAME=pyspark_connect-$PYSPARK_VERSION.tar.gz
      cp spark-$SPARK_VERSION-bin-$NAME/python/dist/$PYTHON_CONNECT_DIST_NAME .

      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
        --output $PYTHON_CONNECT_DIST_NAME.asc \
        --detach-sig $PYTHON_CONNECT_DIST_NAME
      shasum -a 512 $PYTHON_CONNECT_DIST_NAME > $PYTHON_CONNECT_DIST_NAME.sha512

      PYTHON_CLIENT_DIST_NAME=pyspark_client-$PYSPARK_VERSION.tar.gz
      cp spark-$SPARK_VERSION-bin-$NAME/python/dist/$PYTHON_CLIENT_DIST_NAME .

      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
        --output $PYTHON_CLIENT_DIST_NAME.asc \
        --detach-sig $PYTHON_CLIENT_DIST_NAME
      shasum -a 512 $PYTHON_CLIENT_DIST_NAME > $PYTHON_CLIENT_DIST_NAME.sha512
    fi

    echo "Copying and signing regular binary distribution"
    cp spark-$SPARK_VERSION-bin-$NAME/spark-$SPARK_VERSION-bin-$NAME.tgz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output spark-$SPARK_VERSION-bin-$NAME.tgz.asc \
      --detach-sig spark-$SPARK_VERSION-bin-$NAME.tgz
    shasum -a 512 spark-$SPARK_VERSION-bin-$NAME.tgz > spark-$SPARK_VERSION-bin-$NAME.tgz.sha512

    if [[ -n $SPARK_CONNECT_FLAG ]]; then
      echo "Copying and signing Spark Connect binary distribution"
      SPARK_CONNECT_DIST_NAME=spark-$SPARK_VERSION-bin-$NAME-connect.tgz
      cp spark-$SPARK_VERSION-bin-$NAME/$SPARK_CONNECT_DIST_NAME .
      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
        --output $SPARK_CONNECT_DIST_NAME.asc \
        --detach-sig $SPARK_CONNECT_DIST_NAME
      shasum -a 512 $SPARK_CONNECT_DIST_NAME > $SPARK_CONNECT_DIST_NAME.sha512
    fi
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
  if [[ $SPARK_VERSION > "3.5.99" ]]; then
    # Since 4.0, we publish a new distribution with Spark Connect enable.
    BINARY_PKGS_EXTRA["hadoop3"]="withpip,withr,withconnect"
  else
    BINARY_PKGS_EXTRA["hadoop3"]="withpip,withr"
  fi

  # This is dead code as Scala 2.12 is no longer supported, but we keep it as a template for
  # adding new Scala version support in the future. This secondary Scala version only has one
  # binary package to avoid doubling the number of final packages. It doesn't build PySpark and
  # SparkR as the primary Scala version will build them.
  if [[ $PUBLISH_SCALA_2_12 = 1 ]]; then
    key="hadoop3-scala2.12"
    args="-Phadoop-3 $HIVE_PROFILES"
    extra=""
    if ! make_binary_release "$key" "$SCALA_2_12_PROFILES $args" "$extra" "2.12"; then
      error "Failed to build $key package. Check logs for details."
    fi
  fi

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    echo "Packages to build: ${!BINARY_PKGS_ARGS[@]}"
    for key in ${!BINARY_PKGS_ARGS[@]}; do
      args=${BINARY_PKGS_ARGS[$key]}
      extra=${BINARY_PKGS_EXTRA[$key]}
      if ! make_binary_release "$key" "$SCALA_2_13_PROFILES $args" "$extra" "2.13"; then
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
    cp pyspark* "svn-spark/${DEST_DIR_NAME}-bin/"
    cp SparkR* "svn-spark/${DEST_DIR_NAME}-bin/"
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

  if [[ $PUBLISH_SCALA_2_12 = 1 ]]; then
    $MVN --settings $tmp_settings -DskipTests $SCALA_2_12_PROFILES $PUBLISH_PROFILES clean deploy
  fi

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    if [[ $SPARK_VERSION < "4.0" ]]; then
      ./dev/change-scala-version.sh 2.13
    fi
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
    out=$(curl --retry 10 --retry-all-errors -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
    staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachespark-[0-9]\{4\}\).*/\1/")
    echo "Created Nexus staging repository: $staged_repo_id"
  fi

  tmp_repo=$(mktemp -d spark-repo-XXXXX)

  if [[ $PUBLISH_SCALA_2_13 = 1 ]]; then
    if [[ $SPARK_VERSION < "4.0" ]]; then
      ./dev/change-scala-version.sh 2.13
    fi
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

    # Temp file to track errors
    error_flag_file=$(mktemp)

    find . -type f | sed -e 's|^\./||' | \
    xargs -P 4 -n 1 -I {} bash -c '
      file_short="{}"
      dest_url="'$NEXUS_ROOT'/deployByRepositoryId/'$staged_repo_id'/org/apache/spark/$file_short"
      echo "[START] $file_short"

      if curl --retry 10 --retry-all-errors -sS -u "$ASF_USERNAME:$ASF_PASSWORD" \
          --upload-file "$file_short" "$dest_url"; then
        echo "[ OK  ] $file_short"
      else
        echo "[FAIL ] $file_short"
        echo "fail" >> '"$error_flag_file"'
      fi
    '

    # Check if any failures were recorded
    if [ -s "$error_flag_file" ]; then
      echo "One or more uploads failed."
      rm "$error_flag_file"
      exit 1
    else
      echo "All uploads succeeded."
      rm "$error_flag_file"
    fi

    echo "Closing nexus staging repository"
    repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Spark $SPARK_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl --retry 10 --retry-all-errors -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
    echo "Closed Nexus staging repository: $staged_repo_id"

    echo "Sending the RC vote email"
    EMAIL_TO="dev@spark.apache.org"
    EMAIL_SUBJECT="[VOTE] Release Spark ${SPARK_VERSION} (RC${SPARK_RC_COUNT})"

    # Calculate deadline in Pacific Time (PST/PDT)
    DEADLINE=$(TZ=America/Los_Angeles date -d "+4 days" "+%a, %d %b %Y %H:%M:%S %Z")
    PYSPARK_VERSION=`echo "$RELEASE_VERSION" |  sed -e "s/-/./" -e "s/preview/dev/"`

    JIRA_API_URL="https://issues.apache.org/jira/rest/api/2/project/SPARK/versions"
    SPARK_VERSION_BASE=$(echo "$SPARK_VERSION" | sed 's/-preview[0-9]*//')
    JIRA_VERSION_ID=$(curl -s "$JIRA_API_URL" | \
      # Split JSON objects by replacing '},{' with a newline-separated pattern
      tr '}' '\n' | \
      # Find the block containing the exact version name
      grep -F "\"name\":\"$SPARK_VERSION_BASE\"" -A 5 | \
      # Extract the line with "id"
      grep '"id"' | \
      # Extract the numeric id value (assuming "id":"123456")
      sed -E 's/.*"id":"?([0-9]+)"?.*/\1/' | \
      head -1)

    # Configure msmtp
    cat > ~/.msmtprc <<EOF
defaults
auth           on
tls            on
tls_trust_file /etc/ssl/certs/ca-certificates.crt
logfile        ~/.msmtp.log

account        apache
host           mail-relay.apache.org
port           587
from           $ASF_USERNAME@apache.org
user           $ASF_USERNAME
password       $ASF_PASSWORD

account default : apache
EOF

    chmod 600 ~/.msmtprc

    # Compose and send the email
    {
      echo "From: $ASF_USERNAME@apache.org"
      echo "To: $EMAIL_TO"
      echo "Subject: $EMAIL_SUBJECT"
      echo
      echo "Please vote on releasing the following candidate as Apache Spark version ${SPARK_VERSION}."
      echo
      echo "The vote is open until ${DEADLINE} and passes if a majority +1 PMC votes are cast, with"
      echo "a minimum of 3 +1 votes."
      echo
      echo "[ ] +1 Release this package as Apache Spark ${SPARK_VERSION}"
      echo "[ ] -1 Do not release this package because ..."
      echo
      echo "To learn more about Apache Spark, please see https://spark.apache.org/"
      echo
      echo "The tag to be voted on is ${GIT_REF} (commit ${git_hash}):"
      echo "https://github.com/apache/spark/tree/${GIT_REF}"
      echo
      echo "The release files, including signatures, digests, etc. can be found at:"
      echo "https://dist.apache.org/repos/dist/dev/spark/${GIT_REF}-bin/"
      echo
      echo "Signatures used for Spark RCs can be found in this file:"
      echo "https://downloads.apache.org/spark/KEYS"
      echo
      echo "The staging repository for this release can be found at:"
      echo "https://repository.apache.org/content/repositories/${staged_repo_id}/"
      echo
      echo "The documentation corresponding to this release can be found at:"
      echo "https://dist.apache.org/repos/dist/dev/spark/${GIT_REF}-docs/"
      echo
      echo "The list of bug fixes going into ${SPARK_VERSION} can be found at the following URL:"
      echo "https://issues.apache.org/jira/projects/SPARK/versions/${JIRA_VERSION_ID}"
      echo
      echo "FAQ"
      echo
      echo "========================="
      echo "How can I help test this release?"
      echo "========================="
      echo
      echo "If you are a Spark user, you can help us test this release by taking"
      echo "an existing Spark workload and running on this release candidate, then"
      echo "reporting any regressions."
      echo
      echo "If you're working in PySpark you can set up a virtual env and install"
      echo "the current RC via \"pip install https://dist.apache.org/repos/dist/dev/spark/${GIT_REF}-bin/pyspark-${PYSPARK_VERSION}.tar.gz\""
      echo "and see if anything important breaks."
      echo "In the Java/Scala, you can add the staging repository to your project's resolvers and test"
      echo "with the RC (make sure to clean up the artifact cache before/after so"
      echo "you don't end up building with an out of date RC going forward)."
    } | msmtp -t
  fi

  popd
  rm -rf $tmp_repo
  cd ..
  exit 0
fi

cd ..
rm -rf spark
echo "ERROR: expects to be called with 'package', 'docs', 'publish-release', 'publish-snapshot' or 'finalize'"
