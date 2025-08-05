#!/bin/bash

# Check input arguments
release=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      release=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME is not set. Please set SPARK_HOME to the root directory of your Spark distribution."
  exit 1
fi

export ARTIFACTORY_TOKEN=${DATA_PLATFORM_ARTIFACTORY_TOKEN#*=}

# Fetch project version
cd $SPARK_HOME
VERSION=$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1)

# Apply sed only if release is true
if [ "$release" = true ]; then
  VERSION=$(echo "$VERSION" | sed 's/-SNAPSHOT//')
fi

echo "Building Spark jars for ${VERSION}"

build/mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
build/mvn -s $BUILDKITE_BUILD_CHECKOUT_PATH/ci/settings.xml \
  -DskipTests clean package -Phive -Pkubernetes -Phadoop-cloud -Phive-thriftserver

GROUP_ID=org.apache.spark
SCALA_VERSION=2.12

echo "Deploying Spark artifacts for ${VERSION}"
build/mvn deploy -s $BUILDKITE_BUILD_CHECKOUT_PATH/ci/settings.xml -DskipTests