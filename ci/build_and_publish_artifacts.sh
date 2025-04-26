#!/bin/bash

if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME is not set. Please set SPARK_HOME to the root directory of your Spark distribution."
  exit 1
fi

export ARTIFACTORY_TOKEN=${DATA_PLATFORM_ARTIFACTORY_TOKEN#*=}

cd $SPARK_HOME
VERSION=$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1 | sed 's/-SNAPSHOT//')

echo "Building Spark jars for ${SPARK_VERSION}"

build/mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
build/mvn -DskipTests clean package -Phive -Pkubernetes -Phadoop-cloud -Phive-thriftserver

GROUP_ID=org.apache.spark
SCALA_VERSION=2.12

echo "Deploying Spark artifacts for ${SPARK_VERSION}"
build/mvn deploy -s $BUILDKITE_BUILD_CHECKOUT_PATH/ci/settings.xml -DskipTests