#!/bin/bash

set -ex

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

cd $SPARK_HOME

REPONAME=databricksregistry
REPOPATH=${REPONAME}.azurecr.io/base
IMAGE_PATH=base/spark-py
IMAGE=${REPONAME}.azurecr.io/${IMAGE_PATH}

# Fetch project version
VERSION=$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1)

# Apply sed only if release is true
if [ "$release" = true ]; then
  VERSION=$(echo "$VERSION" | sed 's/-SNAPSHOT//')
fi

echo "Building Spark distribution for ${VERSION}"

build/mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
dev/make-distribution.sh --name openai-spark --pip --tgz -Phive -Phive-thriftserver -Pyarn -Pkubernetes -Phadoop-cloud


az acr login -n ${REPONAME} --subscription data-platform

bin/docker-image-tool.sh \
    -r $REPOPATH \
    -t $VERSION \
    -p dist/kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
    -b java_image_tag=17-noble \
    -X \
    build