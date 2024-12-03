#!/bin/bash

set -ex

# To use this script, you need to download a Spark distribution from https://archive.apache.org/dist/spark,
# uncompress the tar.gz, and set the SPARK_HOME to point to the distribution directory.
# Also make sure the Spark version matches the docker image tag.
if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME is not set. Please set SPARK_HOME to the root directory of your Spark distribution."
  exit 1
fi

cd $SPARK_HOME

REPONAME=databricksregistry
REPOPATH=${REPONAME}.azurecr.io/base
IMAGE_PATH=base/spark-py
IMAGE=${REPONAME}.azurecr.io/${IMAGE_PATH}
VERSION=$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1 | sed 's/-SNAPSHOT//')

echo "Building Spark distribution for ${SPARK_VERSION}"

build/mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
dev/make-distribution.sh --name openai-spark --pip --tgz -Phive -Phive-thriftserver -Pyarn -Pkubernetes -Phadoop-cloud

bin/docker-image-tool.sh \
    -r $REPOPATH \
    -t $VERSION \
    -p dist/kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
    build

az acr login -n ${REPONAME} --subscription api
docker push ${IMAGE}:${VERSION}
az acr repository update --name ${REPONAME} --image ${IMAGE_PATH}:${VERSION}
