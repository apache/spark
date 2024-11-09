#!/bin/bash

if [[ $# != 4 ]]; then
  echo "Usage: upload-jar.sh <group id> <artifact id> <version> <jar name>"
  exit 1
fi

GROUP_ID=$1
ARTIFACT_ID=$2
VERSION=$3
JAR_NAME=$4

mvn deploy:deploy-file \
  --settings $BUILDKITE_BUILD_CHECKOUT_PATH/ci/settings.xml \
  -Durl=https://artifactory.data-0.internal.api.openai.org/artifactory/libs-release-local \
  -DgroupId=$GROUP_ID \
  -DartifactId=$ARTIFACT_ID \
  -Dversion=$VERSION \
  -DrepositoryId=artifactory \
  -Dfile=$JAR_NAME \
  -Dartifactory.username=admin \
  -Dartifactory.password="$ARTIFACTORY_TOKEN"
