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
set -ex
TEST_ROOT_DIR=$(git rev-parse --show-toplevel)
UNPACKED_SPARK_TGZ="$TEST_ROOT_DIR/target/spark-dist-unpacked"
IMAGE_TAG_OUTPUT_FILE="$TEST_ROOT_DIR/target/image-tag.txt"
DEPLOY_MODE="minikube"
IMAGE_REPO="docker.io/kubespark"
IMAGE_TAG="N/A"
JAVA_IMAGE_TAG="N/A"
SPARK_TGZ="N/A"
MVN="$TEST_ROOT_DIR/build/mvn"
DOCKER_FILE="N/A"
EXCLUDE_TAGS=""

# Parse arguments
while (( "$#" )); do
  case $1 in
    --unpacked-spark-tgz)
      UNPACKED_SPARK_TGZ="$2"
      shift
      ;;
    --image-repo)
      IMAGE_REPO="$2"
      shift
      ;;
    --image-tag)
      IMAGE_TAG="$2"
      shift
      ;;
    --java-image-tag)
      JAVA_IMAGE_TAG="$2"
      shift
      ;;
    --image-tag-output-file)
      IMAGE_TAG_OUTPUT_FILE="$2"
      shift
      ;;
    --deploy-mode)
      DEPLOY_MODE="$2"
      shift
      ;;
    --spark-tgz)
      SPARK_TGZ="$2"
      shift
      ;;
    --docker-file)
      DOCKER_FILE="$2"
      shift
      ;;
    --test-exclude-tags)
      EXCLUDE_TAGS="$2"
      shift
      ;;
    *)
      break
      ;;
  esac
  shift
done

rm -rf "$UNPACKED_SPARK_TGZ"
if [[ $SPARK_TGZ == "N/A" && $IMAGE_TAG == "N/A" ]];
then
  # If there is no spark image tag to test with and no src dir, build from current
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
  SPARK_INPUT_DIR="$(cd "$SCRIPT_DIR/"../../../../  >/dev/null 2>&1 && pwd )"
  DOCKER_FILE_BASE_PATH="$SPARK_INPUT_DIR/resource-managers/kubernetes/docker/src/main/dockerfiles/spark"
elif [[ $IMAGE_TAG == "N/A" ]];
then
  # If there is a test src tarball and no image tag we will want to build from that
  mkdir -p $UNPACKED_SPARK_TGZ
  tar -xzvf $SPARK_TGZ --strip-components=1 -C $UNPACKED_SPARK_TGZ;
  SPARK_INPUT_DIR="$UNPACKED_SPARK_TGZ"
  DOCKER_FILE_BASE_PATH="$SPARK_INPUT_DIR/kubernetes/dockerfiles/spark"
fi


# If there is a specific Spark image skip building and extraction/copy
if [[ $IMAGE_TAG == "N/A" ]];
then
  VERSION=$("$MVN" help:evaluate -Dexpression=project.version \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
  IMAGE_TAG=${VERSION}_$(uuidgen)
  cd $SPARK_INPUT_DIR

  if [[ $DOCKER_FILE == "N/A" ]]; then
    # OpenJDK base-image tag (e.g. 8-jre-slim, 11-jre-slim)
    JAVA_IMAGE_TAG_BUILD_ARG="-b java_image_tag=$JAVA_IMAGE_TAG"
  else
    if [[ $DOCKER_FILE = /* ]]; then
      JAVA_IMAGE_TAG_BUILD_ARG="-f $DOCKER_FILE"
    else
      JAVA_IMAGE_TAG_BUILD_ARG="-f $DOCKER_FILE_BASE_PATH/$DOCKER_FILE"
    fi
  fi

  # Build PySpark image
  LANGUAGE_BINDING_BUILD_ARGS="-p $DOCKER_FILE_BASE_PATH/bindings/python/Dockerfile"

  # Build SparkR image
  tags=(${EXCLUDE_TAGS//,/ })
  if [[ ! ${tags[@]} =~ "r" ]]; then
    LANGUAGE_BINDING_BUILD_ARGS="$LANGUAGE_BINDING_BUILD_ARGS -R $DOCKER_FILE_BASE_PATH/bindings/R/Dockerfile"
  fi

  # Unset SPARK_HOME to let the docker-image-tool script detect SPARK_HOME. Otherwise, it cannot
  # indicate the unpacked directory as its home. See SPARK-28550.
  unset SPARK_HOME

  case $DEPLOY_MODE in
    cloud)
      # Build images
      $SPARK_INPUT_DIR/bin/docker-image-tool.sh -r $IMAGE_REPO -t $IMAGE_TAG $JAVA_IMAGE_TAG_BUILD_ARG $LANGUAGE_BINDING_BUILD_ARGS build

      # Push images appropriately
      if [[ $IMAGE_REPO == gcr.io* ]] ;
      then
        gcloud docker -- push $IMAGE_REPO/spark:$IMAGE_TAG
      else
        $SPARK_INPUT_DIR/bin/docker-image-tool.sh -r $IMAGE_REPO -t $IMAGE_TAG push
      fi
      ;;

    docker-desktop | docker-for-desktop)
       # Only need to build as this will place it in our local Docker repo which is all
       # we need for Docker for Desktop to work so no need to also push
       $SPARK_INPUT_DIR/bin/docker-image-tool.sh -r $IMAGE_REPO -t $IMAGE_TAG $JAVA_IMAGE_TAG_BUILD_ARG $LANGUAGE_BINDING_BUILD_ARGS build
       ;;

    minikube)
       # Only need to build and if we do this with the -m option for minikube we will
       # build the images directly using the minikube Docker daemon so no need to push
       $SPARK_INPUT_DIR/bin/docker-image-tool.sh -m -r $IMAGE_REPO -t $IMAGE_TAG $JAVA_IMAGE_TAG_BUILD_ARG $LANGUAGE_BINDING_BUILD_ARGS build
       ;;
    *)
       echo "Unrecognized deploy mode $DEPLOY_MODE" && exit 1
       ;;
  esac
  cd -
fi

rm -f $IMAGE_TAG_OUTPUT_FILE
echo -n $IMAGE_TAG > $IMAGE_TAG_OUTPUT_FILE
