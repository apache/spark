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
TEST_ROOT_DIR=$(git rev-parse --show-toplevel)
UNPACKED_SPARK_TGZ="$TEST_ROOT_DIR/target/spark-dist-unpacked"
IMAGE_TAG_OUTPUT_FILE="$TEST_ROOT_DIR/target/image-tag.txt"
DEPLOY_MODE="minikube"
IMAGE_REPO="docker.io/kubespark"
IMAGE_TAG="N/A"
SPARK_TGZ="N/A"

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
    *)
      break
      ;;
  esac
  shift
done

if [[ $SPARK_TGZ == "N/A" ]];
then
  echo "Must specify a Spark tarball to build Docker images against with --spark-tgz." && exit 1;
fi

rm -rf $UNPACKED_SPARK_TGZ
mkdir -p $UNPACKED_SPARK_TGZ
tar -xzvf $SPARK_TGZ --strip-components=1 -C $UNPACKED_SPARK_TGZ;

if [[ $IMAGE_TAG == "N/A" ]];
then
  IMAGE_TAG=$(uuidgen);
  cd $UNPACKED_SPARK_TGZ
  if [[ $DEPLOY_MODE == cloud ]] ;
  then
    $UNPACKED_SPARK_TGZ/bin/docker-image-tool.sh -r $IMAGE_REPO -t $IMAGE_TAG build
    if  [[ $IMAGE_REPO == gcr.io* ]] ;
    then
      gcloud docker -- push $IMAGE_REPO/spark:$IMAGE_TAG
    else
      $UNPACKED_SPARK_TGZ/bin/docker-image-tool.sh -r $IMAGE_REPO -t $IMAGE_TAG push
    fi
  else
    # -m option for minikube.
    $UNPACKED_SPARK_TGZ/bin/docker-image-tool.sh -m -r $IMAGE_REPO -t $IMAGE_TAG build
  fi
  cd -
fi

rm -f $IMAGE_TAG_OUTPUT_FILE
echo -n $IMAGE_TAG > $IMAGE_TAG_OUTPUT_FILE
