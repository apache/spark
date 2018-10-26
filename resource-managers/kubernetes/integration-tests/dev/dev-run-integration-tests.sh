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
set -xo errexit
TEST_ROOT_DIR=$(git rev-parse --show-toplevel)

DEPLOY_MODE="minikube"
IMAGE_REPO="docker.io/kubespark"
SPARK_TGZ="N/A"
IMAGE_TAG="N/A"
SPARK_MASTER=
NAMESPACE=
SERVICE_ACCOUNT=
INCLUDE_TAGS="k8s"
EXCLUDE_TAGS=
SCALA_VERSION="$($TEST_ROOT_DIR/build/mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=scala.binary.version | grep -v '\[' )"

# Parse arguments
while (( "$#" )); do
  case $1 in
    --image-repo)
      IMAGE_REPO="$2"
      shift
      ;;
    --image-tag)
      IMAGE_TAG="$2"
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
    --spark-master)
      SPARK_MASTER="$2"
      shift
      ;;
    --namespace)
      NAMESPACE="$2"
      shift
      ;;
    --service-account)
      SERVICE_ACCOUNT="$2"
      shift
      ;;
    --include-tags)
      INCLUDE_TAGS="k8s,$2"
      shift
      ;;
    --exclude-tags)
      EXCLUDE_TAGS="$2"
      shift
      ;;
    *)
      break
      ;;
  esac
  shift
done

properties=(
  -Dspark.kubernetes.test.sparkTgz=$SPARK_TGZ \
  -Dspark.kubernetes.test.imageTag=$IMAGE_TAG \
  -Dspark.kubernetes.test.imageRepo=$IMAGE_REPO \
  -Dspark.kubernetes.test.deployMode=$DEPLOY_MODE \
  -Dtest.include.tags=$INCLUDE_TAGS
)

if [ -n $NAMESPACE ];
then
  properties=( ${properties[@]} -Dspark.kubernetes.test.namespace=$NAMESPACE )
fi

if [ -n $SERVICE_ACCOUNT ];
then
  properties=( ${properties[@]} -Dspark.kubernetes.test.serviceAccountName=$SERVICE_ACCOUNT )
fi

if [ -n $SPARK_MASTER ];
then
  properties=( ${properties[@]} -Dspark.kubernetes.test.master=$SPARK_MASTER )
fi

if [ -n $EXCLUDE_TAGS ];
then
  properties=( ${properties[@]} -Dtest.exclude.tags=$EXCLUDE_TAGS )
fi

$TEST_ROOT_DIR/build/mvn integration-test -f $TEST_ROOT_DIR/pom.xml -pl resource-managers/kubernetes/integration-tests -am -Pscala-$SCALA_VERSION -Pkubernetes -Pkubernetes-integration-tests -Phadoop-2.7 ${properties[@]}
