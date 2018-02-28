#!/bin/bash

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

### This script is used by Kubernetes Test Infrastructure to run integration tests.
### See documenation at https://github.com/kubernetes/test-infra/tree/master/prow

set -ex

# set cwd correctly
cd "$(dirname "$0")/../"

# Include requisite scripts
source ./include/util.sh

TEST_ROOT_DIR=$(git rev-parse --show-toplevel)
BRANCH="master"
SPARK_REPO="https://github.com/apache/spark"
SPARK_REPO_LOCAL_DIR="$TEST_ROOT_DIR/target/spark"

## Install basic dependencies
## These are for the kubekins-e2e environment in https://github.com/kubernetes/test-infra/tree/master/images/kubekins-e2e
echo "deb http://http.debian.net/debian jessie-backports main" >> /etc/apt/sources.list
apt-get update && apt-get install -y curl wget git tar uuid-runtime
apt-get install -t jessie-backports -y openjdk-8-jdk

# Set up config.
MASTER=$(kubectl cluster-info | head -n 1 | grep -oE "https?://[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(:[0-9]+)?")

# Special GCP project for publishing docker images built by test.
IMAGE_REPO="gcr.io/spark-testing-191023"

# Cloning the spark distribution
echo "Cloning $SPARK_REPO into $SPARK_REPO_LOCAL_DIR and checking out $BRANCH."
clone_build_spark $SPARK_REPO $SPARK_REPO_LOCAL_DIR $BRANCH

# Spark distribution
properties=(
  -Dspark.kubernetes.test.master=k8s://$MASTER \
  -Dspark.kubernetes.test.imageRepo=$IMAGE_REPO \
  -Dspark.kubernetes.test.sparkTgz="$SPARK_TGZ" \
  -Dspark.kubernetes.test.deployMode=cloud \
  -Dspark.kubernetes.test.namespace=spark \
  -Dspark.kubernetes.test.serviceAccountName=spark-sa
)

# Run kubectl commands and create appropriate roles
kubectl create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user pr-kubekins@kubernetes-jenkins-pull.iam.gserviceaccount.com
kubectl create -f ./dev/spark-rbac.yaml

# Run tests.
echo "Starting test with ${properties[@]}"
build/mvn integration-test "${properties[@]}" || :

# Copy out the junit xml files for consumption by k8s test-infra.
ls -1 ./target/surefire-reports/*.xml | cat -n | while read n f; do cp "$f" "/workspace/_artifacts/junit_0$n.xml"; done
