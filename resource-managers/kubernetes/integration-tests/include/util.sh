#!/usr/bin/env bash

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

clone_build_spark() {
  spark_repo=$1
  spark_repo_local_dir=$2
  branch=$3
  pushd .

  # clone spark distribution if needed.
  # TODO(ssuchter): This code, that does a checkout of a spark repo,
  # made more sense when this script was in the repo
  # https://github.com/apache-spark-on-k8s/spark-integration
  # but now we shouldn't check out another copy of spark, we should just
  # build in the copy that is checked out already.
  if [ -d "$spark_repo_local_dir" ];
  then
    (cd $spark_repo_local_dir && git fetch origin $branch);
  else
    mkdir -p $spark_repo_local_dir;
    git clone -b $branch --single-branch $spark_repo $spark_repo_local_dir;
  fi
  cd $spark_repo_local_dir
  git checkout -B $branch origin/$branch
  ./dev/make-distribution.sh --tgz -Phadoop-2.7 -Pkubernetes -DskipTests;
  SPARK_TGZ=$(find $spark_repo_local_dir -name spark-*.tgz)

  popd
}
