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

function error {
  echo "$@" 1>&2
  exit 1
}

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ -n "$REPO" ]; then
    image="$REPO/$image"
  fi
  if [ -n "$TAG" ]; then
    image="$image:$TAG"
  fi
  echo "$image"
}

function build {
  local BUILD_ARGS
  local IMG_PATH="kubernetes/src"

  if [ ! -d "$IMG_PATH" ]; then
    error "Cannot find docker image. This script must be run from a runnable distribution of Apache Spark."
  fi

  local KRB_BUILD_ARGS=(
    --build-arg
    base_img=$(image_ref spark)
  )
  local KDOCKERFILE=${KDOCKERFILE:-"$IMG_PATH/test/dockerfiles/spark/kerberos/Dockerfile"}

  docker pull ifilonenko/hadoop-base:latest

  docker build $NOCACHEARG "${KRB_BUILD_ARGS[@]}" \
    -t $(image_ref spark-kerberos) \
    -f "$KDOCKERFILE" .
}

REPO=
TAG=
NOCACHEARG=
while getopts r:t:n: option
do
 case "${option}"
 in
 r) REPO=${OPTARG};;
 t) TAG=${OPTARG};;
 n) NOCACHEARG="--no-cache";;
 esac
done

eval $(minikube docker-env)
build
