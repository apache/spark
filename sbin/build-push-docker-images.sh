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
#

# This script builds and pushes docker images when run from a release of Spark
# with Kubernetes support.

function error {
  echo "$@" 1>&2
  exit 1
}

# Detect whether this is a git clone or a Spark distribution and adjust paths
# accordingly.
if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ -f "$SPARK_HOME/RELEASE" ]; then
  IMG_PATH="kubernetes/dockerfiles"
  SPARK_JARS="jars"
else
  IMG_PATH="resource-managers/kubernetes/docker/src/main/dockerfiles"
  SPARK_JARS="assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi

if [ ! -d "$IMG_PATH" ]; then
  error "Cannot find docker images. This script must be run from a runnable distribution of Apache Spark."
fi

declare -A path=( [spark-driver]="$IMG_PATH/driver/Dockerfile" \
                  [spark-executor]="$IMG_PATH/executor/Dockerfile" \
                  [spark-init]="$IMG_PATH/init-container/Dockerfile" )

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ $add_repo = 1 ] && [ -n "$REPO" ]; then
    image="$REPO/$image"
  fi
  if [ -n "$TAG" ]; then
    image="$image:$TAG"
  fi
  echo "$image"
}

function build {
  local base_image="$(image_ref spark-base 0)"
  docker build --build-arg "spark_jars=$SPARK_JARS" \
    --build-arg "img_path=$IMG_PATH" \
    -t "$base_image" \
    -f "$IMG_PATH/spark-base/Dockerfile" .
  for image in "${!path[@]}"; do
    docker build --build-arg "base_image=$base_image" -t "$(image_ref $image)" -f ${path[$image]} .
  done
}

function push {
  for image in "${!path[@]}"; do
    docker push "$(image_ref $image)"
  done
}

function usage {
  cat <<EOF
Usage: $0 [options] [command]
Builds or pushes the built-in Spark Docker images.

Commands:
  build       Build images.
  push        Push images to a registry. Requires a repository address to be provided, both
              when building and when pushing the images.

Options:
  -r repo     Repository address.
  -t tag      Tag to apply to built images, or to identify images to be pushed.
  -m          Use minikube's Docker daemon.

Using minikube when building images will do so directly into minikube's Docker daemon.
There is no need to push the images into minikube int that case, they'll be automatically
available when running applications inside the minikube cluster.

Check the following documentation for more information on using the minikube Docker daemon:

  https://kubernetes.io/docs/getting-started-guides/minikube/#reusing-the-docker-daemon

Examples:
  - Build images in minikube with tag "testing"
    $0 -m -t testing build

  - Build and push images with tag "v2.3.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v2.3.0 build
    $0 -r docker.io/myrepo -t v2.3.0 push
EOF
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

REPO=
TAG=
while getopts mr:t: option
do
 case "${option}"
 in
 r) REPO=${OPTARG};;
 t) TAG=${OPTARG};;
 m)
   if ! which minikube 1>/dev/null; then
     error "Cannot find minikube."
   fi
   eval $(minikube docker-env)
   ;;
 esac
done

case "${@: -1}" in
  build)
    build
    ;;
  push)
    if [ -z "$REPO" ]; then
      usage
      exit 1
    fi
    push
    ;;
  *)
    usage
    exit 1
    ;;
esac
