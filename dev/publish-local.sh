#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags)

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

MVN_LOCAL="~/.m2/repository"

publish_artifacts() {
  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" install clean
}

make_dist() {
  build_flags="$1"
  shift 1
  artifact_name="spark-dist_2.11-hadoop-palantir"
  file_name="${artifact_name}-${version}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" $build_flags
  mkdir -p $MVN_LOCAL/org/apache/spark/${artifact_name}/${version} && \
  cp $file_name $MVN_LOCAL/org/apache/spark/${artifact_name}/${version}/
}

publish_artifacts
make_dist "${PALANTIR_FLAGS[*]}"
