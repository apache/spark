#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags --first-parent)

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Pyarn -Psparkr)

MVN_LOCAL="~/.m2/repository"

publish_artifacts() {
  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" install
}

make_dist() {
  build_flags="$1"
  shift 1
  hadoop_name="hadoop-palantir"
  artifact_name="spark-dist_2.11-${hadoop_name}"
  file_name="spark-dist-${version}-${hadoop_name}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" $build_flags
  mkdir -p $MVN_LOCAL/org/apache/spark/${artifact_name}/${version} && \
  cp $file_name $MVN_LOCAL/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz
}

publish_artifacts
make_dist "${PALANTIR_FLAGS[*]}"
