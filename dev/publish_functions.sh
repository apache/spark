#!/usr/bin/env bash

set -euo pipefail

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Pyarn -Psparkr)

get_version() {
  git describe --tags --first-parent
}

set_version_and_package() {
  version=$(get_version)
  ./build/mvn versions:set -DnewVersion="$version"
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" package
}

publish_artifacts() {
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>bintray-palantir-release</id><username>$BINTRAY_USERNAME</username>" >> $tmp_settings
  echo "<password>$BINTRAY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" deploy
}

make_dist() {
  build_flags="$1"
  shift 1
  version=$(get_version)
  hadoop_name="hadoop-palantir"
  artifact_name="spark-dist_2.11-${hadoop_name}"
  file_name="spark-dist-${version}-${hadoop_name}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" $build_flags
  curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -T "$file_name" "https://api.bintray.com/content/palantir/releases/spark/${version}/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz"
  curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -X POST "https://api.bintray.com/content/palantir/releases/spark/${version}/publish"
}
