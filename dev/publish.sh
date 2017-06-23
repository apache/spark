#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags)

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

publish_artifacts() {
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>bintray-palantir-release</id><username>$BINTRAY_USERNAME</username>" >> $tmp_settings
  echo "<password>$BINTRAY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" deploy
}

make_dist() {
  dist_name="$1"
  build_flags="$2"
  shift 2
  dist_version="${version}-${dist_name}"
  file_name="spark-dist-${dist_version}.tgz"
  ./dev/make-distribution.sh --name $dist_name --tgz "$@" $build_flags
  curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -T $file_name "https://api.bintray.com/content/palantir/releases/spark/${version}/org/apache/spark/spark-dist/${dist_version}/${file_name}"
}

publish_artifacts
make_dist hadoop-2.8.0-palantir6 "${PALANTIR_FLAGS[*]}" --clean
make_dist without-hadoop "-Phadoop-provided -Pkubernetes -Phive -Pyarn -Psparkr" --clean
