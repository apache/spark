#!/usr/bin/env bash

set -euo pipefail

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
MVN_LOCAL=~/.m2/repository

source "$FWDIR/publish_functions.sh"

set_version_and_install
DONT_BUILD=true make_dist
mkdir -p $MVN_LOCAL/org/apache/spark/${artifact_name}/${version}
cp $file_name $MVN_LOCAL/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz
