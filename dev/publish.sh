#!/usr/bin/env bash

set -euo pipefail

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"

source "$FWDIR/publish_functions.sh"

publish_artifacts | tee -a "/tmp/publish_artifacts.log"
DONT_BUILD=true make_dist_and_deploy | tee -a "/tmp/make-dist.log"
