#!/bin/bash

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

# Script to create API docs and vignettes for SparkR
# This requires `devtools`, `knitr` and `rmarkdown` to be installed on the machine.

# After running this script the html docs can be found in
# $SPARK_HOME/R/pkg/html
# The vignettes can be found in
# $SPARK_HOME/R/pkg/vignettes/sparkr_vignettes.html

set -o pipefail
set -e

# Figure out where the script is
export FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
export SPARK_HOME="$(cd "`dirname "${BASH_SOURCE[0]}"`"/..; pwd)"

# Required for setting SPARK_SCALA_VERSION
. "${SPARK_HOME}/bin/load-spark-env.sh"

echo "Using Scala $SPARK_SCALA_VERSION"

pushd "$FWDIR" > /dev/null
. "$FWDIR/find-r.sh"

# Install the package (this will also generate the Rd files)
. "$FWDIR/install-dev.sh"

# Now create HTML files

# knit_rd puts html in current working directory
mkdir -p pkg/html
pushd pkg/html

"$R_SCRIPT_PATH/Rscript" -e 'libDir <- "../../lib"; library(SparkR, lib.loc=libDir); library(knitr); knit_rd("SparkR", links = tools::findHTMLlinks(paste(libDir, "SparkR", sep="/")))'

popd

popd
