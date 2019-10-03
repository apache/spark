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

set -o pipefail
set -e

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
pushd "$FWDIR" > /dev/null

. "$FWDIR/find-r.sh"

# Install the package (this is required for code in vignettes to run when building it later)
# Build the latest docs, but not vignettes, which is built with the package next
. "$FWDIR/install-dev.sh"

# Build source package with vignettes
SPARK_HOME="$(cd "${FWDIR}"/..; pwd)"
. "${SPARK_HOME}/bin/load-spark-env.sh"
if [ -f "${SPARK_HOME}/RELEASE" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi

if [ -d "$SPARK_JARS_DIR" ]; then
  # Build a zip file containing the source package with vignettes
  SPARK_HOME="${SPARK_HOME}" "$R_SCRIPT_PATH/R" CMD build "$FWDIR/pkg"

  find pkg/vignettes/. -not -name '.' -not -name '*.Rmd' -not -name '*.md' -not -name '*.pdf' -not -name '*.html' -delete
else
  echo "Error Spark JARs not found in '$SPARK_HOME'"
  exit 1
fi

# Run check as-cran.
VERSION=`grep Version "$FWDIR/pkg/DESCRIPTION" | awk '{print $NF}'`

CRAN_CHECK_OPTIONS="--as-cran"

if [ -n "$NO_TESTS" ]
then
  CRAN_CHECK_OPTIONS=$CRAN_CHECK_OPTIONS" --no-tests"
fi

if [ -n "$NO_MANUAL" ]
then
  CRAN_CHECK_OPTIONS=$CRAN_CHECK_OPTIONS" --no-manual --no-vignettes"
fi

echo "Running CRAN check with $CRAN_CHECK_OPTIONS options"

# Remove this environment variable to allow to check suggested packages once
# Jenkins installs arrow. See SPARK-29339.
export _R_CHECK_FORCE_SUGGESTS_=FALSE

if [ -n "$NO_TESTS" ] && [ -n "$NO_MANUAL" ]
then
  "$R_SCRIPT_PATH/R" CMD check $CRAN_CHECK_OPTIONS "SparkR_$VERSION.tar.gz"
else
  # This will run tests and/or build vignettes, and require SPARK_HOME
  SPARK_HOME="${SPARK_HOME}" "$R_SCRIPT_PATH/R" CMD check $CRAN_CHECK_OPTIONS "SparkR_$VERSION.tar.gz"
fi

popd > /dev/null
