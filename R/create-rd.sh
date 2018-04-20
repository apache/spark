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

# This scripts packages the SparkR source files (R and C files) and
# creates a package that can be loaded in R. The package is by default installed to
# $FWDIR/lib and the package can be loaded by using the following command in R:
#
#   library(SparkR, lib.loc="$FWDIR/lib")
#
# NOTE(shivaram): Right now we use $SPARK_HOME/R/lib to be the installation directory
# to load the SparkR package on the worker nodes.

set -o pipefail
set -e

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
pushd "$FWDIR" > /dev/null
. "$FWDIR/find-r.sh"

# Generate Rd files if devtools is installed
"$R_SCRIPT_PATH/Rscript" -e ' if("devtools" %in% rownames(installed.packages())) { library(devtools); devtools::document(pkg="./pkg", roclets=c("rd")) }'
