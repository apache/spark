#!/bin/bash

# Script to create API docs for SparkR
# This requires `devtools` and `knitr` to be installed on the machine.

# After running this script the html docs can be found in 
# $SPARK_HOME/R/pkg/html

# Figure out where the script is
export FWDIR="$(cd "`dirname "$0"`"; pwd)"
pushd $FWDIR

# Generate Rd file
Rscript -e 'library(devtools); devtools::document(pkg="./pkg", roclets=c("rd"))'

# Install the package
./install-dev.sh

# Now create HTML files

# knit_rd puts html in current working directory
mkdir -p pkg/html
pushd pkg/html

Rscript -e 'library(SparkR, lib.loc="../../lib"); library(knitr); knit_rd("SparkR")'

popd

popd
