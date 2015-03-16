#!/bin/bash

# Install development version of SparkR

FWDIR="$(cd `dirname $0`; pwd)"
LIB_DIR="$FWDIR/lib"

mkdir -p $LIB_DIR

# Install R
R CMD INSTALL --library=$LIB_DIR pkg/
