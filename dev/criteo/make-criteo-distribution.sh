#!/usr/bin/env bash

source `dirname "$0"`/criteo-spark-env.sh

$SPARK_HOME/make-distribution.sh --name criteo --tgz $CRITEO_SPARK_PROFILES
