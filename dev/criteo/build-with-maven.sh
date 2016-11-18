#!/usr/bin/env bash

source `dirname "$0"`/criteo-spark-env.sh

$SPARK_HOME/build/mvn -T 1C $CRITEO_SPARK_PROFILES -DskipTests "${@:-package}"
