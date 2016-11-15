#!/usr/bin/env bash

SPARK_HOME="$(cd "`dirname "$0"`"; cd ../..; pwd)"
cd $SPARK_HOME

export HADOOP_PROFILE=hadoop2.6-criteo
export SPARK_BUILD_TOOL=maven

./dev/run-tests
