#!/usr/bin/env bash

export SPARK_HOME="$(cd "`dirname "$0"`"; cd ../..; pwd)"

export CRITEO_HADOOP_VERSION=2.6.0-cdh5.5.0

export CRITEO_SPARK_PROFILES="-Pyarn -Phadoop-2.6 -Dhadoop.version=$CRITEO_HADOOP_VERSION -Phive -Phive-thriftserver"
