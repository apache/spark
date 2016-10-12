#!/usr/bin/env bash

SPARK_HOME="$(cd "`dirname "$0"`"; cd ../..; pwd)"

$SPARK_HOME/make-distribution.sh --name criteo --tgz -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0-cdh5.5.0 -Phive -Phive-thriftserver
