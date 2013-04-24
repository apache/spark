#!/usr/bin/env bash

# Run a Spark command on all slave hosts.

usage="Usage: spark-daemons.sh [--config confdir] [--hosts hostlistfile] [start|stop] command instance-number args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/spark-config.sh"

exec "$bin/slaves.sh" cd "$SPARK_HOME" \; "$bin/spark-daemon.sh" "$@"
