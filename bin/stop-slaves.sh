#!/usr/bin/env bash

# Starts the master on the machine this script is executed on.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/spark-config.sh"

if [ -f "${SPARK_CONF_DIR}/spark-env.sh" ]; then
  . "${SPARK_CONF_DIR}/spark-env.sh"
fi

if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  "$bin"/spark-daemons.sh stop spark.deploy.worker.Worker 1
else
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    "$bin"/spark-daemons.sh stop spark.deploy.worker.Worker $(( $i + 1 ))
  done
fi
