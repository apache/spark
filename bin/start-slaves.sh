#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/spark-config.sh"

if [ -f "${SPARK_CONF_DIR}/spark-env.sh" ]; then
  . "${SPARK_CONF_DIR}/spark-env.sh"
fi

# Find the port number for the master
if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

hostname=`hostname`
ip=`host "$hostname" | cut -d " " -f 4`

"$bin"/spark-daemons.sh start spark.deploy.worker.Worker spark://$ip:$SPARK_MASTER_PORT