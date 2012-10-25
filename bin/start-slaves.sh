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

if [ "$SPARK_MASTER_IP" = "" ]; then
  hostname=`hostname`
  hostouput=`host "$hostname"`

  if [[ "$hostouput" == *"not found"* ]]; then
    echo $hostouput
    echo "Fail to identiy the IP for the master."
    echo "Set SPARK_MASTER_IP explicitly in configuration instead."
    exit 1
  fi
  ip=`host "$hostname" | cut -d " " -f 4`
else
  ip=$SPARK_MASTER_IP
fi

echo "Master IP: $ip"

"$bin"/spark-daemons.sh start spark.deploy.worker.Worker spark://$ip:$SPARK_MASTER_PORT