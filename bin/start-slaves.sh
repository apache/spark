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
  SPARK_MASTER_IP=`hostname`
fi

echo "Master IP: $SPARK_MASTER_IP"

# Launch the slaves
if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  exec "$bin/slaves.sh" cd "$SPARK_HOME" \; "$bin/start-slave.sh" 1 spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT
else
  if [ "$SPARK_WORKER_WEBUI_PORT" = "" ]; then
    SPARK_WORKER_WEBUI_PORT=8081
  fi
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    "$bin/slaves.sh" cd "$SPARK_HOME" \; "$bin/start-slave.sh" $(( $i + 1 ))  spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT --webui-port $(( $SPARK_WORKER_WEBUI_PORT + $i ))
  done
fi
