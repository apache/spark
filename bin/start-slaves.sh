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
exec "$bin/slaves.sh" cd "$SPARK_HOME" \; "$bin/start-slave.sh" spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT
