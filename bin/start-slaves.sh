#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
