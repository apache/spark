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

# A shell script to stop all workers on a single slave
#
# Environment variables
#
#   SPARK_WORKER_INSTANCES The number of worker instances that should be
#                          running on this slave.  Default is 1.

# Usage: stop-slave.sh
#   Stops all slaves on this worker machine

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  "${SPARK_HOME}/sbin"/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
else
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    "${SPARK_HOME}/sbin"/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker $(( $i + 1 ))
  done
fi
