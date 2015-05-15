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

# Starts the master on the machine this script is executed on.

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

ORIGINAL_ARGS="$@"

START_TACHYON=false

while (( "$#" )); do
case $1 in
    --with-tachyon)
      if [ ! -e "$sbin"/../tachyon/bin/tachyon ]; then
        echo "Error: --with-tachyon specified, but tachyon not found."
        exit -1
      fi
      START_TACHYON=true
      ;;
  esac
shift
done

. "$sbin/spark-config.sh"

. "$SPARK_PREFIX/bin/load-spark-env.sh"

if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

if [ "$SPARK_MASTER_IP" = "" ]; then
  SPARK_MASTER_IP=`hostname`
fi

if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi

"$sbin"/spark-daemon.sh start org.apache.spark.deploy.master.Master 1 \
  --ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS

if [ "$START_TACHYON" == "true" ]; then
  "$sbin"/../tachyon/bin/tachyon bootstrap-conf $SPARK_MASTER_IP
  "$sbin"/../tachyon/bin/tachyon format -s
  "$sbin"/../tachyon/bin/tachyon-start.sh master
fi
