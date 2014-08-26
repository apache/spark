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

# Starts the history server on the machine this script is executed on.
#
# Usage: start-history-server.sh
#
# Use the SPARK_HISTORY_OPTS environment variable to set history server configuration.
#

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/spark-config.sh"
. "$SPARK_PREFIX/bin/load-spark-env.sh"

if [ $# != 0 ]; then
  echo "Using command line arguments for setting the log directory is deprecated. Please "
  echo "set the spark.history.fs.logDirectory configuration option instead."
  export SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=$1"
fi

exec "$sbin"/spark-daemon.sh start org.apache.spark.deploy.history.HistoryServer 1
