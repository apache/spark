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

# Enter posix mode for bash 
set -o posix 

# Shell script for starting the Spark Connect server
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.sql.connect.service.SparkConnectServer"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-connect-server.sh [--wait] [options]"

  "${SPARK_HOME}"/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  exit 0
fi

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$1" == "--wait" ]; then
  shift
  export SPARK_NO_DAEMONIZE=1
fi
exec "${SPARK_HOME}"/sbin/spark-daemon.sh submit $CLASS 1 --name "Spark Connect server" "$@"
