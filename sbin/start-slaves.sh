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

# Starts a slave instance on each machine specified in the conf/slaves file.

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"


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

# Find the port number for the master
if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

if [ "$SPARK_MASTER_IP" = "" ]; then
  SPARK_MASTER_IP="`hostname`"
fi

if [ "$START_TACHYON" == "true" ]; then
  "$sbin/slaves.sh" cd "$SPARK_HOME" \; "$sbin"/../tachyon/bin/tachyon bootstrap-conf "$SPARK_MASTER_IP"

  # set -t so we can call sudo
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no -t" "$sbin/slaves.sh" cd "$SPARK_HOME" \; "$sbin/../tachyon/bin/tachyon-start.sh" worker SudoMount \; sleep 1
fi

# Launch the slaves
"$sbin/slaves.sh" cd "$SPARK_HOME" \; "$sbin/start-slave.sh" "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
