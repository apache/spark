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

# Run a shell command on all worker hosts.
#
# Environment Variables
#
#   SPARK_WORKERS    File naming remote hosts.
#     Default is ${SPARK_CONF_DIR}/workers.
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_WORKER_SLEEP Seconds to sleep between spawning remote commands.
#   SPARK_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: workers.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

# If the workers file is specified in the command line,
# then it takes precedence over the definition in
# spark-env.sh. Save it here.
if [ -f "$SPARK_WORKERS" ]; then
  HOSTLIST=`cat "$SPARK_WORKERS"`
fi
if [ -f "$SPARK_SLAVES" ]; then
  >&2 echo "SPARK_SLAVES is deprecated, use SPARK_WORKERS"
  HOSTLIST=`cat "$SPARK_SLAVES"`
fi


# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$HOSTLIST" = "" ]; then
  if [ "$SPARK_SLAVES" = "" ] && [ "$SPARK_WORKERS" = "" ]; then
    if [ -f "${SPARK_CONF_DIR}/workers" ]; then
      HOSTLIST=`cat "${SPARK_CONF_DIR}/workers"`
    elif [ -f "${SPARK_CONF_DIR}/slaves" ]; then
      HOSTLIST=`cat "${SPARK_CONF_DIR}/slaves"`
    else
      HOSTLIST=localhost
    fi
  else
    if [ -f "$SPARK_WORKERS" ]; then
      HOSTLIST=`cat "$SPARK_WORKERS"`
    fi
    if [ -f "$SPARK_SLAVES" ]; then
      >&2 echo "SPARK_SLAVES is deprecated, use SPARK_WORKERS"
      HOSTLIST=`cat "$SPARK_SLAVES"`
    fi
  fi
fi



# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${SPARK_SSH_FOREGROUND}" ]; then
    ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /"
  else
    ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /" &
  fi
  if [ "$SPARK_WORKER_SLEEP" != "" ]; then
    sleep $SPARK_WORKER_SLEEP
  fi
  if [ "$SPARK_SLAVE_SLEEP" != "" ]; then
    >&2 echo "SPARK_SLAVE_SLEEP is deprecated, use SPARK_WORKER_SLEEP"
    sleep $SPARK_SLAVE_SLEEP
  fi
done

wait
