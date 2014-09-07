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

# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   SPARK_SLAVES    File naming remote hosts.
#     Default is ${SPARK_CONF_DIR}/slaves.
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   SPARK_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: slaves.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# Figure out where Spark is installed
SOURCE=$0
while [ -h "$SOURCE" ]
do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
export SPARK_HOME="$(cd `dirname $SOURCE`/..; pwd)"

. "$SPARK_HOME/sbin/spark-config.sh"

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
# spark-env.sh. Save it here.
HOSTLIST=$SPARK_SLAVES

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir=$1
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR=$conf_dir
  fi
  shift
fi

. "$SPARK_HOME/bin/load-spark-env.sh"

if [ "$HOSTLIST" = "" ]; then
  if [ "$SPARK_SLAVES" = "" ]; then
    export HOSTLIST="${SPARK_CONF_DIR}/slaves"
  else
    export HOSTLIST="${SPARK_SLAVES}"
  fi
fi

# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $SPARK_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$SPARK_SLAVE_SLEEP" != "" ]; then
   sleep $SPARK_SLAVE_SLEEP
 fi
done

wait
