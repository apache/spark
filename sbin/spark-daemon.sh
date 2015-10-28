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

# Runs a Spark command as a daemon.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
#   SPARK_MASTER    host:path where spark code should be rsync'd from
#   SPARK_PID_DIR   The pid files are stored. /tmp by default.
#   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
#   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: spark-daemon.sh [--config <conf-dir>] (start|stop|submit|status) <spark-command> <spark-instance-number> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

# get arguments

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

option=$1
shift
command=$1
shift
instance=$1
shift

spark_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_IDENT_STRING" = "" ]; then
  export SPARK_IDENT_STRING="$USER"
fi


export SPARK_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$SPARK_LOG_DIR" = "" ]; then
  export SPARK_LOG_DIR="${SPARK_HOME}/logs"
fi
mkdir -p "$SPARK_LOG_DIR"
touch "$SPARK_LOG_DIR"/.spark_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$SPARK_LOG_DIR"/.spark_test
else
  chown "$SPARK_IDENT_STRING" "$SPARK_LOG_DIR"
fi

if [ "$SPARK_PID_DIR" = "" ]; then
  SPARK_PID_DIR=/tmp
fi

# some variables
log="$SPARK_LOG_DIR/spark-$SPARK_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$SPARK_PID_DIR/spark-$SPARK_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$SPARK_NICENESS" = "" ]; then
    export SPARK_NICENESS=0
fi

run_command() {
  mode="$1"
  shift

  mkdir -p "$SPARK_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  if [ "$SPARK_MASTER" != "" ]; then
    echo rsync from "$SPARK_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
  fi

  spark_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (submit)
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-submit --class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

  echo "$newpid" > "$pid"
  sleep 2
  # Check if the process has died; in that case we'll tail the log so the user can see
  if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
    echo "failed to launch $command:"
    tail -2 "$log" | sed 's/^/  /'
    echo "full log in $log"
  fi
}

case $option in

  (submit)
    run_command submit "$@"
    ;;

  (start)
    run_command class "$@"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


