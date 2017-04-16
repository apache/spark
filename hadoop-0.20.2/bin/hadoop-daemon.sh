#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Hadoop command as a daemon.
#
# Environment Variables
#
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HADOOP_LOG_DIR   Where log files are stored.  PWD by default.
#   HADOOP_MASTER    host:path where hadoop code should be rsync'd from
#   HADOOP_PID_DIR   The pid files are stored. /tmp by default.
#   HADOOP_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   HADOOP_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: hadoop-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <hadoop-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

hadoop_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$_HADOOP_DAEMON_OUT" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$_HADOOP_DAEMON_OUT.$prev" ] && mv "$_HADOOP_DAEMON_OUT.$prev" "$_HADOOP_DAEMON_OUT.$num"
	    num=$prev
	done
	mv "$_HADOOP_DAEMON_OUT" "$_HADOOP_DAEMON_OUT.$num";
    fi
}

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

if [ "$HADOOP_IDENT_STRING" = "" ]; then
  export HADOOP_IDENT_STRING="$USER"
fi

# get log directory
if [ "$HADOOP_LOG_DIR" = "" ]; then
  export HADOOP_LOG_DIR="$HADOOP_HOME/logs"
fi
mkdir -p "$HADOOP_LOG_DIR"

if [ "$HADOOP_PID_DIR" = "" ]; then
  HADOOP_PID_DIR=/tmp
fi

# some variables
export HADOOP_LOGFILE=hadoop-$HADOOP_IDENT_STRING-$command-$HOSTNAME.log
export HADOOP_ROOT_LOGGER="INFO,DRFA"
export HADOOP_SECURITY_LOGGER="INFO,DRFAS"
export _HADOOP_DAEMON_OUT=$HADOOP_LOG_DIR/hadoop-$HADOOP_IDENT_STRING-$command-$HOSTNAME.out
export _HADOOP_DAEMON_PIDFILE=$HADOOP_PID_DIR/hadoop-$HADOOP_IDENT_STRING-$command.pid
export _HADOOP_DAEMON_DETACHED="true"

# Set default scheduling priority
if [ "$HADOOP_NICENESS" = "" ]; then
    export HADOOP_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$HADOOP_PID_DIR"

    if [ -f $_HADOOP_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_HADOOP_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo $command running as process `cat $_HADOOP_DAEMON_PIDFILE`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$HADOOP_MASTER" != "" ]; then
      echo rsync from $HADOOP_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HADOOP_MASTER/ "$HADOOP_HOME"
    fi

    hadoop_rotate_log $_HADOOP_DAEMON_OUT
    echo starting $command, logging to $_HADOOP_DAEMON_OUT
    cd "$HADOOP_HOME"

    nice -n $HADOOP_NICENESS "$HADOOP_HOME"/bin/hadoop --config $HADOOP_CONF_DIR $command "$@" < /dev/null
    ;;
          
  (stop)

    if [ -f $_HADOOP_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_HADOOP_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $_HADOOP_DAEMON_PIDFILE`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


