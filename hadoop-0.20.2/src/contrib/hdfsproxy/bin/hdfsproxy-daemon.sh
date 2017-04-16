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


# Runs a HdfsProxy as a daemon.
#
# Environment Variables
#
#   HDFSPROXY_CONF_DIR  Alternate conf dir. Default is ${HDFSPROXY_HOME}/conf.
#   HDFSPROXY_LOG_DIR   Where log files are stored.  PWD by default.
#   HDFSPROXY_MASTER    host:path where hdfsproxy code should be rsync'd from
#   HDFSPROXY_PID_DIR   The pid files are stored. /tmp by default.
#   HDFSPROXY_IDENT_STRING   A string representing this instance of hdfsproxy. $USER by default
#   HDFSPROXY_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: hdfsproxy-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hdfsproxy-config.sh

# get arguments
startStop=$1
shift

hdfsproxy_rotate_log ()
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

if [ -f "${HDFSPROXY_CONF_DIR}/hdfsproxy-env.sh" ]; then
  . "${HDFSPROXY_CONF_DIR}/hdfsproxy-env.sh"
fi

# get log directory
if [ "$HDFSPROXY_LOG_DIR" = "" ]; then
  export HDFSPROXY_LOG_DIR="$HDFSPROXY_HOME/logs"
fi
mkdir -p "$HDFSPROXY_LOG_DIR"

if [ "$HDFSPROXY_PID_DIR" = "" ]; then
  HDFSPROXY_PID_DIR=/tmp
fi

if [ "$HDFSPROXY_IDENT_STRING" = "" ]; then
  export HDFSPROXY_IDENT_STRING="$USER"
fi

# some variables
export HDFSPROXY_LOGFILE=hdfsproxy-$HDFSPROXY_IDENT_STRING-$HOSTNAME.log
export HDFSPROXY_ROOT_LOGGER="INFO,DRFA"
log=$HDFSPROXY_LOG_DIR/hdfsproxy-$HDFSPROXY_IDENT_STRING-$HOSTNAME.out
pid=$HDFSPROXY_PID_DIR/hdfsproxy-$HDFSPROXY_IDENT_STRING.pid

# Set default scheduling priority
if [ "$HDFSPROXY_NICENESS" = "" ]; then
    export HDFSPROXY_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$HDFSPROXY_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo hdfsproxy running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$HDFSPROXY_MASTER" != "" ]; then
      echo rsync from $HDFSPROXY_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HDFSPROXY_MASTER/ "$HDFSPROXY_HOME"
    fi

    hdfsproxy_rotate_log $log
    echo starting hdfsproxy, logging to $log
    cd "$HDFSPROXY_HOME"
    nohup nice -n $HDFSPROXY_NICENESS "$HDFSPROXY_HOME"/bin/hdfsproxy --config $HDFSPROXY_CONF_DIR "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping hdfsproxy
        kill `cat $pid`
      else
        echo no hdfsproxy to stop
      fi
    else
      echo no hdfsproxy to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


