#!/usr/bin/env bash

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

usage="Usage: slaves.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/spark-config.sh"

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in 
# spark-env.sh. Save it here.
HOSTLIST=$SPARK_SLAVES

if [ -f "${SPARK_CONF_DIR}/spark-env.sh" ]; then
  . "${SPARK_CONF_DIR}/spark-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$SPARK_SLAVES" = "" ]; then
    export HOSTLIST="${SPARK_CONF_DIR}/slaves"
  else
    export HOSTLIST="${SPARK_SLAVES}"
  fi
fi

echo $"${@// /\\ }"

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
