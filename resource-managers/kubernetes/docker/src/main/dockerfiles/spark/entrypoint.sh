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

# echo commands to the terminal output
set -ex

# Check whether there is a passwd entry for the container UID
myuid=$(id -u)
mygid=$(id -g)
# turn off -e for getent because it will return error code in anonymous uid case
set +e
uidentry=$(getent passwd $myuid)
set -e

# If there is no passwd entry for the container UID, attempt to create one
if [ -z "$uidentry" ] ; then
    if [ -w /etc/passwd ] ; then
        echo "$myuid:x:$myuid:$mygid:${SPARK_USER_NAME:-anonymous uid}:$SPARK_HOME:/bin/false" >> /etc/passwd
    else
        echo "Container ENTRYPOINT failed to add passwd entry for anonymous UID"
    fi
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' | awk '{print $3}')
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > java_opts.txt
if [ "$(command -v readarray)" ]; then
  readarray -t SPARK_EXECUTOR_JAVA_OPTS < java_opts.txt
else
  SPARK_EXECUTOR_JAVA_OPTS=("${(@f)$(< java_opts.txt)}")
fi

if [ -n "$SPARK_EXTRA_CLASSPATH" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"
fi

if ! [ -z ${PYSPARK_PYTHON+x} ]; then
    export PYSPARK_PYTHON
fi
if ! [ -z ${PYSPARK_DRIVER_PYTHON+x} ]; then
    export PYSPARK_DRIVER_PYTHON
fi

# If HADOOP_HOME is set and SPARK_DIST_CLASSPATH is not set, set it here so Hadoop jars are available to the executor.
# It does not set SPARK_DIST_CLASSPATH if already set, to avoid overriding customizations of this value from elsewhere e.g. Docker/K8s.
if [ -n "${HADOOP_HOME}"  ] && [ -z "${SPARK_DIST_CLASSPATH}"  ]; then
  export SPARK_DIST_CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"
fi

if ! [ -z ${HADOOP_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH";
fi

if ! [ -z ${SPARK_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH";
elif ! [ -z ${SPARK_HOME+x} ]; then
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH";
fi

# SPARK-43540: add current working directory into executor classpath
SPARK_CLASSPATH="$SPARK_CLASSPATH:$PWD"

case "$1" in
  driver)
    shift 1
    CMD=(
      "$SPARK_HOME/bin/spark-submit"
      --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
      --conf "spark.executorEnv.SPARK_DRIVER_POD_IP=$SPARK_DRIVER_BIND_ADDRESS"
      --deploy-mode client
      "$@"
    )
    # On non-zero exit, write the last 4KB of driver stderr to the Kubernetes
    # termination log so kubectl describe pod surfaces the error message.
    # awk ring buffer streams stderr to kubectl logs in real-time via a named
    # pipe while keeping only the last 4KB in memory (no unbounded /tmp growth).
    TERMINATION_LOG="${TERMINATION_LOG:-/dev/termination-log}"
    STDERR_PIPE=/tmp/driver_stderr_pipe
    STDERR_BUF=/tmp/driver_stderr_last4k
    # Remove stale pipe from a prior SIGKILL'd container restart (emptyDir /tmp).
    rm -f "$STDERR_PIPE"
    mkfifo "$STDERR_PIPE"
    trap 'rm -f "$STDERR_PIPE" "$STDERR_BUF"' EXIT
    # awk ring buffer: clamp each line to 4096 bytes at insertion so that even
    # a single oversized line never overflows the K8s termination log limit.
    awk -v out="$STDERR_BUF" '
      BEGIN { start = 1; bytes = 0 }
      {
        line = (length($0) > 4096) ? substr($0, length($0) - 4095) : $0
        print line > "/dev/stderr"
        buf[NR] = line
        bytes += length(line) + 1
        while (bytes > 4096 && start < NR) {
          bytes -= length(buf[start]) + 1
          delete buf[start++]
        }
      }
      END { for (i = start; i <= NR; i++) print buf[i] > out }
    ' < "$STDERR_PIPE" &
    AWK_PID=$!
    # Run tini as a background child (not exec) so we can write the termination
    # log on exit. Forward SIGTERM so Kubernetes graceful shutdown still reaches
    # tini and the Spark driver (without this, bash as PID 1 ignores SIGTERM).
    set +e  # wait returns tini's exit code; set -e would abort before EXIT_CODE=$? captures it
    /usr/bin/tini -s -- "${CMD[@]}" 2>"$STDERR_PIPE" &
    TINI_PID=$!
    trap 'kill -TERM "$TINI_PID" 2>/dev/null' TERM INT
    wait "$TINI_PID"
    EXIT_CODE=$?
    trap - TERM INT
    rm -f "$STDERR_PIPE"
    wait "$AWK_PID"
    set -e
    if [ $EXIT_CODE -ne 0 ]; then
      cat "$STDERR_BUF" > "$TERMINATION_LOG" 2>/dev/null || true
    fi
    rm -f "$STDERR_BUF"
    exit $EXIT_CODE
    ;;
  executor)
    shift 1
    CMD=(
      ${JAVA_HOME}/bin/java
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$SPARK_EXECUTOR_MEMORY
      -Xmx$SPARK_EXECUTOR_MEMORY
      -cp "$SPARK_CLASSPATH:$SPARK_DIST_CLASSPATH"
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
      --driver-url $SPARK_DRIVER_URL
      --executor-id $SPARK_EXECUTOR_ID
      --cores $SPARK_EXECUTOR_CORES
      --app-id $SPARK_APPLICATION_ID
      --hostname $SPARK_EXECUTOR_POD_IP
      --resourceProfileId $SPARK_RESOURCE_PROFILE_ID
      --podName $SPARK_EXECUTOR_POD_NAME
    )
    ;;

  *)
    echo "Non-spark-on-k8s command provided, proceeding in pass-through mode..."
    CMD=("$@")
    ;;
esac

# Execute the container CMD under tini for better hygiene
exec /usr/bin/tini -s -- "${CMD[@]}"
