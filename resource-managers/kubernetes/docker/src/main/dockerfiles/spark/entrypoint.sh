#!/bin/bash
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
uidentry=$(getent passwd $myuid)

# If there is no passwd entry for the container UID, attempt to create one
if [ -z "$uidentry" ] ; then
    if [ -w /etc/passwd ] ; then
        echo "$myuid:x:$myuid:$mygid:anonymous uid:$SPARK_HOME:/bin/false" >> /etc/passwd
    else
        echo "Container ENTRYPOINT failed to add passwd entry for anonymous UID"
    fi
fi

SPARK_K8S_CMD="$1"
if [ -z "$SPARK_K8S_CMD" ]; then
  echo "No command to execute has been provided." 1>&2
  exit 1
fi
shift 1

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
env | grep SPARK_JAVA_OPT_ | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt
readarray -t SPARK_DRIVER_JAVA_OPTS < /tmp/java_opts.txt
if [ -n "$SPARK_MOUNTED_CLASSPATH" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_MOUNTED_CLASSPATH"
fi
if [ -n "$SPARK_MOUNTED_FILES_DIR" ]; then
  cp -R "$SPARK_MOUNTED_FILES_DIR/." .
fi

case "$SPARK_K8S_CMD" in
  driver)
    CMD=(
      ${JAVA_HOME}/bin/java
      "${SPARK_DRIVER_JAVA_OPTS[@]}"
      -cp "$SPARK_CLASSPATH"
      -Xms$SPARK_DRIVER_MEMORY
      -Xmx$SPARK_DRIVER_MEMORY
      -Dspark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS
      $SPARK_DRIVER_CLASS
      $SPARK_DRIVER_ARGS
    )
    ;;

  executor)
    CMD=(
      ${JAVA_HOME}/bin/java
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$SPARK_EXECUTOR_MEMORY
      -Xmx$SPARK_EXECUTOR_MEMORY
      -cp "$SPARK_CLASSPATH"
      org.apache.spark.executor.CoarseGrainedExecutorBackend
      --driver-url $SPARK_DRIVER_URL
      --executor-id $SPARK_EXECUTOR_ID
      --cores $SPARK_EXECUTOR_CORES
      --app-id $SPARK_APPLICATION_ID
      --hostname $SPARK_EXECUTOR_POD_IP
    )
    ;;

  init)
    CMD=(
      "$SPARK_HOME/bin/spark-class"
      "org.apache.spark.deploy.k8s.SparkPodInitContainer"
      "$@"
    )
    ;;

  *)
    echo "Unknown command: $SPARK_K8S_CMD" 1>&2
    exit 1
esac

# Execute the container CMD under tini for better hygiene
exec /sbin/tini -s -- "${CMD[@]}"
