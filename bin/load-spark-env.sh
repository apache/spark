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

# This script loads spark-env.sh if it exists, and ensures it is only loaded once.
# spark-env.sh is loaded from SPARK_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

# Figure out where Spark is installed
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

SPARK_ENV_SH="spark-env.sh"
if [ -z "$SPARK_ENV_LOADED" ]; then
  export SPARK_ENV_LOADED=1

  export SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}"/conf}"

  SPARK_ENV_SH="${SPARK_CONF_DIR}/${SPARK_ENV_SH}"
  if [[ -f "${SPARK_ENV_SH}" ]]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . ${SPARK_ENV_SH}
    set +a
  fi
fi

# Setting SPARK_SCALA_VERSION if not already set.
export SPARK_SCALA_VERSION=2.13
#if [ -z "$SPARK_SCALA_VERSION" ]; then
#  SCALA_VERSION_1=2.13
#  SCALA_VERSION_2=2.12
#
#  ASSEMBLY_DIR_1="${SPARK_HOME}/assembly/target/scala-${SCALA_VERSION_1}"
#  ASSEMBLY_DIR_2="${SPARK_HOME}/assembly/target/scala-${SCALA_VERSION_2}"
#  ENV_VARIABLE_DOC="https://spark.apache.org/docs/latest/configuration.html#environment-variables"
#  if [[ -d "$ASSEMBLY_DIR_1" && -d "$ASSEMBLY_DIR_2" ]]; then
#    echo "Presence of build for multiple Scala versions detected ($ASSEMBLY_DIR_1 and $ASSEMBLY_DIR_2)." 1>&2
#    echo "Remove one of them or, export SPARK_SCALA_VERSION=$SCALA_VERSION_1 in ${SPARK_ENV_SH}." 1>&2
#    echo "Visit ${ENV_VARIABLE_DOC} for more details about setting environment variables in spark-env.sh." 1>&2
#    exit 1
#  fi
#
#  if [[ -d "$ASSEMBLY_DIR_1" ]]; then
#    export SPARK_SCALA_VERSION=${SCALA_VERSION_1}
#  else
#    export SPARK_SCALA_VERSION=${SCALA_VERSION_2}
#  fi
#fi

# Append jline option to enable the Beeline process to run in background.
if [ -e /usr/bin/tty -a "`tty`" != "not a tty" -a ! -p /dev/stdin ]; then
  export SPARK_BEELINE_OPTS="$SPARK_BEELINE_OPTS -Djline.terminal=jline.UnsupportedTerminal"
fi
