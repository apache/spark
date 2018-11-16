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

if [ -z "$SPARK_ENV_LOADED" ]; then
  export SPARK_ENV_LOADED=1

  export SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}"/conf}"

  if [ -f "${SPARK_CONF_DIR}/spark-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${SPARK_CONF_DIR}/spark-env.sh"
    set +a
  fi
fi

# Setting SPARK_SCALA_VERSION if not already set.

if [ -z "$SPARK_SCALA_VERSION" ]; then
  SCALA_VERSION_1=2.12
  SCALA_VERSION_2=2.11

  ASSEMBLY_DIR_1="${SPARK_HOME}/assembly/target/scala-${SCALA_VERSION_1}"
  ASSEMBLY_DIR_2="${SPARK_HOME}/assembly/target/scala-${SCALA_VERSION_2}"
  if [[ -d "$ASSEMBLY_DIR_1" && -d "$ASSEMBLY_DIR_2" ]]; then
    echo "Presence of build for multiple Scala versions detected ($ASSEMBLY_DIR_1 and $ASSEMBLY_DIR_2)." 1>&2
    echo "Remove one of them or, export SPARK_SCALA_VERSION=$SCALA_VERSION_1 in ${SPARK_CONF_DIR}/spark-env.sh." 1>&2
    exit 1
  fi

  if [[ -d "$ASSEMBLY_DIR_1" ]]; then
    export SPARK_SCALA_VERSION=${SCALA_VERSION_1}
  else
    export SPARK_SCALA_VERSION=${SCALA_VERSION_2}
  fi
fi
