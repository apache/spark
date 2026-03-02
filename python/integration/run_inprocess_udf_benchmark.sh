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

# Run the in-process UDF vs pandas UDF benchmark.
#
# Prerequisites (same as the integration tests):
#   python3 -m venv python/.venv-inprocess
#   python/.venv-inprocess/bin/pip install jep pyarrow cloudpickle pandas pytest
#
#   build/sbt "sql/package"
#   cp sql/core/target/scala-2.13/spark-sql_2.13-*.jar \
#      assembly/target/scala-2.13/jars/
#
# Usage:
#   bash python/integration/run_inprocess_udf_benchmark.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

VENV_DIR="$SPARK_HOME/python/.venv-inprocess"
VENV_PY="$VENV_DIR/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "ERROR: Virtual environment not found at $VENV_DIR" >&2
  echo "       Run: python3 -m venv python/.venv-inprocess" >&2
  echo "            python/.venv-inprocess/bin/pip install jep pyarrow cloudpickle pandas" >&2
  exit 1
fi

JEP_JAR="$(find "$VENV_DIR" -name "jep-*.jar" 2>/dev/null | head -1)"
if [[ -z "$JEP_JAR" ]]; then
  echo "ERROR: jep JAR not found in $VENV_DIR" >&2
  exit 1
fi
JEP_NATIVE_DIR="$(dirname "$JEP_JAR")"

PY4J_ZIP="$(find "$SPARK_HOME/python/lib" -name "py4j-*-src.zip" | head -1)"
VENV_SITE="$(dirname "$JEP_NATIVE_DIR")"

export SPARK_HOME
export INPROCESS_TESTS=1
export PYTHONPATH="$VENV_SITE:$SPARK_HOME/python:$PY4J_ZIP"
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g --driver-class-path $JEP_JAR --driver-java-options \"-Djava.library.path=$JEP_NATIVE_DIR -XX:MaxDirectMemorySize=8g\" pyspark-shell"

exec "$VENV_PY" "$SCRIPT_DIR/benchmark_inprocess_udf.py"
