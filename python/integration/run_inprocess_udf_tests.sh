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

# Run end-to-end integration tests for in-process Python UDFs.
#
# Prerequisites (one-time setup):
#   python3 -m venv python/.venv-inprocess
#   python/.venv-inprocess/bin/pip install jep pyarrow cloudpickle pytest
#
#   build/sbt "sql/package"
#   cp sql/core/target/scala-2.13/spark-sql_2.13-*.jar \
#      assembly/target/scala-2.13/jars/
#
# Usage:
#   bash python/integration/run_inprocess_udf_tests.sh          # stop on first failure
#   bash python/integration/run_inprocess_udf_tests.sh --no-x   # run all tests

set -e

# Resolve SPARK_HOME as the repo root (two directories above this script).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

VENV_DIR="$SPARK_HOME/python/.venv-inprocess"
VENV_PY="$VENV_DIR/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "ERROR: Virtual environment not found at $VENV_DIR" >&2
  echo "       Run: python3 -m venv python/.venv-inprocess" >&2
  echo "            python/.venv-inprocess/bin/pip install jep pyarrow cloudpickle pytest" >&2
  exit 1
fi

# Locate jep JAR and native library directory inside the venv.
JEP_JAR="$(find "$VENV_DIR" -name "jep-*.jar" 2>/dev/null | head -1)"
if [[ -z "$JEP_JAR" ]]; then
  echo "ERROR: jep JAR not found in $VENV_DIR" >&2
  echo "       Run: $VENV_DIR/bin/pip install jep" >&2
  exit 1
fi
JEP_NATIVE_DIR="$(dirname "$JEP_JAR")"

# PYTHONPATH: venv site-packages first so the embedded jep interpreter
# can import 'jep' and 'pyspark.inprocess'.
PY4J_ZIP="$(find "$SPARK_HOME/python/lib" -name "py4j-*-src.zip" | head -1)"
VENV_SITE="$(dirname "$JEP_NATIVE_DIR")"

export SPARK_HOME
export INPROCESS_TESTS=1
export PYTHONPATH="$VENV_SITE:$SPARK_HOME/python:$PY4J_ZIP"
# Add jep JAR to driver classpath; set java.library.path for libjep native library.
export PYSPARK_SUBMIT_ARGS="--driver-class-path $JEP_JAR --driver-java-options -Djava.library.path=$JEP_NATIVE_DIR pyspark-shell"

PYTEST_ARGS=(-v --no-header -x)
if [[ "${1:-}" == "--no-x" ]]; then
  PYTEST_ARGS=(-v --no-header)
fi

exec "$VENV_PY" -m pytest \
  "$SPARK_HOME/python/pyspark/inprocess/tests/test_inprocess_udf.py" \
  "${PYTEST_ARGS[@]}"
