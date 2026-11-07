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

#
# Cloud Agent install script for Apache Spark.
#
# Idempotent: prepares the Python tooling PySpark needs, installs PySpark's
# runtime Python dependencies, and builds the Spark assembly + examples (and
# every module they depend on) so that bin/spark-shell, bin/pyspark and
# bin/run-example work out of the box.
#
# Heavy, source-derived work belongs here (it runs once to create the
# environment build snapshot). No long-running service is required, so there
# is no `start` phase.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Some Spark dev scripts invoke `python`; make sure it resolves to python3.
if ! command -v python >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y python-is-python3
fi

# PySpark runtime dependencies. Versions mirror the minimums declared in
# python/packaging/classic/setup.py. Installed to the user site so they persist
# in the snapshot without needing a virtualenv on PATH.
python3 -m pip install --break-system-packages --user \
  "py4j==0.10.9.9" \
  "numpy>=1.21" \
  "pandas>=2.2.0" \
  "pyarrow>=15.0.0" \
  "grpcio>=1.67.0" \
  "grpcio-status>=1.67.0" \
  "googleapis-common-protos>=1.65.0"

# Build the Spark assembly + examples (and their upstream modules). The
# build/mvn wrapper downloads the pinned Maven version into build/ automatically.
# JAVA_HOME is derived from javac by the wrapper; java 21 is on PATH at runtime.
export MAVEN_OPTS="${MAVEN_OPTS:--Xss128m -Xmx6g -XX:ReservedCodeCacheSize=256m}"
./build/mvn -DskipTests -pl assembly,examples -am package
