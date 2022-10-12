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

# Script to create SQL API and config docs. This requires `mkdocs` and to build
# Spark first. After running this script the html docs can be found in
# $SPARK_HOME/sql/site

set -o pipefail
set -e

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
SPARK_HOME="$(cd "`dirname "${BASH_SOURCE[0]}"`"/..; pwd)"

if ! hash python3 2>/dev/null; then
  echo "Missing python3 in your path, skipping SQL documentation generation."
  exit 0
fi

if ! hash mkdocs 2>/dev/null; then
  echo "Missing mkdocs in your path, trying to install mkdocs for SQL documentation generation."
  pip3 install mkdocs
fi

pushd "$FWDIR" > /dev/null

rm -fr docs
mkdir docs

echo "Generating SQL API Markdown files."
"$SPARK_HOME/bin/spark-submit" gen-sql-api-docs.py

echo "Generating SQL configuration table HTML file."
"$SPARK_HOME/bin/spark-submit" gen-sql-config-docs.py

echo "Generating HTML files for SQL function table and examples."
"$SPARK_HOME/bin/spark-submit" gen-sql-functions-docs.py

echo "Generating HTML files for SQL API documentation."
mkdocs build --clean
rm -fr docs

popd
