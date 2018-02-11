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

# Script to create SQL API docs. This requires `mkdocs` and to build
# Spark first. After running this script the html docs can be found in
# $SPARK_HOME/sql/site

set -o pipefail
set -e

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"
SPARK_HOME="$(cd "`dirname "${BASH_SOURCE[0]}"`"/..; pwd)"

if ! hash python 2>/dev/null; then
  echo "Missing python in your path, skipping SQL documentation generation."
  exit 0
fi

if ! hash mkdocs 2>/dev/null; then
  echo "Missing mkdocs in your path, trying to install mkdocs for SQL documentation generation."
  pip install mkdocs
fi

pushd "$FWDIR" > /dev/null

# Now create the markdown file
rm -fr docs
mkdir docs
echo "Generating markdown files for SQL documentation."
"$SPARK_HOME/bin/spark-submit" gen-sql-markdown.py

# Now create the HTML files
echo "Generating HTML files for SQL documentation."
mkdocs build --clean
rm -fr docs

popd
