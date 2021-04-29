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

set -e
SELF=$(cd $(dirname $0) && pwd)

# Re-uses helper funcs for the release scripts
if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  . "$SELF/release-util.sh"
else
  . "$SELF/../create-release/release-util.sh"
fi

# Checks out tpcds-kit and builds dsdgen
rm -rf tpcds-kit
git clone https://github.com/databricks/tpcds-kit
cd tpcds-kit/tools
run_silent "Building dsdgen in tpcds-kit..." "$SELF/dsdgen-build.log" make OS=LINUX
DSDGEN=`pwd`
cd ../..

# Builds Spark to generate TPC-DS data
OUTPUT_PATH=`pwd`/tpcds-data
if [ -z "$SCALE_FACTOR" ]; then
  SCALE_FACTOR=1
fi

rm -rf spark
git clone https://github.com/apache/spark
cd spark
run_silent "Building Spark to generate TPC-DS data in $OUTPUT_PATH..." "$SELF/spark-build.log" \
  ./build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSData --dsdgenDir $DSDGEN --location $OUTPUT_PATH --scaleFactor $SCALE_FACTOR --numPartitions 1 --overwrite"
cd ..

rm -rf spark tpcds-kit
