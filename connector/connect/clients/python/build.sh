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

# Until we fully release Spark Connect, we will continue adding code inside
# PySpark first and then build a release package from that.

# connector/connect/clients/python
SPARK_HOME="$(cd "`dirname $0`"/../../../..; pwd)"
RELEASE_PATH="$(cd "`dirname $0`"/pyspark-connect; pwd)"


# Copy the connect folder into the right place
mkdir -p $RELEASE_PATH/pyspark/sql

# Copy Spark Connect
cp -R $SPARK_HOME/python/pyspark/sql/connect $RELEASE_PATH/pyspark/sql/

#Cloudpickle
cp -R $SPARK_HOME/python/pyspark/cloudpickle $RELEASE_PATH/pyspark/

#################################################################################################
# The below files are manually copied and must be synced manually to remove unavailable imports.
#cp -R $SPARK_HOME/python/pyspark/serializers.py $RELEASE_PATH/pyspark/
#cp -R $SPARK_HOME/python/pyspark/util.py $RELEASE_PATH/pyspark/
#cp -R $SPARK_HOME/python/pyspark/sql/utils.py $RELEASE_PATH/pyspark/sql/
# cp -R $SPARK_HOME/python/pyspark/sql/types.py $RELEASE_PATH/pyspark/sql/

pushd $RELEASE_PATH
poetry build
popd


echo "Release build: ${RELEASE_PATH}/dist"