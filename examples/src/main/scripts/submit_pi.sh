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

# Usage: Use Spark Cluster and 'pi.py' Python script
#        In case of standalone clusters on K8s, we can use the local file.
#        - local:///opt/spark/examples/src/main/python/pi.py
#        Otherwise, use any cluster-accessible file via HTTP/HTTPS, S3, or HDFS
#        - https://raw.githubusercontent.com/apache/spark/master/examples/src/main/python/pi.py
#
#    submit_pi.sh <spark_master_hostname> <location_of_python_script>
#

SPARK_MASTER=${1:-localhost}
PYTHON_FILE=${2:-https://raw.githubusercontent.com/apache/spark/master/examples/src/main/python/pi.py}

curl -XPOST http://$SPARK_MASTER:6066/v1/submissions/create \
--data '{
  "appResource": "",
  "sparkProperties": {
    "spark.submit.deployMode": "cluster",
    "spark.app.name": "SparkPi",
    "spark.driver.cores": "1",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
    "spark.cores.max": "2"
  },
  "clientSparkVersion": "",
  "mainClass": "org.apache.spark.deploy.SparkSubmit",
  "environmentVariables": {
    "MASTER": "spark://'$SPARK_MASTER':7077"
  },
  "action": "CreateSubmissionRequest",
  "appArgs": [ "'$PYTHON_FILE'", "2000" ]
}'
