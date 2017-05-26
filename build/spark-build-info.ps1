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

# This script generates the build info for spark and places it into the spark-version-info.properties file.
# Arguments:
#   RESOURCE_DIR - The target directory where properties file would be created. [./core/target/extra-resources]
#   SPARK_VERSION - The current version of spark

param (
  [string]$RESOURCE_DIR,
  [string]$SPARK_VERSION
)

if (-not (Test-Path $RESOURCE_DIR)) {
  mkdir $RESOURCE_DIR
}
$SPARK_BUILD_INFO="$RESOURCE_DIR\spark-version-info.properties"

function echo_build_properties {
  echo version=$SPARK_VERSION
  echo user=$env:USERNAME
  echo revision=$(git rev-parse HEAD)
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo date=$((Get-date).ToUniversalTime().toString("yyyy-MM-ddThh:mm:ssZ"))
  echo url=$(git config --get remote.origin.url)
}

echo_build_properties > $SPARK_BUILD_INFO
