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

acquire_rat_jar () {

  URL1="http://search.maven.org/remotecontent?filepath=org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"
  URL2="http://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"

  JAR=$rat_jar
  
  if [[ ! -f "$rat_jar" ]]; then
    # Download rat launch jar if it hasn't been downloaded yet
    if [ ! -f ${JAR} ]; then
    # Download
    printf "Attempting to fetch rat\n"
    JAR_DL=${JAR}.part
    if hash curl 2>/dev/null; then
      (curl --progress-bar ${URL1} > ${JAR_DL} || curl --progress-bar ${URL2} > ${JAR_DL}) && mv ${JAR_DL} ${JAR}
    elif hash wget 2>/dev/null; then
      (wget --progress=bar ${URL1} -O ${JAR_DL} || wget --progress=bar ${URL2} -O ${JAR_DL}) && mv ${JAR_DL} ${JAR}
    else
      printf "You do not have curl or wget installed, please install rat manually.\n"
      exit -1
    fi
    fi
    if [ ! -f ${JAR} ]; then
    # We failed to download
    printf "Our attempt to download rat locally to ${JAR} failed. Please install rat manually.\n"
    exit -1
    fi
    printf "Launching rat from ${JAR}\n"
  fi
}
