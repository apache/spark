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

FWDIR="$(cd `dirname $0`; pwd)"

FAILED=0
LOGFILE=$FWDIR/unit-tests.out
rm -f $LOGFILE

SPARK_AVRO_JAR_PATH=$(find $FWDIR/../external/avro/ -name "spark-avro*jar" -print | egrep -v "tests.jar|test-sources.jar|sources.jar|javadoc.jar")

if [[ $(echo $SPARK_AVRO_JAR_PATH | wc -l) -eq 1 ]]; then
  SPARK_JARS=$SPARK_AVRO_JAR_PATH
fi

if [ -z "$SPARK_JARS" ]; then
  SPARK_TESTING=1 NOT_CRAN=true $FWDIR/../bin/spark-submit --driver-java-options "-Dlog4j.configurationFile=file:$FWDIR/log4j2.properties" --conf spark.hadoop.fs.defaultFS="file:///" --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" $FWDIR/pkg/tests/run-all.R 2>&1 | tee -a $LOGFILE
else
  SPARK_TESTING=1 NOT_CRAN=true $FWDIR/../bin/spark-submit --jars $SPARK_JARS --driver-java-options "-Dlog4j.configurationFile=file:$FWDIR/log4j2.properties" --conf spark.hadoop.fs.defaultFS="file:///" --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" $FWDIR/pkg/tests/run-all.R 2>&1 | tee -a $LOGFILE
fi

FAILED=$((PIPESTATUS[0]||$FAILED))

NUM_TEST_WARNING="$(grep -c -e 'Warnings ----------------' $LOGFILE)"

# Also run the documentation tests for CRAN
CRAN_CHECK_LOG_FILE=$FWDIR/cran-check.out
rm -f $CRAN_CHECK_LOG_FILE

NO_TESTS=1 NO_MANUAL=1 $FWDIR/check-cran.sh 2>&1 | tee -a $CRAN_CHECK_LOG_FILE
FAILED=$((PIPESTATUS[0]||$FAILED))

NUM_CRAN_WARNING="$(grep -c WARNING$ $CRAN_CHECK_LOG_FILE)"
NUM_CRAN_ERROR="$(grep -c ERROR$ $CRAN_CHECK_LOG_FILE)"
NUM_CRAN_NOTES="$(grep -c NOTE$ $CRAN_CHECK_LOG_FILE)"
HAS_PACKAGE_VERSION_WARN="$(grep -c "Insufficient package version" $CRAN_CHECK_LOG_FILE)"

if [[ $FAILED != 0 || $NUM_TEST_WARNING != 0 ]]; then
    cat $LOGFILE
    echo -en "\033[31m"  # Red
    echo "Had test warnings or failures; see logs."
    echo -en "\033[0m"  # No color
    exit -1
else
    # We have 2 NOTEs: for RoxygenNote and one in Jenkins only "No repository set"
    # For non-latest version branches, one WARNING for package version
    if [[ ($NUM_CRAN_WARNING != 0 || $NUM_CRAN_ERROR != 0 || $NUM_CRAN_NOTES -gt 2) &&
          ($HAS_PACKAGE_VERSION_WARN != 1 || $NUM_CRAN_WARNING != 1 || $NUM_CRAN_ERROR != 0 || $NUM_CRAN_NOTES -gt 1) ]]; then
      cat $CRAN_CHECK_LOG_FILE
      echo -en "\033[31m"  # Red
      echo "Had CRAN check errors; see logs."
      echo -en "\033[0m"  # No color
      exit -1
    else
      echo -en "\033[32m"  # Green
      echo "Tests passed."
      echo -en "\033[0m"  # No color
    fi
fi
