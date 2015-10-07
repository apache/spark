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

SPARK_TESTING=1 $FWDIR/../bin/sparkR --driver-java-options "-Dlog4j.configuration=file:$FWDIR/log4j.properties" $FWDIR/pkg/tests/run-all.R 2>&1 | tee -a $LOGFILE
FAILED=$((PIPESTATUS[0]||$FAILED))

if [[ $FAILED != 0 ]]; then
    cat $LOGFILE
    echo -en "\033[31m"  # Red
    echo "Had test failures; see logs."
    echo -en "\033[0m"  # No color
    exit -1
else
    echo -en "\033[32m"  # Green
    echo "Tests passed."
    echo -en "\033[0m"  # No color
fi
