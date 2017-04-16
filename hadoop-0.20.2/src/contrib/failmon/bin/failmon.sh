#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# First we need to determine whether Failmon has been distributed with
# Hadoop, or as standalone. In the latter case failmon.jar will lie in
# the current directory.

JARNAME="failmon.jar"
HADOOPDIR=""
CLASSPATH=""

if [ `ls -l | grep src | wc -l` == 0 ]
then
    # standalone binary
    if [ -n $1 ] && [ "$1" == "--mergeFiles" ]
    then
	jar -ufe $JARNAME org.apache.hadoop.contrib.failmon.HDFSMerger
        java -jar $JARNAME
    else
    	jar -ufe $JARNAME org.apache.hadoop.contrib.failmon.RunOnce
	java -jar $JARNAME $*
    fi
else
    # distributed with Hadoop
    HADOOPDIR=`pwd`/../../../
    CLASSPATH=$CLASSPATH:$HADOOPDIR/build/contrib/failmon/classes
    CLASSPATH=$CLASSPATH:$HADOOPDIR/build/classes
    CLASSPATH=$CLASSPATH:`ls -1 $HADOOPDIR/lib/commons-logging-api-1*.jar`
    CLASSPATH=$CLASSPATH:`ls -1 $HADOOPDIR/lib/commons-logging-1*.jar`
    CLASSPATH=$CLASSPATH:`ls -1 $HADOOPDIR/lib/log4j-*.jar`
#    echo $CLASSPATH
    if [ -n $1 ] && [ "$1" == "--mergeFiles" ]
    then
        java -cp $CLASSPATH org.apache.hadoop.contrib.failmon.HDFSMerger
    else
        java -cp $CLASSPATH org.apache.hadoop.contrib.failmon.RunOnce $*
    fi
fi

