#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# Check if HADOOP_HOME AND JAVA_HOME is set.
if [ -z $HADOOP_HOME ] ; then
  echo "HADOOP_HOME environment variable not defined"
  exit -1;
fi

if [ -z $JAVA_HOME ] ; then
  echo "JAVA_HOME environment variable not defined"
  exit -1;
fi

hadoopVersion=`$HADOOP_HOME/bin/hadoop version | awk 'BEGIN { RS = "" ; FS = "\n" } ; { print $1 }' | awk '{print $2}'`

$JAVA_HOME/bin/java -classpath $HADOOP_HOME/hadoop-${hadoopVersion}-core.jar:$HADOOP_HOME/contrib/vaidya/hadoop-${hadoopVersion}-vaidya.jar:$HADOOP_HOME/lib/commons-logging-1.0.4.jar:${CLASSPATH} org.apache.hadoop.vaidya.postexdiagnosis.PostExPerformanceDiagnoser $@
