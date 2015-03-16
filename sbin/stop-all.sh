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

# Stop all spark daemons.
# Run this on the master node.


sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

# Load the Spark configuration
. "$sbin/spark-config.sh"

# Stop the slaves, then the master
"$sbin"/stop-slaves.sh
"$sbin"/stop-master.sh

if [ "$1" == "--wait" ]
then
  printf "Waiting for workers to shut down..."
  while true
  do
    running=`$sbin/slaves.sh ps -ef | grep -v grep | grep deploy.worker.Worker`
    if [ -z "$running" ]
    then
      printf "\nAll workers successfully shut down.\n"
      break
    else
      printf "."
      sleep 10
    fi
  done
fi
