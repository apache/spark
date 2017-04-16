#!/usr/bin/env bash

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

# local folder with new configuration file
LOCAL_DIR=$1
# remote daemon host
HOST=$2
#remote dir points to the location of new config files
REMOTE_DIR=$3
# remote daemon HADOOP_CONF_DIR location
DAEMON_HADOOP_CONF_DIR=$4

if [ $# -ne 4 ]; then
  echo "Wrong number of parameters" >&2
  exit 2
fi

ret_value=0

echo The script makes a remote copy of existing ${DAEMON_HADOOP_CONF_DIR} to ${REMOTE_DIR}
echo and populates it with new configs prepared in $LOCAL_DIR

ssh ${HOST} cp -r ${DAEMON_HADOOP_CONF_DIR}/* ${REMOTE_DIR}
ret_value=$?

# make sure files are writeble
ssh ${HOST} chmod u+w ${REMOTE_DIR}/*

# copy new files over
scp -r ${LOCAL_DIR}/* ${HOST}:${REMOTE_DIR}

err_code=`echo $? + $ret_value | bc`
echo Copying of files from local to remote returned ${err_code}

