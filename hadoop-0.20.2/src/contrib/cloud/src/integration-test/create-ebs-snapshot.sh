#!/usr/bin/env bash
#
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
#
# This script tests the "hadoop-ec2 create-formatted-snapshot" command.
# The snapshot is deleted immediately afterwards.
#
# Example usage:
# ./create-ebs-snapshot.sh
#

set -e
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

WORKSPACE=${WORKSPACE:-`pwd`}
CONFIG_DIR=${CONFIG_DIR:-$WORKSPACE/.hadoop-cloud}
CLUSTER=${CLUSTER:-hadoop-cloud-$USER-test-cluster}
AVAILABILITY_ZONE=${AVAILABILITY_ZONE:-us-east-1c}
KEY_NAME=${KEY_NAME:-$USER}
HADOOP_CLOUD_HOME=${HADOOP_CLOUD_HOME:-$bin/../py}
HADOOP_CLOUD_PROVIDER=${HADOOP_CLOUD_PROVIDER:-ec2}
SSH_OPTIONS=${SSH_OPTIONS:-"-i ~/.$HADOOP_CLOUD_PROVIDER/id_rsa-$KEY_NAME \
  -o StrictHostKeyChecking=no"}

HADOOP_CLOUD_SCRIPT=$HADOOP_CLOUD_HOME/hadoop-$HADOOP_CLOUD_PROVIDER

$HADOOP_CLOUD_SCRIPT create-formatted-snapshot --config-dir=$CONFIG_DIR \
  --key-name=$KEY_NAME --availability-zone=$AVAILABILITY_ZONE \
  --ssh-options="$SSH_OPTIONS" \
  $CLUSTER 1 > out.tmp

snapshot_id=`grep 'Created snapshot' out.tmp | awk '{print $3}'`

ec2-delete-snapshot $snapshot_id

rm -f out.tmp
