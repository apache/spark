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


set +e
set -x
echo "Asked to decommission"
# Find the pid to signal
date | tee -a ${LOG}
WORKER_PID=$(ps -o pid,cmd -C java |grep Executor \
	       | tail -n 1| awk '{ sub(/^[ \t]+/, ""); print }' \
	       | cut -f 1 -d " ")
echo "Using worker pid $WORKER_PID"
kill -s SIGPWR ${WORKER_PID}
# If the worker does exit stop blocking K8s cleanup. Note this is a "soft"
# block since the pod it's self will have a maximum decommissioning time which will
# overload this.
echo "Waiting for worker pid to exit"
tail --pid=${WORKER_PID} -f /dev/null
sleep 1
date
echo "Done"
date
sleep 1
