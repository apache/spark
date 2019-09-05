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


set -ex
export LOG=/dev/termination-log
echo "Asked to decommission" > ${LOG}
# Find the pid to signal
date | tee -a ${LOG}
WORKER_PID=$(ps axf | grep java |grep org.apache.spark.executor.CoarseGrainedExecutorBackend | grep -v grep)
echo "Using worker pid $WORKER_PID" | tee -a ${LOG}
kill -s SIGPWR ${WORKER_PID} | tee -a ${LOG}
# For now we expect this to timeout, since we don't start exiting the backend.
echo "Waiting for worker pid to exit" |tee -a ${LOG}
date | tee -a ${LOG}
# If the worker does exit stop blocking the cleanup.
timeout 60 tail --pid=${WORKER_PID} -f /dev/null | tee -a ${LOG}
date | tee -a ${LOG}
echo "Done" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Term log was:"
cat $LOG
