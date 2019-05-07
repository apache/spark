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
echo "hi"
echo "Starting decom adventures" > ${LOG} || echo "logging is hard"
date | tee -a ${LOG}
# TODO(holden): Fix this PID extraction
WORKER_PID=$(ps axf | grep java |grep org.apache.spark.executor.CoarseGrainedExecutorBackend | grep -v grep)
echo "Using worker pid $WORKER_PID" | tee -a ${LOG}
kill -s SIGPWR ${WORKER_PID} | tee -a ${LOG}
echo "Waiting for worker pid to exit"
date
timeout 60 tail --pid=${WORKER_PID} -f /dev/null | tee -a ${LOG}
date
sleep 60 | tee -a ${LOG}
date
echo "Done" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Term log was:"
cat $LOG
