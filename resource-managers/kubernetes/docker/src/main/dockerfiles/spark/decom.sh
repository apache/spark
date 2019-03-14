#!/usr/bin/env bash

set -x
LOG=/dev/termination-log
echo "hi"
echo "Starting decom adventures" > ${LOG} || echo "logging is hard"
date | tee -a ${LOG}
WORKER_PID=$(ps axf | grep java |grep org.apache.spark.executor.CoarseGrainedExecutorBackend | grep -v grep  |  awk '{print "kill -9 " $1}')
echo "Using worker pid $WORKER_PID" | tee -a ${LOG}
kill -s SIGPWR ${WORKER_PID} | tee -a ${LOG}
killall -s SIGPWR java  | tee -a ${LOG}
waitpid ${WORKER_PID} | tee -a ${LOG}
sleep 30 | tee -a ${LOG}
echo "Done" | tee -a ${LOG}
date | tee -a ${LOG}
