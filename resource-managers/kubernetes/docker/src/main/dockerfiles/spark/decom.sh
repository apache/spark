#!/usr/bin/env bash

set -x
echo "hi"
echo "Decom adventures" > /dev/termination-log || echo "logging is hard"
WORKER_PID=$(ps axf | grep java |grep org.apache.spark.executor.CoarseGrainedExecutorBackend | grep -v grep  |  awk '{print "kill -9 " $1}')
kill -s SIGPWR ${WORKER_PID}
killall -s SIGPWR java
waitpid ${WORKER_PID}
sleep 30
