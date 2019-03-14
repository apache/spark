#!/usr/bin/env bash

set -ex
echo "hi"
echo "Decom adventures" > /dev/termination-log || echo "logging is hard"
WORKER_PID=$(ps axf | grep java |grep org.apache.spark | grep -v grep  |  awk '{print "kill -9 " $1}')
kill -s SIGPWR ${WORKER_PID}
waitpid ${WORKER_PID}
