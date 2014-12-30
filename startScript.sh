#!/bin/bash


if [[ "${EXECUTION_MODE}" = "master" ]]
then
    ./sbin/start-master.sh
elif [[ "${EXECUTION_MODE}" = "worker" ]]
then
    ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://master_ip:${MASTER_PORT}
else
    /bin/bash
fi
