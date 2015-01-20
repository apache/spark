#!/bin/bash


HOST_IP_ADDRESS=`sh getEth0Inet.sh`
export SPARK_MASTER_IP=${MASTER_IP_ADDRESS}
export SPARK_LOCAL_IP=${HOST_IP_ADDRESS}


if [[ "${EXECUTION_MODE}" = "master" ]]
then
    /root/spark1.3/sbin/start-master.sh
    /bin/bash
elif [[ "${EXECUTION_MODE}" = "worker" ]]
then
    /root/spark1.3/bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER_IP_ADDRESS}:${MASTER_PORT}
else
    /bin/bash
fi
