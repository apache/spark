#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Set SPARK_PUBLIC_DNS so slaves can be linked in master web UI
if [ "$SPARK_PUBLIC_DNS" = "" ]; then
    # If we appear to be running on EC2, use the public address by default:
    if [[ `hostname` == *ec2.internal ]]; then
        export SPARK_PUBLIC_DNS=`wget -q -O - http://instance-data.ec2.internal/latest/meta-data/public-hostname`
    fi
fi

"$bin"/spark-daemon.sh start spark.deploy.worker.Worker $1
