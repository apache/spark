#!/usr/bin/env bash

# This file contains environment variables required to run Spark. Copy it as
# spark-env.sh and edit that to configure Spark for your site.
#
# The following variables can be set in this file:
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - MESOS_NATIVE_LIBRARY, to point to your libmesos.so if you use Mesos
# - SPARK_JAVA_OPTS, to set node-specific JVM options for Spark. Note that
#   we recommend setting app-wide options in the application's driver program.
#     Examples of node-specific options : -Dspark.local.dir, GC options
#     Examples of app-wide options : -Dspark.serializer
#
# If using the standalone deploy mode, you can also set variables for it here:
# - SPARK_MASTER_IP, to bind the master to a different IP address or hostname
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much memory to use (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT
# - SPARK_WORKER_INSTANCES, to set the number of worker processes per node
# - SPARK_WORKER_DIR, to set the working directory of worker processes
if [[ -z "$MASTER" ]]; then
  echo "Sparkify: Connecting to chicago spark cluster ..."
  export MASTER=spark://dn05.chi.shopify.com:7077
  export REMOTE_SPARK_HOME=/u/apps/spark/current
  export SPARK_JAVA_OPTS="-Dspark.cores.max=10"

  vpn_interface=tap0;
  if ifconfig $vpn_interface > /dev/null 2>&1; then
    export SPARK_LOCAL_IP=`ifconfig $vpn_interface 2>&1 | grep "inet" | awk '{print $2}'`
  else
    echo
    echo "WARNING: could not find an VPN interface to connect to the Shopify Spark Cluster! Is your VPN connected?"
    echo
  fi

  if which ipython > /dev/null; then
    export IPYTHON=1
  fi
fi
