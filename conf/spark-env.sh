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
echoerr() { echo "$@" 1>&2; }
FWDIR="$(cd `dirname $0`/..; pwd)"

if [ -z "$SPARK_ON_MESOS" ]; then
  if [[ -z "$MASTER" ]]; then
    echoerr "Sparkify: Connecting to chicago spark cluster ..."
    export MASTER=spark://dn05.chi.shopify.com:7077

    # Figure out the local IP to bind spark to for shell <-> master communication
    vpn_interface=tap0;
    get_ip_command="ifconfig $vpn_interface 2>&1 | grep 'inet' | awk '{print \$2}'"
    if ifconfig $vpn_interface > /dev/null 2>&1; then
      export SPARK_LOCAL_IP=`bash -c "$get_ip_command"`
    else
      if [[ -e /Applications/Viscosity.app ]]; then
        echoerr "WARNING: could not find an VPN interface to connect to the Shopify Spark Cluster! Trying to autoconnect..."
        osascript -e "
          tell application \"Viscosity\"
            connect first connection
            set total to 0
            repeat while (first connection's state is not equal to \"Connected\") or total is greater than 15
              delay 0.5
              set total to total + 0.5
            end repeat
            return \"Connected!\"
          end tell
        "
        export SPARK_LOCAL_IP=`$get_ip_command`
      else
        echoerr "ERROR: could not find an VPN interface to connect to the Shopify Spark Cluster! Please connect your VPN client! See https://vault-unicorn.shopify.com/VPN---Servers ."
        exit 1
      fi
    fi

    if which ipython > /dev/null; then
      export IPYTHON=1
    fi
  fi

  if [[ $MASTER == 'local' ]]; then
    export SPARK_LOCAL_IP=127.0.0.1
  fi
else
  export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so
  export MASTER=zk://kafka01.chi.shopify.com:2181/mesos
  export SPARK_EXECUTOR_URI=http://pack.chi.shopify.com/packages/Shopify/spark/latest.tar.gz
fi
