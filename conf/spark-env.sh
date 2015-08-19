#!/usr/bin/env bash

echoerr() { echo "$@" 1>&2; }
FWDIR="$(cd `dirname $0`/..; pwd)"


if [ "$(uname)" == "Darwin" ]; then
  case "$PYTHON_ENV" in
    'remote_development')
        echoerr "Sparkify: Connecting to chicago spark cluster ..."
        # Figure out the local IP to bind spark to for shell <-> master communication
        vpn_interface=tap0;
        get_ip_command="ifconfig $vpn_interface 2>&1 | grep 'inet' | awk '{print \$2}'"
        if ifconfig $vpn_interface > /dev/null 2>&1; then
          export SPARK_LOCAL_IP=`bash -c "$get_ip_command"`
        else
          echoerr "ERROR: could not find an VPN interface to connect to the Shopify Spark Cluster! Please connect your VPN client! See https://vault-unicorn.shopify.com/VPN---Servers ."
          exit 1
        fi

        export HADOOP_CONF_DIR=$FWDIR/conf/conf.cloudera.yarn
        ;;
    'test'|'development')
      export SPARK_LOCAL_IP=127.0.0.1
      ;;
  esac
fi

if which ipython > /dev/null; then
  export IPYTHON=1
fi
