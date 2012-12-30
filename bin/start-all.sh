#!/usr/bin/env bash

# Start all spark daemons.
# Starts the master on this node.
# Starts a worker on each node specified in conf/slaves

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Load the Spark configuration
. "$bin/spark-config.sh"

# Start Master
"$bin"/start-master.sh

# Start Workers
"$bin"/start-slaves.sh
