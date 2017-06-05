#!/usr/bin/env bash

# make sure all the nodes are up
# vagrant up 1>/dev/null 2>&1 

source vagrant_nodes.sh

sshpass -p "$VAGRANT_SPARK_PASSWORD" ssh-copy-id "$VAGRANT_SPARK_USER"@"$VAGRANT_SPARK_MASTER" 1>/dev/null 2>&1 
sshpass -p "$VAGRANT_SPARK_PASSWORD" ssh-copy-id "$VAGRANT_SPARK_USER"@"$VAGRANT_SPARK_WORKER_1" 1>/dev/null 2>&1 
sshpass -p "$VAGRANT_SPARK_PASSWORD" ssh-copy-id "$VAGRANT_SPARK_USER"@"$VAGRANT_SPARK_WORKER_2" 1>/dev/null 2>&1 

