#!/bin/bash

# These variables are automatically filled in by the mesos-ec2 script.
export MESOS_MASTERS="{{master_list}}"
export MESOS_SLAVES="{{slave_list}}"
export MESOS_ZOO_LIST="{{zoo_list}}"
export MESOS_HDFS_DATA_DIRS="{{hdfs_data_dirs}}"
export MESOS_MAPRED_LOCAL_DIRS="{{mapred_local_dirs}}"
