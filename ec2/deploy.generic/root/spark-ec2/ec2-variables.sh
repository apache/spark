#!/usr/bin/env bash

# These variables are automatically filled in by the spark-ec2 script.
export MASTERS="{{master_list}}"
export SLAVES="{{slave_list}}"
export HDFS_DATA_DIRS="{{hdfs_data_dirs}}"
export MAPRED_LOCAL_DIRS="{{mapred_local_dirs}}"
export SPARK_LOCAL_DIRS="{{spark_local_dirs}}"
export MODULES="{{modules}}"
export SPARK_VERSION="{{spark_version}}"
export SHARK_VERSION="{{shark_version}}"
export HADOOP_MAJOR_VERSION="{{hadoop_major_version}}"
export SWAP_MB="{{swap}}"
