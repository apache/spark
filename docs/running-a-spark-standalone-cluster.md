---
layout: global
title: Running a Spark Standalone Cluster
---
**Note**: Standalone mode is only supported on the dev branch right now.

## Before you Start

- Download and compile Spark as described in the [README](https://github.com/mesos/spark/wiki).
- If you wish to use the EC2 Scripts (described below), you must have an Amazon AWS Account and an EC2 key pair.

## Configuration

In order to run a Spark cluster there are two main points of configuration, the `conf/spark-env.sh` file, and the `conf/slaves` file. the `conf/spark-env.sh` file lets you specify settings for the master and slave instances, such as memory, or port numbers to bind on. The file itself is well documented and all configuration variables are explained.

The `conf/slaves` file contains a list of all machines where you would like to start a Spark slave (worker) instance when using then scripts below. The master machine must be able to access each of the slave machines via ssh. For testing purposes, you can have a single `localhost` entry in the slaves file.

## Scripts

In order to make starting master and slave instances easier, we have provided Hadoop-style shell scripts. The scripts can be found in the `bin` directory. A quick overview:

- `bin/start_master` - Starts a master instance on the machine the script is executed on.
- `bin/start_slaves` - Starts a slave instance on each machine specified in the `conf/slaves` file.
- `bin/start_all` - Starts both a master and a number of slaves as described above.
- `bin/stop_master` - Stops the master that was started via the `bin/start_master` script.
- `bin/stop_slaves` - Stops the slave intances that were started via the `bin/start_slaves` script.
- `bin/stop_all` - Stops both the master and the slaves as described above.

Note that the scripts must be executed on the machine you want to start the Spark master on, not your local machine. By default, both the master and slaves write their log files to the `logs` in the Spark home directory.

## Connecting to the Cluster

To run a job against a standalone cluster, pass a URI of the form `spark://<SPARK_MASTER_IP>:<SPARK_MASTER_PORT>` as the Master's URI when setting up a Spark Context (or using the interactive shell).

## Spark Web Interface

In order to allow for easy debugging of your cluster we have provided a simple web interface that described the current state of the cluster and jobs. By default you can access it via `http://<SPARK_MASTER_IP>:8080`, but you can specify a different port number in`conf/spark_env.sh`.

## EC2 Scripts

To save you from needing to set up a cluster of Spark machines yourself, we provide a set of scripts that launch Amazon EC2 instances with a preinstalled Spark distribution. These scripts are identical to the [EC2 Mesos Scripts](https://github.com/mesos/spark/wiki/EC2-Scripts), except that you need to execute `ec2/spark-ec2` with the following additional parameters: `--cluster-type standalone -a standalone --user ec2-user`. Note that the Spark version on these machines may not reflect the latest changes, so it may be a good idea to ssh into the machines and merge the latest version from github.
