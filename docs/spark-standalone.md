---
layout: global
title: Spark Standalone Mode
---

{% comment %}
TODO(andyk):
  - Add a table of contents
  - Move configuration towards the end so that it doesn't come first
  - Say the scripts will guess the resource amounts (i.e. # cores) automatically
{% endcomment %}

In addition to running on top of [Mesos](https://github.com/mesos/mesos), Spark also supports a standalone mode, consisting of one Spark master and several Spark worker processes. You can run the Spark standalone mode either locally or on a cluster. If you wish to run an Spark Amazon EC2 cluster using standalone mode we have provided [a set of scripts](ec2-scripts.html) that make it easy to do so.

## Getting Started

Download and compile Spark as described in the [Getting Started Guide](index.html). You do not need to install mesos on your machine if you are using the standalone mode.

## Standalone Mode Configuration

The `conf/spark_env.sh` file contains several configuration parameters for the standalone mode. Here is a quick overview:

- SPARK\_MASTER\_IP - Use this to bind the master to a particular ip address, for example a public one. (Default: local ip address)
- SPARK\_MASTER\_PORT - Start the spark master on a different port (Default: 7077)
- SPARK\_MASTER\_WEBUI\_POR -  Specify a different port for the Master WebUI (Default: 8080)
- SPARK\_WORKER\_PORT - Start the spark worker on a specific port (Default: random)
- SPARK\_WORKER\_CORES - Specify the number of cores to use (Default: all available cores)
- SPARK\_WORKER\_MEMORY - Specify how much memory to use, e.g. 1000M, 2G (Default: MAX(Available - 1024MB, 512MB))
- SPARK\_WORKER\_WEBUI\_PORT - Specify a different port for the Worker WebUI (Default: 8081)

## Starting the standalone Master

You can start a standalone master server by executing:

    ./run spark.deploy.master.Master

The program takes additional arguments that will overwrite the configuration values:

    -i IP, --ip IP         IP address or DNS name to listen on
    -p PORT, --port PORT   Port to listen on (default: 7077)
    --webui-port PORT      Port for web UI (default: 8080)

The master process should print out the Master's URL of the form `spark://IP:PORT` which you can use to create a `SparkContext` in your applications. 

## Starting standalone Workers

Similar to the master, you can start one or more standalone workers via:

`./run spark.deploy.worker.Worker spark://IP:PORT`

The following options can be passed to the worker: 

    -c CORES, --cores CORES  Number of cores to use
    -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)
    -i IP, --ip IP           IP address or DNS name to listen on
    -p PORT, --port PORT     Port to listen on (default: random)
    --webui-port PORT        Port for web UI (default: 8081)

## Debugging a standalone cluster

Spark offers a web-based user interface in the standalone mode. The master and each worker has its own WebUI that shows cluster and job statistics. By default you can access the WebUI for the master at port 8080. The port can be changed either in the configuration file or via command-line options.

Detailed log output for the jobs is written to the `work` drectory by default.

## Running on a Cluster

In order to run a Spark standalone cluster there are two main points of configuration, the `conf/spark-env.sh` file (described above), and the `conf/slaves` file. the `conf/spark-env.sh` file lets you specify global settings for the master and slave instances, such as memory, or port numbers to bind to. We are assuming that all your machines share the same configuration parameters.

The `conf/slaves` file contains a list of all machines where you would like to start a Spark slave (worker) instance when using the scripts below. The master machine must be able to access each of the slave machines via ssh. For testing purposes, you can have a single `localhost` entry in the slaves file.

In order to make starting master and slave instances easier, we have provided Hadoop-style shell scripts. The scripts can be found in the `bin` directory. A quick overview:

- `bin/start_master` - Starts a master instance on the machine the script is executed on.
- `bin/start_slaves` - Starts a slave instance on each machine specified in the `conf/slaves` file.
- `bin/start_all` - Starts both a master and a number of slaves as described above.
- `bin/stop_master` - Stops the master that was started via the `bin/start_master` script.
- `bin/stop_slaves` - Stops the slave intances that were started via the `bin/start_slaves` script.
- `bin/stop_all` - Stops both the master and the slaves as described above.

Note that the scripts must be executed on the machine you want to start the Spark master on, not your local machine.

{% comment %}
## EC2 Scripts

To save you from needing to set up a cluster of Spark machines yourself, we provide a set of scripts that launch Amazon EC2 instances with a preinstalled Spark distribution. These scripts are identical to the [EC2 Mesos Scripts](https://github.com/mesos/spark/wiki/EC2-Scripts), except that you need to execute `ec2/spark-ec2` with the following additional parameters: `--cluster-type standalone -a standalone`. Note that the Spark version on these machines may not reflect the latest changes, so it may be a good idea to ssh into the machines and merge the latest version from github.
{% endcomment %}
