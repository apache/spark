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

In addition to running on top of [Mesos](https://github.com/mesos/mesos), Spark also supports a standalone mode, consisting of one Spark master and several Spark worker processes. You can run the Spark standalone mode either locally (for testing) or on a cluster. If you wish to run on a cluster, we have provided [a set of deploy scripts](#cluster-launch-scripts) to launch a whole cluster.

# Getting Started

Compile Spark with `sbt package` as described in the [Getting Started Guide](index.html). You do not need to install Mesos on your machine if you are using the standalone mode.

# Starting a Cluster Manually

You can start a standalone master server by executing:

    ./run spark.deploy.master.Master

Once started, the master will print out a `spark://IP:PORT` URL for itself, which you can use to connect workers to it,
or pass as the "master" argument to `SparkContext` to connect a job to the cluster. You can also find this URL on
the master's web UI, which is [http://localhost:8080](http://localhost:8080) by default.

Similarly, you can start one or more workers and connect them to the master via:

    ./run spark.deploy.worker.Worker spark://IP:PORT

Once you have started a worker, look at the master's web UI ([http://localhost:8080](http://localhost:8080) by default).
You should see the new node listed there, along with its number of CPUs and memory (minus one gigabyte left for the OS).

Finally, the following configuration options can be passed to the master and worker: 

<table class="table">
  <tr><th style="width:21%">Argument</th><th>Meaning</th></tr>
  <tr>
    <td><code>-i IP</code>, <code>--ip IP</code></td>
    <td>IP address or DNS name to listen on</td>
  </tr>
  <tr>
    <td><code>-p PORT</code>, <code>--port PORT</code></td>
    <td>IP address or DNS name to listen on (default: 7077 for master, random for worker)</td>
  </tr>
  <tr>
    <td><code>--webui-port PORT</code></td>
    <td>Port for web UI (default: 8080 for master, 8081 for worker)</td>
  </tr>
  <tr>
    <td><code>-c CORES</code>, <code>--cores CORES</code></td>
    <td>Total CPU cores to allow Spark jobs to use on the machine (default: all available); only on worker</td>
  </tr>
  <tr>
    <td><code>-m MEM</code>, <code>--memory MEM</code></td>
    <td>Total amount of memory to allow Spark jobs to use on the machine, in a format like 1000M or 2G (default: your machine's total RAM minus 1 GB); only on worker</td>
  </tr>
  <tr>
    <td><code>-d DIR</code>, <code>--work-dir DIR</code></td>
    <td>Directory to use for scratch space and job output logs (default: SPARK_HOME/work); only on worker</td>
  </tr>
</table>


# Cluster Launch Scripts

To launch a Spark standalone cluster with the deploy scripts, you need to create a file called `conf/slaves` in your Spark directory, which should contain the hostnames of all the machines where you would like to start Spark workers, one per line. The master machine must be able to access each of the slave machines via password-less `ssh` (using a private key). For testing, you can just put `localhost` in this file.

Once you've set up this fine, you can launch or stop your cluster with the following shell scripts, based on Hadoop's deploy scripts, and available in `SPARK_HOME/bin`:

- `bin/start-master.sh` - Starts a master instance on the machine the script is executed on.
- `bin/start-slaves.sh` - Starts a slave instance on each machine specified in the `conf/slaves` file.
- `bin/start-all.sh` - Starts both a master and a number of slaves as described above.
- `bin/stop-master.sh` - Stops the master that was started via the `bin/start-master.sh` script.
- `bin/stop-slaves.sh` - Stops the slave instances that were started via `bin/start-slaves.sh`.
- `bin/stop-all.sh` - Stops both the master and the slaves as described above.

Note that these scripts must be executed on the machine you want to run the Spark master on, not your local machine.

You can optionally configure the cluster further by setting environment variables in `conf/spark-env.sh`. Create this file by starting with the `conf/spark-env.sh.template`, and _copy it to all your worker machines_ for the settings to take effect. The following settings are available:

<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>SPARK_MASTER_IP</code></td>
    <td>Bind the master to a specific IP address, for example a public one</td>
  </tr>
  <tr>
    <td><code>SPARK_MASTER_PORT</code></td>
    <td>Start the master on a different port (default: 7077)</td>
  </tr>
  <tr>
    <td><code>SPARK_MASTER_WEBUI_PORT</code></td>
    <td>Port for the master web UI (default: 8080)</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_PORT</code></td>
    <td>Start the Spark worker on a specific port (default: random)</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_DIR</code></td>
    <td>Directory to run jobs in, which will include both logs and scratch space (default: SPARK_HOME/work)</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_CORES</code></td>
    <td>Total number of cores to allow Spark jobs to use on the machine (default: all available cores)</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_MEMORY</code></td>
    <td>Total amount of memory to allow Spark jobs to use on the machine, e.g. 1000M, 2G (default: total memory minus 1 GB); note that each job's <i>individual</i> memory is configured using <code>SPARK_MEM</code></td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_WEBUI_PORT</code></td>
    <td>Port for the worker web UI (default: 8081)</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_MEMORY</code></td>
    <td>Memory to allocate to the Spark master and worker daemons themselves (default: 512m)</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_JAVA_OPTS</code></td>
    <td>JVM options for the Spark master and worker daemons themselves (default: none)</td>
  </tr>
</table>



# Connecting a Job to the Cluster

To run a job on the Spark cluster, simply pass the `spark://IP:PORT` URL of the master as to the [`SparkContext`
constructor](scala-programming-guide.html#initializing-spark).

To run an interactive Spark shell against the cluster, run the following command:

    MASTER=spark://IP:PORT ./spark-shell


# Job Scheduling

The standalone cluster mode currently only supports a simple FIFO scheduler across jobs.
However, to allow multiple concurrent jobs, you can control the maximum number of resources each Spark job will acquire.
By default, it will acquire *all* the cores in the cluster, which only makes sense if you run just a single
job at a time. You can cap the number of cores using `System.setProperty("spark.cores.max", "10")` (for example).
This value must be set *before* initializing your SparkContext.


# Monitoring and Logging

Spark's standalone mode offers a web-based user interface to monitor the cluster. The master and each worker has its own web UI that shows cluster and job statistics. By default you can access the web UI for the master at port 8080. The port can be changed either in the configuration file or via command-line options.

In addition, detailed log output for each job is also written to the work directory of each slave node (`SPARK_HOME/work` by default). You will see two files for each job, `stdout` and `stderr`, with all output it wrote to its console.


# Running Alongside Hadoop

You can run Spark alongside your existing Hadoop cluster by just launching it as a separate service on the machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI). Alternatively, you can set up a separate cluster for Spark, and still have it access HDFS over the network; this will be slower than disk-local access, but may not be a concern if you are still running in the same local area network (e.g. you place a few Spark machines on each rack that you have Hadoop on).

