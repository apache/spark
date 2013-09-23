---
layout: global
title: Spark Standalone Mode
---

In addition to running on the Mesos or YARN cluster managers, Spark also provides a simple standalone deploy mode. You can launch a standalone cluster either manually, by starting a master and workers by hand, or use our provided [launch scripts](#cluster-launch-scripts). It is also possible to run these daemons on a single machine for testing.

# Installing Spark Standalone to a Cluster

The easiest way to deploy Spark is by running the `./make-distribution.sh` script to create a binary distribution.
This distribution can be deployed to any machine with the Java runtime installed; there is no need to install Scala.

The recommended procedure is to deploy and start the master on one node first, get the master spark URL,
then modify `conf/spark-env.sh` in the `dist/` directory before deploying to all the other nodes.

# Starting a Cluster Manually

You can start a standalone master server by executing:

    ./bin/start-master.sh

Once started, the master will print out a `spark://HOST:PORT` URL for itself, which you can use to connect workers to it,
or pass as the "master" argument to `SparkContext`. You can also find this URL on
the master's web UI, which is [http://localhost:8080](http://localhost:8080) by default.

Similarly, you can start one or more workers and connect them to the master via:

    ./sbin/spark-class org.apache.spark.deploy.worker.Worker spark://IP:PORT

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
    <td>Port for service to listen on (default: 7077 for master, random for worker)</td>
  </tr>
  <tr>
    <td><code>--webui-port PORT</code></td>
    <td>Port for web UI (default: 8080 for master, 8081 for worker)</td>
  </tr>
  <tr>
    <td><code>-c CORES</code>, <code>--cores CORES</code></td>
    <td>Total CPU cores to allow Spark applicatons to use on the machine (default: all available); only on worker</td>
  </tr>
  <tr>
    <td><code>-m MEM</code>, <code>--memory MEM</code></td>
    <td>Total amount of memory to allow Spark applicatons to use on the machine, in a format like 1000M or 2G (default: your machine's total RAM minus 1 GB); only on worker</td>
  </tr>
  <tr>
    <td><code>-d DIR</code>, <code>--work-dir DIR</code></td>
    <td>Directory to use for scratch space and job output logs (default: SPARK_HOME/work); only on worker</td>
  </tr>
</table>


# Cluster Launch Scripts

To launch a Spark standalone cluster with the launch scripts, you need to create a file called `conf/slaves` in your Spark directory, which should contain the hostnames of all the machines where you would like to start Spark workers, one per line. The master machine must be able to access each of the slave machines via password-less `ssh` (using a private key). For testing, you can just put `localhost` in this file.

Once you've set up this file, you can launch or stop your cluster with the following shell scripts, based on Hadoop's deploy scripts, and available in `SPARK_HOME/bin`:

- `sbin/start-master.sh` - Starts a master instance on the machine the script is executed on.
- `sbin/start-slaves.sh` - Starts a slave instance on each machine specified in the `conf/slaves` file.
- `sbin/start-all.sh` - Starts both a master and a number of slaves as described above.
- `sbin/stop-master.sh` - Stops the master that was started via the `bin/start-master.sh` script.
- `sbin/stop-slaves.sh` - Stops the slave instances that were started via `bin/start-slaves.sh`.
- `sbin/stop-all.sh` - Stops both the master and the slaves as described above.

Note that these scripts must be executed on the machine you want to run the Spark master on, not your local machine.

You can optionally configure the cluster further by setting environment variables in `conf/spark-env.sh`. Create this file by starting with the `conf/spark-env.sh.template`, and _copy it to all your worker machines_ for the settings to take effect. The following settings are available:

<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>SPARK_MASTER_IP</code></td>
    <td>Bind the master to a specific IP address, for example a public one.</td>
  </tr>
  <tr>
    <td><code>SPARK_MASTER_PORT</code></td>
    <td>Start the master on a different port (default: 7077).</td>
  </tr>
  <tr>
    <td><code>SPARK_MASTER_WEBUI_PORT</code></td>
    <td>Port for the master web UI (default: 8080).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_PORT</code></td>
    <td>Start the Spark worker on a specific port (default: random).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_DIR</code></td>
    <td>Directory to run applications in, which will include both logs and scratch space (default: SPARK_HOME/work).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_CORES</code></td>
    <td>Total number of cores to allow Spark applications to use on the machine (default: all available cores).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_MEMORY</code></td>
    <td>Total amount of memory to allow Spark applications to use on the machine, e.g. <code>1000m</code>, <code>2g</code> (default: total memory minus 1 GB); note that each application's <i>individual</i> memory is configured using its <code>spark.executor.memory</code> property.</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_WEBUI_PORT</code></td>
    <td>Port for the worker web UI (default: 8081).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_INSTANCES</code></td>
    <td>
      Number of worker instances to run on each machine (default: 1). You can make this more than 1 if
      you have have very large machines and would like multiple Spark worker processes. If you do set
      this, make sure to also set <code>SPARK_WORKER_CORES</code> explicitly to limit the cores per worker,
      or else each worker will try to use all the cores.
    </td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_MEMORY</code></td>
    <td>Memory to allocate to the Spark master and worker daemons themselves (default: 512m).</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_JAVA_OPTS</code></td>
    <td>JVM options for the Spark master and worker daemons themselves (default: none).</td>
  </tr>
</table>

**Note:** The launch scripts do not currently support Windows. To run a Spark cluster on Windows, start the master and workers by hand.

# Connecting an Application to the Cluster

To run an application on the Spark cluster, simply pass the `spark://IP:PORT` URL of the master as to the [`SparkContext`
constructor](scala-programming-guide.html#initializing-spark).

To run an interactive Spark shell against the cluster, run the following command:

    MASTER=spark://IP:PORT ./spark-shell

Note that if you are running spark-shell from one of the spark cluster machines, the `spark-shell` script will
automatically set MASTER from the `SPARK_MASTER_IP` and `SPARK_MASTER_PORT` variables in `conf/spark-env.sh`.

You can also pass an option `-c <numCores>` to control the number of cores that spark-shell uses on the cluster.

# Resource Scheduling

The standalone cluster mode currently only supports a simple FIFO scheduler across applications.
However, to allow multiple concurrent users, you can control the maximum number of resources each
application will acquire.
By default, it will acquire *all* cores in the cluster, which only makes sense if you just run one
application at a time. You can cap the number of cores using
`System.setProperty("spark.cores.max", "10")` (for example).
This value must be set *before* initializing your SparkContext.


# Monitoring and Logging

Spark's standalone mode offers a web-based user interface to monitor the cluster. The master and each worker has its own web UI that shows cluster and job statistics. By default you can access the web UI for the master at port 8080. The port can be changed either in the configuration file or via command-line options.

In addition, detailed log output for each job is also written to the work directory of each slave node (`SPARK_HOME/work` by default). You will see two files for each job, `stdout` and `stderr`, with all output it wrote to its console.


# Running Alongside Hadoop

You can run Spark alongside your existing Hadoop cluster by just launching it as a separate service on the same machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI). Alternatively, you can set up a separate cluster for Spark, and still have it access HDFS over the network; this will be slower than disk-local access, but may not be a concern if you are still running in the same local area network (e.g. you place a few Spark machines on each rack that you have Hadoop on).

