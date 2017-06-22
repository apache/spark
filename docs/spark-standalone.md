---
layout: global
title: Spark Standalone Mode
---

* This will become a table of contents (this text will be scraped).
{:toc}

In addition to running on the Mesos or YARN cluster managers, Spark also provides a simple standalone deploy mode. You can launch a standalone cluster either manually, by starting a master and workers by hand, or use our provided [launch scripts](#cluster-launch-scripts). It is also possible to run these daemons on a single machine for testing.

# Installing Spark Standalone to a Cluster

To install Spark Standalone mode, you simply place a compiled version of Spark on each node on the cluster. You can obtain pre-built versions of Spark with each release or [build it yourself](building-spark.html).

# Starting a Cluster Manually

You can start a standalone master server by executing:

    ./sbin/start-master.sh

Once started, the master will print out a `spark://HOST:PORT` URL for itself, which you can use to connect workers to it,
or pass as the "master" argument to `SparkContext`. You can also find this URL on
the master's web UI, which is [http://localhost:8080](http://localhost:8080) by default.

Similarly, you can start one or more workers and connect them to the master via:

    ./sbin/start-slave.sh <master-spark-URL>

Once you have started a worker, look at the master's web UI ([http://localhost:8080](http://localhost:8080) by default).
You should see the new node listed there, along with its number of CPUs and memory (minus one gigabyte left for the OS).

Finally, the following configuration options can be passed to the master and worker:

<table class="table">
  <tr><th style="width:21%">Argument</th><th>Meaning</th></tr>
  <tr>
    <td><code>-h HOST</code>, <code>--host HOST</code></td>
    <td>Hostname to listen on</td>
  </tr>
  <tr>
    <td><code>-i HOST</code>, <code>--ip HOST</code></td>
    <td>Hostname to listen on (deprecated, use -h or --host)</td>
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
    <td>Total CPU cores to allow Spark applications to use on the machine (default: all available); only on worker</td>
  </tr>
  <tr>
    <td><code>-m MEM</code>, <code>--memory MEM</code></td>
    <td>Total amount of memory to allow Spark applications to use on the machine, in a format like 1000M or 2G (default: your machine's total RAM minus 1 GB); only on worker</td>
  </tr>
  <tr>
    <td><code>-d DIR</code>, <code>--work-dir DIR</code></td>
    <td>Directory to use for scratch space and job output logs (default: SPARK_HOME/work); only on worker</td>
  </tr>
  <tr>
    <td><code>--properties-file FILE</code></td>
    <td>Path to a custom Spark properties file to load (default: conf/spark-defaults.conf)</td>
  </tr>
</table>


# Cluster Launch Scripts

To launch a Spark standalone cluster with the launch scripts, you should create a file called conf/slaves in your Spark directory,
which must contain the hostnames of all the machines where you intend to start Spark workers, one per line.
If conf/slaves does not exist, the launch scripts defaults to a single machine (localhost), which is useful for testing.
Note, the master machine accesses each of the worker machines via ssh. By default, ssh is run in parallel and requires password-less (using a private key) access to be setup.
If you do not have a password-less setup, you can set the environment variable SPARK_SSH_FOREGROUND and serially provide a password for each worker.


Once you've set up this file, you can launch or stop your cluster with the following shell scripts, based on Hadoop's deploy scripts, and available in `SPARK_HOME/sbin`:

- `sbin/start-master.sh` - Starts a master instance on the machine the script is executed on.
- `sbin/start-slaves.sh` - Starts a slave instance on each machine specified in the `conf/slaves` file.
- `sbin/start-slave.sh` - Starts a slave instance on the machine the script is executed on.
- `sbin/start-all.sh` - Starts both a master and a number of slaves as described above.
- `sbin/stop-master.sh` - Stops the master that was started via the `sbin/start-master.sh` script.
- `sbin/stop-slaves.sh` - Stops all slave instances on the machines specified in the `conf/slaves` file.
- `sbin/stop-all.sh` - Stops both the master and the slaves as described above.

Note that these scripts must be executed on the machine you want to run the Spark master on, not your local machine.

You can optionally configure the cluster further by setting environment variables in `conf/spark-env.sh`. Create this file by starting with the `conf/spark-env.sh.template`, and _copy it to all your worker machines_ for the settings to take effect. The following settings are available:

<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>SPARK_MASTER_HOST</code></td>
    <td>Bind the master to a specific hostname or IP address, for example a public one.</td>
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
    <td><code>SPARK_MASTER_OPTS</code></td>
    <td>Configuration properties that apply only to the master in the form "-Dx=y" (default: none). See below for a list of possible options.</td>
  </tr>
  <tr>
    <td><code>SPARK_LOCAL_DIRS</code></td>
    <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get
    stored on disk. This should be on a fast, local disk in your system. It can also be a
    comma-separated list of multiple directories on different disks.
    </td>
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
    <td><code>SPARK_WORKER_PORT</code></td>
    <td>Start the Spark worker on a specific port (default: random).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_WEBUI_PORT</code></td>
    <td>Port for the worker web UI (default: 8081).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_DIR</code></td>
    <td>Directory to run applications in, which will include both logs and scratch space (default: SPARK_HOME/work).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_OPTS</code></td>
    <td>Configuration properties that apply only to the worker in the form "-Dx=y" (default: none). See below for a list of possible options.</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_MEMORY</code></td>
    <td>Memory to allocate to the Spark master and worker daemons themselves (default: 1g).</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_JAVA_OPTS</code></td>
    <td>JVM options for the Spark master and worker daemons themselves in the form "-Dx=y" (default: none).</td>
  </tr>
  <tr>
    <td><code>SPARK_PUBLIC_DNS</code></td>
    <td>The public DNS name of the Spark master and workers (default: none).</td>
  </tr>
</table>

**Note:** The launch scripts do not currently support Windows. To run a Spark cluster on Windows, start the master and workers by hand.

SPARK_MASTER_OPTS supports the following system properties:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.deploy.retainedApplications</code></td>
  <td>200</td>
  <td>
    The maximum number of completed applications to display. Older applications will be dropped from the UI to maintain this limit.<br/>
  </td>
</tr>
<tr>
  <td><code>spark.deploy.retainedDrivers</code></td>
  <td>200</td>
  <td>
   The maximum number of completed drivers to display. Older drivers will be dropped from the UI to maintain this limit.<br/>
  </td>
</tr>
<tr>
  <td><code>spark.deploy.spreadOut</code></td>
  <td>true</td>
  <td>
    Whether the standalone cluster manager should spread applications out across nodes or try
    to consolidate them onto as few nodes as possible. Spreading out is usually better for
    data locality in HDFS, but consolidating is more efficient for compute-intensive workloads. <br/>
  </td>
</tr>
<tr>
  <td><code>spark.deploy.defaultCores</code></td>
  <td>(infinite)</td>
  <td>
    Default number of cores to give to applications in Spark's standalone mode if they don't
    set <code>spark.cores.max</code>. If not set, applications always get all available
    cores unless they configure <code>spark.cores.max</code> themselves.
    Set this lower on a shared cluster to prevent users from grabbing
    the whole cluster by default. <br/>
  </td>
</tr>
<tr>
  <td><code>spark.deploy.maxExecutorRetries</code></td>
  <td>10</td>
  <td>
    Limit on the maximum number of back-to-back executor failures that can occur before the
    standalone cluster manager removes a faulty application. An application will never be removed
    if it has any running executors. If an application experiences more than
    <code>spark.deploy.maxExecutorRetries</code> failures in a row, no executors
    successfully start running in between those failures, and the application has no running
    executors then the standalone cluster manager will remove the application and mark it as failed.
    To disable this automatic removal, set <code>spark.deploy.maxExecutorRetries</code> to
    <code>-1</code>.
    <br/>
  </td>
</tr>
<tr>
  <td><code>spark.worker.timeout</code></td>
  <td>60</td>
  <td>
    Number of seconds after which the standalone deploy master considers a worker lost if it
    receives no heartbeats.
  </td>
</tr>
</table>

SPARK_WORKER_OPTS supports the following system properties:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.worker.cleanup.enabled</code></td>
  <td>false</td>
  <td>
    Enable periodic cleanup of worker / application directories.  Note that this only affects standalone
    mode, as YARN works differently. Only the directories of stopped applications are cleaned up.
  </td>
</tr>
<tr>
  <td><code>spark.worker.cleanup.interval</code></td>
  <td>1800 (30 minutes)</td>
  <td>
    Controls the interval, in seconds, at which the worker cleans up old application work dirs
    on the local machine.
  </td>
</tr>
<tr>
  <td><code>spark.worker.cleanup.appDataTtl</code></td>
  <td>604800 (7 days, 7 * 24 * 3600)</td>
  <td>
    The number of seconds to retain application work directories on each worker.  This is a Time To Live
    and should depend on the amount of available disk space you have.  Application logs and jars are
    downloaded to each application work dir.  Over time, the work dirs can quickly fill up disk space,
    especially if you run jobs very frequently.
  </td>
</tr>
<tr>
  <td><code>spark.worker.ui.compressedLogFileLengthCacheSize</code></td>
  <td>100</td>
  <td>
    For compressed log files, the uncompressed file can only be computed by uncompressing the files.
    Spark caches the uncompressed file size of compressed log files. This property controls the cache
    size.
  </td>
</tr>
</table>

# Connecting an Application to the Cluster

To run an application on the Spark cluster, simply pass the `spark://IP:PORT` URL of the master as to the [`SparkContext`
constructor](programming-guide.html#initializing-spark).

To run an interactive Spark shell against the cluster, run the following command:

    ./bin/spark-shell --master spark://IP:PORT

You can also pass an option `--total-executor-cores <numCores>` to control the number of cores that spark-shell uses on the cluster.

# Launching Spark Applications

The [`spark-submit` script](submitting-applications.html) provides the most straightforward way to
submit a compiled Spark application to the cluster. For standalone clusters, Spark currently
supports two deploy modes. In `client` mode, the driver is launched in the same process as the
client that submits the application. In `cluster` mode, however, the driver is launched from one
of the Worker processes inside the cluster, and the client process exits as soon as it fulfills
its responsibility of submitting the application without waiting for the application to finish.

If your application is launched through Spark submit, then the application jar is automatically
distributed to all worker nodes. For any additional jars that your application depends on, you
should specify them through the `--jars` flag using comma as a delimiter (e.g. `--jars jar1,jar2`).
To control the application's configuration or execution environment, see
[Spark Configuration](configuration.html).

Additionally, standalone `cluster` mode supports restarting your application automatically if it
exited with non-zero exit code. To use this feature, you may pass in the `--supervise` flag to
`spark-submit` when launching your application. Then, if you wish to kill an application that is
failing repeatedly, you may do so through:

    ./bin/spark-class org.apache.spark.deploy.Client kill <master url> <driver ID>

You can find the driver ID through the standalone Master web UI at `http://<master url>:8080`.

# Resource Scheduling

The standalone cluster mode currently only supports a simple FIFO scheduler across applications.
However, to allow multiple concurrent users, you can control the maximum number of resources each
application will use.
By default, it will acquire *all* cores in the cluster, which only makes sense if you just run one
application at a time. You can cap the number of cores by setting `spark.cores.max` in your
[SparkConf](configuration.html#spark-properties). For example:

{% highlight scala %}
val conf = new SparkConf()
  .setMaster(...)
  .setAppName(...)
  .set("spark.cores.max", "10")
val sc = new SparkContext(conf)
{% endhighlight %}

In addition, you can configure `spark.deploy.defaultCores` on the cluster master process to change the
default for applications that don't set `spark.cores.max` to something less than infinite.
Do this by adding the following to `conf/spark-env.sh`:

{% highlight bash %}
export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=<value>"
{% endhighlight %}

This is useful on shared clusters where users might not have configured a maximum number of cores
individually.

# Monitoring and Logging

Spark's standalone mode offers a web-based user interface to monitor the cluster. The master and each worker has its own web UI that shows cluster and job statistics. By default you can access the web UI for the master at port 8080. The port can be changed either in the configuration file or via command-line options.

In addition, detailed log output for each job is also written to the work directory of each slave node (`SPARK_HOME/work` by default). You will see two files for each job, `stdout` and `stderr`, with all output it wrote to its console.


# Running Alongside Hadoop

You can run Spark alongside your existing Hadoop cluster by just launching it as a separate service on the same machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI). Alternatively, you can set up a separate cluster for Spark, and still have it access HDFS over the network; this will be slower than disk-local access, but may not be a concern if you are still running in the same local area network (e.g. you place a few Spark machines on each rack that you have Hadoop on).


# Configuring Ports for Network Security

Spark makes heavy use of the network, and some environments have strict requirements for using
tight firewall settings. For a complete list of ports to configure, see the
[security page](security.html#configuring-ports-for-network-security).

# High Availability

By default, standalone scheduling clusters are resilient to Worker failures (insofar as Spark itself is resilient to losing work by moving it to other workers). However, the scheduler uses a Master to make scheduling decisions, and this (by default) creates a single point of failure: if the Master crashes, no new applications can be created. In order to circumvent this, we have two high availability schemes, detailed below.

## Standby Masters with ZooKeeper

**Overview**

Utilizing ZooKeeper to provide leader election and some state storage, you can launch multiple Masters in your cluster connected to the same ZooKeeper instance. One will be elected "leader" and the others will remain in standby mode. If the current leader dies, another Master will be elected, recover the old Master's state, and then resume scheduling. The entire recovery process (from the time the first leader goes down) should take between 1 and 2 minutes. Note that this delay only affects scheduling _new_ applications -- applications that were already running during Master failover are unaffected.

Learn more about getting started with ZooKeeper [here](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html).

**Configuration**

In order to enable this recovery mode, you can set SPARK_DAEMON_JAVA_OPTS in spark-env by configuring `spark.deploy.recoveryMode` and related spark.deploy.zookeeper.* configurations.
For more information about these configurations please refer to the [configuration doc](configuration.html#deploy)

Possible gotcha: If you have multiple Masters in your cluster but fail to correctly configure the Masters to use ZooKeeper, the Masters will fail to discover each other and think they're all leaders. This will not lead to a healthy cluster state (as all Masters will schedule independently).

**Details**

After you have a ZooKeeper cluster set up, enabling high availability is straightforward. Simply start multiple Master processes on different nodes with the same ZooKeeper configuration (ZooKeeper URL and directory). Masters can be added and removed at any time.

In order to schedule new applications or add Workers to the cluster, they need to know the IP address of the current leader. This can be accomplished by simply passing in a list of Masters where you used to pass in a single one. For example, you might start your SparkContext pointing to ``spark://host1:port1,host2:port2``. This would cause your SparkContext to try registering with both Masters -- if ``host1`` goes down, this configuration would still be correct as we'd find the new leader, ``host2``.

There's an important distinction to be made between "registering with a Master" and normal operation. When starting up, an application or Worker needs to be able to find and register with the current lead Master. Once it successfully registers, though, it is "in the system" (i.e., stored in ZooKeeper). If failover occurs, the new leader will contact all previously registered applications and Workers to inform them of the change in leadership, so they need not even have known of the existence of the new Master at startup.

Due to this property, new Masters can be created at any time, and the only thing you need to worry about is that _new_ applications and Workers can find it to register with in case it becomes the leader. Once registered, you're taken care of.

## Single-Node Recovery with Local File System

**Overview**

ZooKeeper is the best way to go for production-level high availability, but if you just want to be able to restart the Master if it goes down, FILESYSTEM mode can take care of it. When applications and Workers register, they have enough state written to the provided directory so that they can be recovered upon a restart of the Master process.

**Configuration**

In order to enable this recovery mode, you can set SPARK_DAEMON_JAVA_OPTS in spark-env using this configuration:

<table class="table">
  <tr><th style="width:21%">System property</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.deploy.recoveryMode</code></td>
    <td>Set to FILESYSTEM to enable single-node recovery mode (default: NONE).</td>
  </tr>
  <tr>
    <td><code>spark.deploy.recoveryDirectory</code></td>
    <td>The directory in which Spark will store recovery state, accessible from the Master's perspective.</td>
  </tr>
</table>

**Details**

* This solution can be used in tandem with a process monitor/manager like [monit](http://mmonit.com/monit/), or just to enable manual recovery via restart.
* While filesystem recovery seems straightforwardly better than not doing any recovery at all, this mode may be suboptimal for certain development or experimental purposes. In particular, killing a master via stop-master.sh does not clean up its recovery state, so whenever you start a new Master, it will enter recovery mode. This could increase the startup time by up to 1 minute if it needs to wait for all previously-registered Workers/clients to timeout.
* While it's not officially supported, you could mount an NFS directory as the recovery directory. If the original Master node dies completely, you could then start a Master on a different node, which would correctly recover all previously registered Workers/applications (equivalent to ZooKeeper recovery). Future applications will have to be able to find the new Master, however, in order to register.
