---
layout: global
title: Spark Standalone Mode
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scraped).
{:toc}

In addition to running on the YARN cluster manager, Spark also provides a simple standalone deploy mode. You can launch a standalone cluster either manually, by starting a master and workers by hand, or use our provided [launch scripts](#cluster-launch-scripts). It is also possible to run these daemons on a single machine for testing.

# Security

Security features like authentication are not enabled by default. When deploying a cluster that is open to the internet
or an untrusted network, it's important to secure access to the cluster to prevent unauthorized applications
from running on the cluster.
Please see [Spark Security](security.html) and the specific security sections in this doc before running Spark.

# Installing Spark Standalone to a Cluster

To install Spark Standalone mode, you simply place a compiled version of Spark on each node on the cluster. You can obtain pre-built versions of Spark with each release or [build it yourself](building-spark.html).

# Starting a Cluster Manually

You can start a standalone master server by executing:

    ./sbin/start-master.sh

Once started, the master will print out a `spark://HOST:PORT` URL for itself, which you can use to connect workers to it,
or pass as the "master" argument to `SparkContext`. You can also find this URL on
the master's web UI, which is [http://localhost:8080](http://localhost:8080) by default.

Similarly, you can start one or more workers and connect them to the master via:

    ./sbin/start-worker.sh <master-spark-URL>

Once you have started a worker, look at the master's web UI ([http://localhost:8080](http://localhost:8080) by default).
You should see the new node listed there, along with its number of CPUs and memory (minus one gigabyte left for the OS).

Finally, the following configuration options can be passed to the master and worker:

<table>
  <thead><tr><th style="width:21%">Argument</th><th>Meaning</th></tr></thead>
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
    <td>Total amount of memory to allow Spark applications to use on the machine, in a format like 1000M or 2G (default: your machine's total RAM minus 1 GiB); only on worker</td>
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

To launch a Spark standalone cluster with the launch scripts, you should create a file called conf/workers in your Spark directory,
which must contain the hostnames of all the machines where you intend to start Spark workers, one per line.
If conf/workers does not exist, the launch scripts defaults to a single machine (localhost), which is useful for testing.
Note, the master machine accesses each of the worker machines via ssh. By default, ssh is run in parallel and requires password-less (using a private key) access to be setup.
If you do not have a password-less setup, you can set the environment variable SPARK_SSH_FOREGROUND and serially provide a password for each worker.


Once you've set up this file, you can launch or stop your cluster with the following shell scripts, based on Hadoop's deploy scripts, and available in `SPARK_HOME/sbin`:

- `sbin/start-master.sh` - Starts a master instance on the machine the script is executed on.
- `sbin/start-workers.sh` - Starts a worker instance on each machine specified in the `conf/workers` file.
- `sbin/start-worker.sh` - Starts a worker instance on the machine the script is executed on.
- `sbin/start-connect-server.sh` - Starts a Spark Connect server on the machine the script is executed on.
- `sbin/start-all.sh` - Starts both a master and a number of workers as described above.
- `sbin/stop-master.sh` - Stops the master that was started via the `sbin/start-master.sh` script.
- `sbin/stop-worker.sh` - Stops all worker instances on the machine the script is executed on.
- `sbin/stop-workers.sh` - Stops all worker instances on the machines specified in the `conf/workers` file.
- `sbin/stop-connect-server.sh` - Stops all Spark Connect server instances on the machine the script is executed on.
- `sbin/stop-all.sh` - Stops both the master and the workers as described above.

Note that these scripts must be executed on the machine you want to run the Spark master on, not your local machine.

You can optionally configure the cluster further by setting environment variables in `conf/spark-env.sh`. Create this file by starting with the `conf/spark-env.sh.template`, and _copy it to all your worker machines_ for the settings to take effect. The following settings are available:

<table>
  <thead><tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr></thead>
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
    <td><code>SPARK_LOG_DIR</code></td>
    <td>Where log files are stored. (default: SPARK_HOME/logs).</td>
  </tr>
  <tr>
    <td><code>SPARK_LOG_MAX_FILES</code></td>
    <td>The maximum number of log files (default: 5).</td>
  </tr>
  <tr>
    <td><code>SPARK_PID_DIR</code></td>
    <td>Where pid files are stored. (default: /tmp).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_CORES</code></td>
    <td>Total number of cores to allow Spark applications to use on the machine (default: all available cores).</td>
  </tr>
  <tr>
    <td><code>SPARK_WORKER_MEMORY</code></td>
    <td>Total amount of memory to allow Spark applications to use on the machine, e.g. <code>1000m</code>, <code>2g</code> (default: total memory minus 1 GiB); note that each application's <i>individual</i> memory is configured using its <code>spark.executor.memory</code> property.</td>
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
    <td><code>SPARK_DAEMON_CLASSPATH</code></td>
    <td>Classpath for the Spark master and worker daemons themselves (default: none).</td>
  </tr>
  <tr>
    <td><code>SPARK_PUBLIC_DNS</code></td>
    <td>The public DNS name of the Spark master and workers (default: none).</td>
  </tr>
</table>

**Note:** The launch scripts do not currently support Windows. To run a Spark cluster on Windows, start the master and workers by hand.

SPARK_MASTER_OPTS supports the following system properties:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.master.ui.port</code></td>
  <td><code>8080</code></td>
  <td>
    Specifies the port number of the Master Web UI endpoint.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.master.ui.title</code></td>
  <td>(None)</td>
  <td>
    Specifies the title of the Master UI page. If unset, <code>Spark Master at 'master url'</code>
    is used by default.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.master.ui.decommission.allow.mode</code></td>
  <td><code>LOCAL</code></td>
  <td>
    Specifies the behavior of the Master Web UI's /workers/kill endpoint. Possible choices
    are: <code>LOCAL</code> means allow this endpoint from IP's that are local to the machine running
    the Master, <code>DENY</code> means to completely disable this endpoint, <code>ALLOW</code> means to allow
    calling this endpoint from any IP.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.master.ui.historyServerUrl</code></td>
  <td>(None)</td>
  <td>
    The URL where Spark history server is running. Please note that this assumes
    that all Spark jobs share the same event log location where the history server accesses.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.master.rest.enabled</code></td>
  <td><code>false</code></td>
  <td>
    Whether to use the Master REST API endpoint or not.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.master.rest.host</code></td>
  <td>(None)</td>
  <td>
    Specifies the host of the Master REST API endpoint.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.master.rest.port</code></td>
  <td><code>6066</code></td>
  <td>
    Specifies the port number of the Master REST API endpoint.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.master.useAppNameAsAppId.enabled</code></td>
  <td><code>false</code></td>
  <td>
    (Experimental) If true, Spark master uses the user-provided appName for appId.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.deploy.retainedApplications</code></td>
  <td>200</td>
  <td>
    The maximum number of completed applications to display. Older applications will be dropped from the UI to maintain this limit.<br/>
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.deploy.retainedDrivers</code></td>
  <td>200</td>
  <td>
   The maximum number of completed drivers to display. Older drivers will be dropped from the UI to maintain this limit.<br/>
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.deploy.spreadOutDrivers</code></td>
  <td>true</td>
  <td>
    Whether the standalone cluster manager should spread drivers out across nodes or try
    to consolidate them onto as few nodes as possible. Spreading out is usually better for
    data locality in HDFS, but consolidating is more efficient for compute-intensive workloads.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.deploy.spreadOutApps</code></td>
  <td>true</td>
  <td>
    Whether the standalone cluster manager should spread applications out across nodes or try
    to consolidate them onto as few nodes as possible. Spreading out is usually better for
    data locality in HDFS, but consolidating is more efficient for compute-intensive workloads. <br/>
  </td>
  <td>0.6.1</td>
</tr>
<tr>
  <td><code>spark.deploy.defaultCores</code></td>
  <td>Int.MaxValue</td>
  <td>
    Default number of cores to give to applications in Spark's standalone mode if they don't
    set <code>spark.cores.max</code>. If not set, applications always get all available
    cores unless they configure <code>spark.cores.max</code> themselves.
    Set this lower on a shared cluster to prevent users from grabbing
    the whole cluster by default. <br/>
  </td>
  <td>0.9.0</td>
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
  <td>1.6.3</td>
</tr>
<tr>
  <td><code>spark.deploy.maxDrivers</code></td>
  <td>Int.MaxValue</td>
  <td>
    The maximum number of running drivers.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.deploy.appNumberModulo</code></td>
  <td>(None)</td>
  <td>
    The modulo for app number. By default, the next of `app-yyyyMMddHHmmss-9999` is
    `app-yyyyMMddHHmmss-10000`. If we have 10000 as modulo, it will be `app-yyyyMMddHHmmss-0000`.
    In most cases, the prefix `app-yyyyMMddHHmmss` is increased already during creating 10000 applications.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.deploy.driverIdPattern</code></td>
  <td>driver-%s-%04d</td>
  <td>
    The pattern for driver ID generation based on Java `String.format` method.
    The default value is `driver-%s-%04d` which represents the existing driver id string, e.g., `driver-20231031224459-0019`. Please be careful to generate unique IDs.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.deploy.appIdPattern</code></td>
  <td>app-%s-%04d</td>
  <td>
    The pattern for app ID generation based on Java `String.format` method.
    The default value is `app-%s-%04d` which represents the existing app id string, e.g.,
    `app-20231031224509-0008`. Plesae be careful to generate unique IDs.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.timeout</code></td>
  <td>60</td>
  <td>
    Number of seconds after which the standalone deploy master considers a worker lost if it
    receives no heartbeats.
  </td>
  <td>0.6.2</td>
</tr>
<tr>
  <td><code>spark.dead.worker.persistence</code></td>
  <td>15</td>
  <td>
    Number of iterations to keep the deae worker information in UI. By default, the dead worker is visible for (15 + 1) * <code>spark.worker.timeout</code> since its last heartbeat.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.worker.resource.{name}.amount</code></td>
  <td>(none)</td>
  <td>
    Amount of a particular resource to use on the worker.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.resource.{name}.discoveryScript</code></td>
  <td>(none)</td>
  <td>
    Path to resource discovery script, which is used to find a particular resource while worker starting up.
    And the output of the script should be formatted like the <code>ResourceInformation</code> class.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.resourcesFile</code></td>
  <td>(none)</td>
  <td>
    Path to resources file which is used to find various resources while worker starting up.
    The content of resources file should be formatted like
    <code>[{"id":{"componentName": "spark.worker", "resourceName":"gpu"}, "addresses":["0","1","2"]}]</code>.
    If a particular resource is not found in the resources file, the discovery script would be used to
    find that resource. If the discovery script also does not find the resources, the worker will fail
    to start up.
  </td>
  <td>3.0.0</td>
</tr>
</table>

SPARK_WORKER_OPTS supports the following system properties:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.worker.initialRegistrationRetries</code></td>
  <td>6</td>
  <td>
    The number of retries to reconnect in short intervals (between 5 and 15 seconds).
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.maxRegistrationRetries</code></td>
  <td>16</td>
  <td>
    The max number of retries to reconnect.
    After <code>spark.worker.initialRegistrationRetries</code> attempts, the interval is between
    30 and 90 seconds.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.cleanup.enabled</code></td>
  <td>true</td>
  <td>
    Enable periodic cleanup of worker / application directories.  Note that this only affects standalone
    mode, as YARN works differently. Only the directories of stopped applications are cleaned up.
    This should be enabled if spark.shuffle.service.db.enabled is "true"
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.cleanup.interval</code></td>
  <td>1800 (30 minutes)</td>
  <td>
    Controls the interval, in seconds, at which the worker cleans up old application work dirs
    on the local machine.
  </td>
  <td>1.0.0</td>
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
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.enabled</code></td>
  <td>true</td>
  <td>
    Store External Shuffle service state on local disk so that when the external shuffle service is restarted, it will
    automatically reload info on current executors.  This only affects standalone mode (yarn always has this behavior
    enabled).  You should also enable <code>spark.worker.cleanup.enabled</code>, to ensure that the state
    eventually gets cleaned up.  This config may be removed in the future.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.backend</code></td>
  <td>ROCKSDB</td>
  <td>
    When <code>spark.shuffle.service.db.enabled</code> is true, user can use this to specify the kind of disk-based
    store used in shuffle service state store. This supports `ROCKSDB` and `LEVELDB` (deprecated) now and `ROCKSDB` as default value.
    The original data store in `RocksDB/LevelDB` will not be automatically convert to another kind of storage now.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.storage.cleanupFilesAfterExecutorExit</code></td>
  <td>true</td>
  <td>
    Enable cleanup non-shuffle files(such as temp. shuffle blocks, cached RDD/broadcast blocks,
    spill files, etc) of worker directories following executor exits. Note that this doesn't
    overlap with `spark.worker.cleanup.enabled`, as this enables cleanup of non-shuffle files in
    local directories of a dead executor, while `spark.worker.cleanup.enabled` enables cleanup of
    all files/subdirectories of a stopped and timeout application.
    This only affects Standalone mode, support of other cluster managers can be added in the future.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.worker.ui.compressedLogFileLengthCacheSize</code></td>
  <td>100</td>
  <td>
    For compressed log files, the uncompressed file can only be computed by uncompressing the files.
    Spark caches the uncompressed file size of compressed log files. This property controls the cache
    size.
  </td>
  <td>2.0.2</td>
</tr>
<tr>
  <td><code>spark.worker.idPattern</code></td>
  <td>worker-%s-%s-%d</td>
  <td>
    The pattern for worker ID generation based on Java `String.format` method.
    The default value is `worker-%s-%s-%d` which represents the existing worker id string, e.g.,
    `worker-20231109183042-[fe80::1%lo0]-39729`. Please be careful to generate unique IDs
  </td>
  <td>4.0.0</td>
</tr>
</table>

# Resource Allocation and Configuration Overview

Please make sure to have read the Custom Resource Scheduling and Configuration Overview section on the [configuration page](configuration.html). This section only talks about the Spark Standalone specific aspects of resource scheduling.

Spark Standalone has 2 parts, the first is configuring the resources for the Worker, the second is the resource allocation for a specific application.

The user must configure the Workers to have a set of resources available so that it can assign them out to Executors. The <code>spark.worker.resource.{resourceName}.amount</code> is used to control the amount of each resource the worker has allocated. The user must also specify either <code>spark.worker.resourcesFile</code> or <code>spark.worker.resource.{resourceName}.discoveryScript</code> to specify how the Worker discovers the resources its assigned. See the descriptions above for each of those to see which method works best for your setup.

The second part is running an application on Spark Standalone. The only special case from the standard Spark resource configs is when you are running the Driver in client mode. For a Driver in client mode, the user can specify the resources it uses via <code>spark.driver.resourcesFile</code> or <code>spark.driver.resource.{resourceName}.discoveryScript</code>. If the Driver is running on the same host as other Drivers, please make sure the resources file or discovery script only returns resources that do not conflict with other Drivers running on the same node.

Note, the user does not need to specify a discovery script when submitting an application as the Worker will start each Executor with the resources it allocates to it.

# Connecting an Application to the Cluster

To run an application on the Spark cluster, simply pass the `spark://IP:PORT` URL of the master as to the [`SparkContext`
constructor](rdd-programming-guide.html#initializing-spark).

To run an interactive Spark shell against the cluster, run the following command:

    ./bin/spark-shell --master spark://IP:PORT

You can also pass an option `--total-executor-cores <numCores>` to control the number of cores that spark-shell uses on the cluster.

# Client Properties

Spark applications supports the following configuration properties specific to standalone mode:

<table class="spark-config">
  <thead><tr><th>Property Name</th><th>Default Value</th><th>Meaning</th><th>Since Version</th></tr></thead>
  <tr>
  <td><code>spark.standalone.submit.waitAppCompletion</code></td>
  <td><code>false</code></td>
  <td>
  In standalone cluster mode, controls whether the client waits to exit until the application completes.
  If set to <code>true</code>, the client process will stay alive polling the driver's status.
  Otherwise, the client process will exit after submission.
  </td>
  <td>3.1.0</td>
  </tr>
</table>


# Launching Spark Applications

## Spark Protocol

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

## REST API

If `spark.master.rest.enabled` is enabled, Spark master provides additional REST API
via <code>http://[host:port]/[version]/submissions/[action]</code> where
<code>host</code> is the master host, and
<code>port</code> is the port number specified by `spark.master.rest.port` (default: 6066), and 
<code>version</code> is a protocol version, <code>v1</code> as of today, and
<code>action</code> is one of the following supported actions.

<table class="spark-config">
  <thead><tr><th>Command</th><th>Description</th><th>HTTP METHOD</th><th>Since Version</th></tr></thead>
  <tr>
    <td><code>create</code></td>
    <td>Create a Spark driver via <code>cluster</code> mode. Since 4.0.0, Spark master supports server-side
      variable replacements for the values of Spark properties and environment variables.
    </td>
    <td>POST</td>
    <td>1.3.0</td>
  </tr>
  <tr>
    <td><code>kill</code></td>
    <td>Kill a single Spark driver.</td>
    <td>POST</td>
    <td>1.3.0</td>
  </tr>
  <tr>
    <td><code>killall</code></td>
    <td>Kill all running Spark drivers.</td>
    <td>POST</td>
    <td>4.0.0</td>
  </tr>
  <tr>
    <td><code>status</code></td>
    <td>Check the status of a Spark job.</td>
    <td>GET</td>
    <td>1.3.0</td>
  </tr>
  <tr>
    <td><code>clear</code></td>
    <td>Clear the completed drivers and applications.</td>
    <td>POST</td>
    <td>4.0.0</td>
  </tr>
</table>

The following is a <code>curl</code> CLI command example with the `pi.py` and REST API.

```bash
$ curl -XPOST http://IP:PORT/v1/submissions/create \
--header "Content-Type:application/json;charset=UTF-8" \
--data '{
  "appResource": "",
  "sparkProperties": {
    "spark.master": "spark://master:7077",
    "spark.app.name": "Spark Pi",
    "spark.driver.memory": "1g",
    "spark.driver.cores": "1",
    "spark.jars": ""
  },
  "clientSparkVersion": "",
  "mainClass": "org.apache.spark.deploy.SparkSubmit",
  "environmentVariables": { },
  "action": "CreateSubmissionRequest",
  "appArgs": [ "/opt/spark/examples/src/main/python/pi.py", "10" ]
}'
```

The following is the response from the REST API for the above <code>create</code> request.

```bash
{
  "action" : "CreateSubmissionResponse",
  "message" : "Driver successfully submitted as driver-20231124153531-0000",
  "serverSparkVersion" : "4.0.0",
  "submissionId" : "driver-20231124153531-0000",
  "success" : true
}
```

For <code>sparkProperties</code> and <code>environmentVariables</code>, users can use place
holders for server-side environment variables like the following.

```bash
{% raw %}
...
  "sparkProperties": {
    "spark.hadoop.fs.s3a.endpoint": "{{AWS_ENDPOINT_URL}}",
    "spark.hadoop.fs.s3a.endpoint.region": "{{AWS_REGION}}"
  },
  "environmentVariables": {
    "AWS_CA_BUNDLE": "{{AWS_CA_BUNDLE}}"
  },
...
{% endraw %}
```

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

# Executors Scheduling

The number of cores assigned to each executor is configurable. When `spark.executor.cores` is
explicitly set, multiple executors from the same application may be launched on the same worker
if the worker has enough cores and memory. Otherwise, each executor grabs all the cores available
on the worker by default, in which case only one executor per application may be launched on each
worker during one single schedule iteration.

# Stage Level Scheduling Overview

Stage level scheduling is supported on Standalone:
- When dynamic allocation is disabled: It allows users to specify different task resource requirements at the stage level and will use the same executors requested at startup.
- When dynamic allocation is enabled: Currently, when the Master allocates executors for one application, it will schedule based on the order of the ResourceProfile ids for multiple ResourceProfiles. The ResourceProfile with smaller id will be scheduled firstly. Normally this won’t matter as Spark finishes one stage before starting another one, the only case this might have an affect is in a job server type scenario, so its something to keep in mind. For scheduling, we will only take executor memory and executor cores from built-in executor resources and all other custom resources from a ResourceProfile, other built-in executor resources such as offHeap and memoryOverhead won't take any effect. The base default profile will be created based on the spark configs when you submit an application. Executor memory and executor cores from the base default profile can be propagated to custom ResourceProfiles, but all other custom resources can not be propagated.

## Caveats

As mentioned in [Dynamic Resource Allocation](job-scheduling.html#dynamic-resource-allocation), if cores for each executor is not explicitly specified with dynamic allocation enabled, spark will possibly acquire much more executors than expected. So you are recommended to explicitly set executor cores for each resource profile when using stage level scheduling.

# Monitoring and Logging

Spark's standalone mode offers a web-based user interface to monitor the cluster. The master and each worker has its own web UI that shows cluster and job statistics. By default, you can access the web UI for the master at port 8080. The port can be changed either in the configuration file or via command-line options.

In addition, detailed log output for each job is also written to the work directory of each worker node (`SPARK_HOME/work` by default). You will see two files for each job, `stdout` and `stderr`, with all output it wrote to its console.


# Running Alongside Hadoop

You can run Spark alongside your existing Hadoop cluster by just launching it as a separate service on the same machines. To access Hadoop data from Spark, just use an hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI). Alternatively, you can set up a separate cluster for Spark, and still have it access HDFS over the network; this will be slower than disk-local access, but may not be a concern if you are still running in the same local area network (e.g. you place a few Spark machines on each rack that you have Hadoop on).


# Configuring Ports for Network Security

Generally speaking, a Spark cluster and its services are not deployed on the public internet.
They are generally private services, and should only be accessible within the network of the
organization that deploys Spark. Access to the hosts and ports used by Spark services should
be limited to origin hosts that need to access the services.

This is particularly important for clusters using the standalone resource manager, as they do
not support fine-grained access control in a way that other resource managers do.

For a complete list of ports to configure, see the
[security page](security.html#configuring-ports-for-network-security).

# High Availability

By default, standalone scheduling clusters are resilient to Worker failures (insofar as Spark itself is resilient to losing work by moving it to other workers). However, the scheduler uses a Master to make scheduling decisions, and this (by default) creates a single point of failure: if the Master crashes, no new applications can be created. In order to circumvent this, we have two high availability schemes, detailed below.

## Standby Masters with ZooKeeper

**Overview**

Utilizing ZooKeeper to provide leader election and some state storage, you can launch multiple Masters in your cluster connected to the same ZooKeeper instance. One will be elected "leader" and the others will remain in standby mode. If the current leader dies, another Master will be elected, recover the old Master's state, and then resume scheduling. The entire recovery process (from the time the first leader goes down) should take between 1 and 2 minutes. Note that this delay only affects scheduling _new_ applications -- applications that were already running during Master failover are unaffected.

Learn more about getting started with ZooKeeper [here](https://zookeeper.apache.org/doc/current/zookeeperStarted.html).

**Configuration**

In order to enable this recovery mode, you can set `SPARK_DAEMON_JAVA_OPTS` in spark-env by configuring `spark.deploy.recoveryMode` and related `spark.deploy.zookeeper.*` configurations.

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

<table class="spark-config">
  <thead><tr><th>System property</th><th>Default Value</th><th>Meaning</th><th>Since Version</th></tr></thead>
  <tr>
    <td><code>spark.deploy.recoveryMode</code></td>
    <td>NONE</td>
    <td>The recovery mode setting to recover submitted Spark jobs with cluster mode when it failed and relaunches. Set to
      FILESYSTEM to enable file-system-based single-node recovery mode,
      ROCKSDB to enable RocksDB-based single-node recovery mode,
      ZOOKEEPER to use Zookeeper-based recovery mode, and
      CUSTOM to provide a customer provider class via additional `spark.deploy.recoveryMode.factory` configuration.
      NONE is the default value which disables this recovery mode.
    </td>
    <td>0.8.1</td>
  </tr>
  <tr>
    <td><code>spark.deploy.recoveryDirectory</code></td>
    <td>""</td>
    <td>The directory in which Spark will store recovery state, accessible from the Master's perspective.
      Note that the directory should be clearly manually if <code>spark.deploy.recoveryMode</code>
      or <code>spark.deploy.recoveryCompressionCodec</code> is changed.
    </td>
    <td>0.8.1</td>
  </tr>
  <tr>
    <td><code>spark.deploy.recoveryCompressionCodec</code></td>
    <td>(none)</td>
    <td>A compression codec for persistence engines. none (default), lz4, lzf, snappy, and zstd. Currently, only FILESYSTEM mode supports this configuration.</td>
    <td>4.0.0</td>
  </tr>
  <tr>
    <td><code>spark.deploy.recoveryTimeout</code></td>
    <td>(none)</td>
    <td>
      The timeout for recovery process. The default value is the same with
      <code>spark.worker.timeout</code>.
    </td>
    <td>4.0.0</td>
  </tr>
  <tr>
    <td><code>spark.deploy.recoveryMode.factory</code></td>
    <td>""</td>
    <td>A class to implement <code>StandaloneRecoveryModeFactory</code> interface</td>
    <td>1.2.0</td>
  </tr>
  <tr>
    <td><code>spark.deploy.zookeeper.url</code></td>
    <td>None</td>
    <td>When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.</td>
    <td>0.8.1</td>
  </tr>
  <tr>
    <td><code>spark.deploy.zookeeper.dir</code></td>
    <td>None</td>
    <td>When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state.</td>
    <td>0.8.1</td>
  </tr>
</table>

**Details**

* This solution can be used in tandem with a process monitor/manager like [monit](https://mmonit.com/monit/), or just to enable manual recovery via restart.
* While filesystem recovery seems straightforwardly better than not doing any recovery at all, this mode may be suboptimal for certain development or experimental purposes. In particular, killing a master via stop-master.sh does not clean up its recovery state, so whenever you start a new Master, it will enter recovery mode. This could increase the startup time by up to 1 minute if it needs to wait for all previously-registered Workers/clients to timeout.
* While it's not officially supported, you could mount an NFS directory as the recovery directory. If the original Master node dies completely, you could then start a Master on a different node, which would correctly recover all previously registered Workers/applications (equivalent to ZooKeeper recovery). Future applications will have to be able to find the new Master, however, in order to register.
