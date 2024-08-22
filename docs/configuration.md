---
layout: global
displayTitle: Spark Configuration
title: Configuration
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

Spark provides three locations to configure the system:

* [Spark properties](#spark-properties) control most application parameters and can be set by using
  a [SparkConf](api/scala/org/apache/spark/SparkConf.html) object, or through Java
  system properties.
* [Environment variables](#environment-variables) can be used to set per-machine settings, such as
  the IP address, through the `conf/spark-env.sh` script on each node.
* [Logging](#configuring-logging) can be configured through `log4j2.properties`.

# Spark Properties

Spark properties control most application settings and are configured separately for each
application. These properties can be set directly on a
[SparkConf](api/scala/org/apache/spark/SparkConf.html) passed to your
`SparkContext`. `SparkConf` allows you to configure some of the common properties
(e.g. master URL and application name), as well as arbitrary key-value pairs through the
`set()` method. For example, we could initialize an application with two threads as follows:

Note that we run with local[2], meaning two threads - which represents "minimal" parallelism,
which can help detect bugs that only exist when we run in a distributed context.

{% highlight scala %}
val conf = new SparkConf()
             .setMaster("local[2]")
             .setAppName("CountingSheep")
val sc = new SparkContext(conf)
{% endhighlight %}

Note that we can have more than 1 thread in local mode, and in cases like Spark Streaming, we may
actually require more than 1 thread to prevent any sort of starvation issues.

Properties that specify some time duration should be configured with a unit of time.
The following format is accepted:

    25ms (milliseconds)
    5s (seconds)
    10m or 10min (minutes)
    3h (hours)
    5d (days)
    1y (years)


Properties that specify a byte size should be configured with a unit of size.
The following format is accepted:

    1b (bytes)
    1k or 1kb (kibibytes = 1024 bytes)
    1m or 1mb (mebibytes = 1024 kibibytes)
    1g or 1gb (gibibytes = 1024 mebibytes)
    1t or 1tb (tebibytes = 1024 gibibytes)
    1p or 1pb (pebibytes = 1024 tebibytes)

While numbers without units are generally interpreted as bytes, a few are interpreted as KiB or MiB.
See documentation of individual configuration properties. Specifying units is desirable where
possible.

## Dynamically Loading Spark Properties

In some cases, you may want to avoid hard-coding certain configurations in a `SparkConf`. For
instance, if you'd like to run the same application with different masters or different
amounts of memory. Spark allows you to simply create an empty conf:

{% highlight scala %}
val sc = new SparkContext(new SparkConf())
{% endhighlight %}

Then, you can supply configuration values at runtime:
```sh
./bin/spark-submit \
  --name "My app" \
  --master "local[4]" \
  --conf spark.eventLog.enabled=false \
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  myApp.jar
```

The Spark shell and [`spark-submit`](submitting-applications.html)
tool support two ways to load configurations dynamically. The first is command line options,
such as `--master`, as shown above. `spark-submit` can accept any Spark property using the `--conf/-c`
flag, but uses special flags for properties that play a part in launching the Spark application.
Running `./bin/spark-submit --help` will show the entire list of these options.

When configurations are specified via the `--conf/-c` flags, `bin/spark-submit` will also read
configuration options from `conf/spark-defaults.conf`, in which each line consists of a key and
a value separated by whitespace. For example:

    spark.master            spark://5.6.7.8:7077
    spark.executor.memory   4g
    spark.eventLog.enabled  true
    spark.serializer        org.apache.spark.serializer.KryoSerializer

In addition, a property file with Spark configurations can be passed to `bin/spark-submit` via
`--properties-file` parameter. When this is set, Spark will no longer load configurations from
`conf/spark-defaults.conf` unless another parameter `--load-spark-defaults` is provided.

Any values specified as flags or in the properties file will be passed on to the application
and merged with those specified through SparkConf. Properties set directly on the SparkConf
take the highest precedence, then those through `--conf` flags or `--properties-file` passed to
`spark-submit` or `spark-shell`, then options in the `spark-defaults.conf` file. A few
configuration keys have been renamed since earlier versions of Spark; in such cases, the older
key names are still accepted, but take lower precedence than any instance of the newer key.

Spark properties mainly can be divided into two kinds: one is related to deploy, like
"spark.driver.memory", "spark.executor.instances", this kind of properties may not be affected when
setting programmatically through `SparkConf` in runtime, or the behavior is depending on which
cluster manager and deploy mode you choose, so it would be suggested to set through configuration
file or `spark-submit` command line options; another is mainly related to Spark runtime control,
like "spark.task.maxFailures", this kind of properties can be set in either way.

## Viewing Spark Properties

The application web UI at `http://<driver>:4040` lists Spark properties in the "Environment" tab.
This is a useful place to check to make sure that your properties have been set correctly. Note
that only values explicitly specified through `spark-defaults.conf`, `SparkConf`, or the command
line will appear. For all other configuration properties, you can assume the default value is used.

## Available Properties

Most of the properties that control internal settings have reasonable default values. Some
of the most common options to set are:

### Application Properties

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.app.name</code></td>
  <td>(none)</td>
  <td>
    The name of your application. This will appear in the UI and in log data.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.driver.cores</code></td>
  <td>1</td>
  <td>
    Number of cores to use for the driver process, only in cluster mode.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.driver.maxResultSize</code></td>
  <td>1g</td>
  <td>
    Limit of total size of serialized results of all partitions for each Spark action (e.g.
    collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total
    size is above this limit.
    Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory
    and memory overhead of objects in JVM). Setting a proper limit can protect the driver from
    out-of-memory errors.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.driver.memory</code></td>
  <td>1g</td>
  <td>
    Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the
    same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
    (e.g. <code>512m</code>, <code>2g</code>).
    <br />
    <em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-memory</code> command line option
    or in your default properties file.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.driver.memoryOverhead</code></td>
  <td>driverMemory * <code>spark.driver.memoryOverheadFactor</code>, with minimum of <code>spark.driver.minMemoryOverhead</code></td>
  <td>
    Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless
    otherwise specified. This is memory that accounts for things like VM overheads, interned strings,
    other native overheads, etc. This tends to grow with the container size (typically 6-10%).
    This option is currently supported on YARN and Kubernetes.
    <em>Note:</em> Non-heap memory includes off-heap memory
    (when <code>spark.memory.offHeap.enabled=true</code>) and memory used by other driver processes
    (e.g. python process that goes with a PySpark driver) and memory used by other non-driver
    processes running in the same container. The maximum memory size of container to running
    driver is determined by the sum of <code>spark.driver.memoryOverhead</code>
    and <code>spark.driver.memory</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.driver.minMemoryOverhead</code></td>
  <td>384m</td>
  <td>
    The minimum amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified, if <code>spark.driver.memoryOverhead</code> is not defined.
    This option is currently supported on YARN and Kubernetes.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.memoryOverheadFactor</code></td>
  <td>0.10</td>
  <td>
    Fraction of driver memory to be allocated as additional non-heap memory per driver process in cluster mode.
    This is memory that accounts for things like VM overheads, interned strings,
    other native overheads, etc. This tends to grow with the container size.
    This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults to
    0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks
    commonly fail with "Memory Overhead Exceeded" errors. This preempts this error
    with a higher default.
    This value is ignored if <code>spark.driver.memoryOverhead</code> is set directly.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.amount</code></td>
  <td>0</td>
  <td>
    Amount of a particular resource type to use on the driver.
    If this is used, you must also specify the
    <code>spark.driver.resource.{resourceName}.discoveryScript</code>
    for the driver to find the resource on startup.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.discoveryScript</code></td>
  <td>None</td>
  <td>
    A script for the driver to run to discover a particular resource type. This should
    write to STDOUT a JSON string in the format of the ResourceInformation class. This has a
    name and an array of addresses. For a client-submitted driver, discovery script must assign
    different resource addresses to this driver comparing to other drivers on the same host.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.vendor</code></td>
  <td>None</td>
  <td>
    Vendor of the resources to use for the driver. This option is currently
    only supported on Kubernetes and is actually both the vendor and domain following
    the Kubernetes device plugin naming convention. (e.g. For GPUs on Kubernetes
    this config would be set to nvidia.com or amd.com)
  </td>
  <td>3.0.0</td>
</tr>
<tr>
 <td><code>spark.resources.discoveryPlugin</code></td>
  <td>org.apache.spark.resource.ResourceDiscoveryScriptPlugin</td>
  <td>
    Comma-separated list of class names implementing
    org.apache.spark.api.resource.ResourceDiscoveryPlugin to load into the application.
    This is for advanced users to replace the resource discovery class with a
    custom implementation. Spark will try each class specified until one of them
    returns the resource information for that resource. It tries the discovery
    script last if none of the plugins return information for that resource.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.memory</code></td>
  <td>1g</td>
  <td>
    Amount of memory to use per executor process, in the same format as JVM memory strings with
    a size unit suffix ("k", "m", "g" or "t") (e.g. <code>512m</code>, <code>2g</code>).
  </td>
  <td>0.7.0</td>
</tr>
<tr>
 <td><code>spark.executor.pyspark.memory</code></td>
  <td>Not set</td>
  <td>
    The amount of memory to be allocated to PySpark in each executor, in MiB
    unless otherwise specified.  If set, PySpark memory for an executor will be
    limited to this amount. If not set, Spark will not limit Python's memory use
    and it is up to the application to avoid exceeding the overhead memory space
    shared with other non-JVM processes. When PySpark is run in YARN or Kubernetes, this memory
    is added to executor resource requests.
    <br/>
    <em>Note:</em> This feature is dependent on Python's <code>resource</code> module; therefore, the behaviors and
    limitations are inherited. For instance, Windows does not support resource limiting and actual
    resource is not limited on MacOS.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
 <td><code>spark.executor.memoryOverhead</code></td>
  <td>executorMemory * <code>spark.executor.memoryOverheadFactor</code>, with minimum of <code>spark.executor.minMemoryOverhead</code></td>
  <td>
    Amount of additional memory to be allocated per executor process, in MiB unless otherwise specified.
    This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc.
    This tends to grow with the executor size (typically 6-10%). This option is currently supported on YARN and Kubernetes.
    <br/>
    <em>Note:</em> Additional memory includes PySpark executor memory
    (when <code>spark.executor.pyspark.memory</code> is not configured) and memory used by other
    non-executor processes running in the same container. The maximum memory size of container to
    running executor is determined by the sum of <code>spark.executor.memoryOverhead</code>,
    <code>spark.executor.memory</code>, <code>spark.memory.offHeap.size</code> and
    <code>spark.executor.pyspark.memory</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.driver.minMemoryOverhead</code></td>
  <td>384m</td>
  <td>
    The minimum amount of non-heap memory to be allocated per executor process, in MiB unless otherwise specified, if <code>spark.executor.memoryOverhead</code> is not defined.
    This option is currently supported on YARN and Kubernetes.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.memoryOverheadFactor</code></td>
  <td>0.10</td>
  <td>
    Fraction of executor memory to be allocated as additional non-heap memory per executor process.
    This is memory that accounts for things like VM overheads, interned strings,
    other native overheads, etc. This tends to grow with the container size.
    This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults to
    0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks
    commonly fail with "Memory Overhead Exceeded" errors. This preempts this error
    with a higher default.
    This value is ignored if <code>spark.executor.memoryOverhead</code> is set directly.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.amount</code></td>
  <td>0</td>
  <td>
    Amount of a particular resource type to use per executor process.
    If this is used, you must also specify the
    <code>spark.executor.resource.{resourceName}.discoveryScript</code>
    for the executor to find the resource on startup.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.discoveryScript</code></td>
  <td>None</td>
  <td>
    A script for the executor to run to discover a particular resource type. This should
    write to STDOUT a JSON string in the format of the ResourceInformation class. This has a
    name and an array of addresses.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.vendor</code></td>
  <td>None</td>
  <td>
    Vendor of the resources to use for the executors. This option is currently
    only supported on Kubernetes and is actually both the vendor and domain following
    the Kubernetes device plugin naming convention. (e.g. For GPUs on Kubernetes
    this config would be set to nvidia.com or amd.com)
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.extraListeners</code></td>
  <td>(none)</td>
  <td>
    A comma-separated list of classes that implement <code>SparkListener</code>; when initializing
    SparkContext, instances of these classes will be created and registered with Spark's listener
    bus.  If a class has a single-argument constructor that accepts a SparkConf, that constructor
    will be called; otherwise, a zero-argument constructor will be called. If no valid constructor
    can be found, the SparkContext creation will fail with an exception.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.local.dir</code></td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get
    stored on disk. This should be on a fast, local disk in your system. It can also be a
    comma-separated list of multiple directories on different disks.

    <br/>
    <em>Note:</em> This will be overridden by SPARK_LOCAL_DIRS (Standalone) or
    LOCAL_DIRS (YARN) environment variables set by the cluster manager.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.logConf</code></td>
  <td>false</td>
  <td>
    Logs the effective SparkConf as INFO when a SparkContext is started.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.master</code></td>
  <td>(none)</td>
  <td>
    The cluster manager to connect to. See the list of
    <a href="submitting-applications.html#master-urls"> allowed master URL's</a>.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.submit.deployMode</code></td>
  <td>client</td>
  <td>
    The deploy mode of Spark driver program, either "client" or "cluster",
    Which means to launch driver program locally ("client")
    or remotely ("cluster") on one of the nodes inside the cluster.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.log.callerContext</code></td>
  <td>(none)</td>
  <td>
    Application information that will be written into Yarn RM log/HDFS audit log when running on Yarn/HDFS.
    Its length depends on the Hadoop configuration <code>hadoop.caller.context.max.size</code>. It should be concise,
    and typically can have up to 50 characters.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.log.level</code></td>
  <td>(none)</td>
  <td>
    When set, overrides any user-defined log settings as if calling
    <code>SparkContext.setLogLevel()</code> at Spark startup. Valid log levels include: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN".
  </td>
  <td>3.5.0</td>
</tr>
<tr>
  <td><code>spark.driver.supervise</code></td>
  <td>false</td>
  <td>
    If true, restarts the driver automatically if it fails with a non-zero exit status.
    Only has effect in Spark standalone mode.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.driver.timeout</code></td>
  <td>0min</td>
  <td>
    A timeout for Spark driver in minutes. 0 means infinite. For the positive time value,
    terminate the driver with the exit code 124 if it runs after timeout duration. To use,
    it's required to set <code>spark.plugins</code> with
    <code>org.apache.spark.deploy.DriverTimeoutPlugin</code>.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.log.localDir</code></td>
  <td>(none)</td>
  <td>
    Specifies a local directory to write driver logs and enable Driver Log UI Tab.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.log.dfsDir</code></td>
  <td>(none)</td>
  <td>
    Base directory in which Spark driver logs are synced, if <code>spark.driver.log.persistToDfs.enabled</code>
    is true. Within this base directory, each application logs the driver logs to an application specific file.
    Users may want to set this to a unified location like an HDFS directory so driver log files can be persisted
    for later usage. This directory should allow any Spark user to read/write files and the Spark History Server
    user to delete files. Additionally, older logs from this directory are cleaned by the
    <a href="monitoring.html#spark-history-server-configuration-options">Spark History Server</a> if
    <code>spark.history.fs.driverlog.cleaner.enabled</code> is true and, if they are older than max age configured
    by setting <code>spark.history.fs.driverlog.cleaner.maxAge</code>.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.log.persistToDfs.enabled</code></td>
  <td>false</td>
  <td>
    If true, spark application running in client mode will write driver logs to a persistent storage, configured
    in <code>spark.driver.log.dfsDir</code>. If <code>spark.driver.log.dfsDir</code> is not configured, driver logs
    will not be persisted. Additionally, enable the cleaner by setting <code>spark.history.fs.driverlog.cleaner.enabled</code>
    to true in <a href="monitoring.html#spark-history-server-configuration-options">Spark History Server</a>.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.log.layout</code></td>
  <td>%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n%ex</td>
  <td>
    The layout for the driver logs that are synced to <code>spark.driver.log.localDir</code> and <code>spark.driver.log.dfsDir</code>. If this is not configured,
    it uses the layout for the first appender defined in log4j2.properties. If that is also not configured, driver logs
    use the default layout.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.log.allowErasureCoding</code></td>
  <td>false</td>
  <td>
    Whether to allow driver logs to use erasure coding.  On HDFS, erasure coded files will not
    update as quickly as regular replicated files, so they make take longer to reflect changes
    written by the application. Note that even if this is true, Spark will still not force the
    file to use erasure coding, it will simply use file system defaults.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.decommission.enabled</code></td>
  <td>false</td>
  <td>
    When decommission enabled, Spark will try its best to shut down the executor gracefully.
    Spark will try to migrate all the RDD blocks (controlled by <code>spark.storage.decommission.rddBlocks.enabled</code>)
    and shuffle blocks (controlled by <code>spark.storage.decommission.shuffleBlocks.enabled</code>) from the decommissioning
    executor to a remote executor when <code>spark.storage.decommission.enabled</code> is enabled.
    With decommission enabled, Spark will also decommission an executor instead of killing when <code>spark.dynamicAllocation.enabled</code> enabled.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.executor.decommission.killInterval</code></td>
  <td>(none)</td>
  <td>
    Duration after which a decommissioned executor will be killed forcefully by an outside (e.g. non-spark) service.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.executor.decommission.forceKillTimeout</code></td>
  <td>(none)</td>
  <td>
    Duration after which a Spark will force a decommissioning executor to exit.
    This should be set to a high value in most situations as low values will prevent block migrations from having enough time to complete.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.executor.decommission.signal</code></td>
  <td>PWR</td>
  <td>
    The signal that used to trigger the executor to start decommission.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.executor.maxNumFailures</code></td>
  <td>numExecutors * 2, with minimum of 3</td>
  <td>
    The maximum number of executor failures before failing the application.
    This configuration only takes effect on YARN, or Kubernetes when 
    <code>spark.kubernetes.allocation.pods.allocator</code> is set to 'direct'.
  </td>
  <td>3.5.0</td>
</tr>
<tr>
  <td><code>spark.executor.failuresValidityInterval</code></td>
  <td>(none)</td>
  <td>
    Interval after which executor failures will be considered independent and
    not accumulate towards the attempt count.
    This configuration only takes effect on YARN, or Kubernetes when 
    <code>spark.kubernetes.allocation.pods.allocator</code> is set to 'direct'.
  </td>
  <td>3.5.0</td>
</tr>
</table>

Apart from these, the following properties are also available, and may be useful in some situations:

### Runtime Environment

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.driver.extraClassPath</code></td>
  <td>(none)</td>
  <td>
    Extra classpath entries to prepend to the classpath of the driver.

    <br /><em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-class-path</code> command line option or in
    your default properties file.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.defaultJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of default JVM options to prepend to <code>spark.driver.extraJavaOptions</code>.
    This is intended to be set by administrators.

    For instance, GC settings or other logging.
    Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap
    size settings can be set with <code>spark.driver.memory</code> in the cluster mode and through
    the <code>--driver-memory</code> command line option in the client mode.

    <br /><em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-java-options</code> command line option or in
    your default properties file.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of extra JVM options to pass to the driver. This is intended to be set by users.

    For instance, GC settings or other logging.
    Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap
    size settings can be set with <code>spark.driver.memory</code> in the cluster mode and through
    the <code>--driver-memory</code> command line option in the client mode.

    <br /><em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-java-options</code> command line option or in
    your default properties file.

    <code>spark.driver.defaultJavaOptions</code> will be prepended to this configuration.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>
    Set a special library path to use when launching the driver JVM.

    <br /><em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-library-path</code> command line option or in
    your default properties file.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.driver.userClassPathFirst</code></td>
  <td>false</td>
  <td>
    (Experimental) Whether to give user-added jars precedence over Spark's own jars when loading
    classes in the driver. This feature can be used to mitigate conflicts between Spark's
    dependencies and user dependencies. It is currently an experimental feature.

    This is used in cluster mode only.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.executor.extraClassPath</code></td>
  <td>(none)</td>
  <td>
    Extra classpath entries to prepend to the classpath of executors. This exists primarily for
    backwards-compatibility with older versions of Spark. Users typically should not need to set
    this option.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.defaultJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of default JVM options to prepend to <code>spark.executor.extraJavaOptions</code>.
    This is intended to be set by administrators.

    For instance, GC settings or other logging.
    Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this
    option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file
    used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory.

    The following symbols, if present will be interpolated: {{APP_ID}} will be replaced by
    application ID and {{EXECUTOR_ID}} will be replaced by executor ID. For example, to enable
    verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of:
    <code>-verbose:gc -Xloggc:/tmp/{{APP_ID}}-{{EXECUTOR_ID}}.gc</code>
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of extra JVM options to pass to executors. This is intended to be set by users.

    For instance, GC settings or other logging.
    Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this
    option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file
    used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory.

    The following symbols, if present will be interpolated: {{APP_ID}} will be replaced by
    application ID and {{EXECUTOR_ID}} will be replaced by executor ID. For example, to enable
    verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of:
    <code>-verbose:gc -Xloggc:/tmp/{{APP_ID}}-{{EXECUTOR_ID}}.gc</code>

    <code>spark.executor.defaultJavaOptions</code> will be prepended to this configuration.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>
    Set a special library path to use when launching executor JVM's.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxRetainedFiles</code></td>
  <td>-1</td>
  <td>
    Sets the number of latest rolling log files that are going to be retained by the system.
    Older log files will be deleted. Disabled by default.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.enableCompression</code></td>
  <td>false</td>
  <td>
    Enable executor log compression. If it is enabled, the rolled executor logs will be compressed.
    Disabled by default.
  </td>
  <td>2.0.2</td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxSize</code></td>
  <td>1024 * 1024</td>
  <td>
    Set the max size of the file in bytes by which the executor logs will be rolled over.
    Rolling is disabled by default. See <code>spark.executor.logs.rolling.maxRetainedFiles</code>
    for automatic cleaning of old logs.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.strategy</code></td>
  <td>"" (disabled)</td>
  <td>
    Set the strategy of rolling of executor logs. By default it is disabled. It can
    be set to "time" (time-based rolling) or "size" (size-based rolling) or "" (disabled). For "time",
    use <code>spark.executor.logs.rolling.time.interval</code> to set the rolling interval.
    For "size", use <code>spark.executor.logs.rolling.maxSize</code> to set
    the maximum file size for rolling.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.time.interval</code></td>
  <td>daily</td>
  <td>
    Set the time interval by which the executor logs will be rolled over.
    Rolling is disabled by default. Valid values are <code>daily</code>, <code>hourly</code>, <code>minutely</code> or
    any interval in seconds. See <code>spark.executor.logs.rolling.maxRetainedFiles</code>
    for automatic cleaning of old logs.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.executor.userClassPathFirst</code></td>
  <td>false</td>
  <td>
    (Experimental) Same functionality as <code>spark.driver.userClassPathFirst</code>, but
    applied to executor instances.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.executorEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>
    Add the environment variable specified by <code>EnvironmentVariableName</code> to the Executor
    process. The user can specify multiple of these to set multiple environment variables.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.redaction.regex</code></td>
  <td>(?i)secret|password|token|access[.]?key</td>
  <td>
    Regex to decide which Spark configuration properties and environment variables in driver and
    executor environments contain sensitive information. When this regex matches a property key or
    value, the value is redacted from the environment UI and various logs like YARN and event logs.
  </td>
  <td>2.1.2</td>
</tr>
<tr>
  <td><code>spark.redaction.string.regex</code></td>
  <td>(none)</td>
  <td>
    Regex to decide which parts of strings produced by Spark contain sensitive information.
    When this regex matches a string part, that string part is replaced by a dummy value.
    This is currently used to redact the output of SQL explain commands.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.python.profile</code></td>
  <td>false</td>
  <td>
    Enable profiling in Python worker, the profile result will show up by <code>sc.show_profiles()</code>,
    or it will be displayed before the driver exits. It also can be dumped into disk by
    <code>sc.dump_profiles(path)</code>. If some of the profile results had been displayed manually,
    they will not be displayed automatically before driver exiting.

    By default the <code>pyspark.profiler.BasicProfiler</code> will be used, but this can be overridden by
    passing a profiler class in as a parameter to the <code>SparkContext</code> constructor.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.python.profile.dump</code></td>
  <td>(none)</td>
  <td>
    The directory which is used to dump the profile result before driver exiting.
    The results will be dumped as separated file for each RDD. They can be loaded
    by <code>pstats.Stats()</code>. If this is specified, the profile result will not be displayed
    automatically.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.python.worker.memory</code></td>
  <td>512m</td>
  <td>
    Amount of memory to use per python worker process during aggregation, in the same
    format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
    (e.g. <code>512m</code>, <code>2g</code>).
    If the memory used during aggregation goes above this amount, it will spill the data into disks.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.python.worker.reuse</code></td>
  <td>true</td>
  <td>
    Reuse Python worker or not. If yes, it will use a fixed number of Python workers,
    does not need to fork() a Python process for every task. It will be very useful
    if there is a large broadcast, then the broadcast will not need to be transferred
    from JVM to Python worker for every task.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.files</code></td>
  <td></td>
  <td>
    Comma-separated list of files to be placed in the working directory of each executor. Globs are allowed.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.submit.pyFiles</code></td>
  <td></td>
  <td>
    Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Globs are allowed.
  </td>
  <td>1.0.1</td>
</tr>
<tr>
  <td><code>spark.jars</code></td>
  <td></td>
  <td>
    Comma-separated list of jars to include on the driver and executor classpaths. Globs are allowed.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.jars.packages</code></td>
  <td></td>
  <td>
    Comma-separated list of Maven coordinates of jars to include on the driver and executor
    classpaths. The coordinates should be groupId:artifactId:version. If <code>spark.jars.ivySettings</code>
    is given artifacts will be resolved according to the configuration in the file, otherwise artifacts
    will be searched for in the local maven repo, then maven central and finally any additional remote
    repositories given by the command-line option <code>--repositories</code>. For more details, see
    <a href="submitting-applications.html#advanced-dependency-management">Advanced Dependency Management</a>.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.jars.excludes</code></td>
  <td></td>
  <td>
    Comma-separated list of groupId:artifactId, to exclude while resolving the dependencies
    provided in <code>spark.jars.packages</code> to avoid dependency conflicts.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.jars.ivy</code></td>
  <td></td>
  <td>
    Path to specify the Ivy user directory, used for the local Ivy cache and package files from
    <code>spark.jars.packages</code>. This will override the Ivy property <code>ivy.default.ivy.user.dir</code>
    which defaults to ~/.ivy2.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.jars.ivySettings</code></td>
  <td></td>
  <td>
    Path to an Ivy settings file to customize resolution of jars specified using <code>spark.jars.packages</code>
    instead of the built-in defaults, such as maven central. Additional repositories given by the command-line
    option <code>--repositories</code> or <code>spark.jars.repositories</code> will also be included.
    Useful for allowing Spark to resolve artifacts from behind a firewall e.g. via an in-house
    artifact server like Artifactory. Details on the settings file format can be
    found at <a href="http://ant.apache.org/ivy/history/latest-milestone/settings.html">Settings Files</a>.
    Only paths with <code>file://</code> scheme are supported. Paths without a scheme are assumed to have
    a <code>file://</code> scheme.
    <p/>
    When running in YARN cluster mode, this file will also be localized to the remote driver for dependency
    resolution within <code>SparkContext#addJar</code>
  </td>
  <td>2.2.0</td>
</tr>
 <tr>
  <td><code>spark.jars.repositories</code></td>
  <td></td>
  <td>
    Comma-separated list of additional remote repositories to search for the maven coordinates
    given with <code>--packages</code> or <code>spark.jars.packages</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.archives</code></td>
  <td></td>
  <td>
    Comma-separated list of archives to be extracted into the working directory of each executor.
    .jar, .tar.gz, .tgz and .zip are supported. You can specify the directory name to unpack via
    adding <code>#</code> after the file name to unpack, for example, <code>file.zip#directory</code>.
    This configuration is experimental.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.pyspark.driver.python</code></td>
  <td></td>
  <td>
    Python binary executable to use for PySpark in driver.
    (default is <code>spark.pyspark.python</code>)
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.pyspark.python</code></td>
  <td></td>
  <td>
    Python binary executable to use for PySpark in both driver and executors.
  </td>
  <td>2.1.0</td>
</tr>
</table>

### Shuffle Behavior

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.reducer.maxSizeInFlight</code></td>
  <td>48m</td>
  <td>
    Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless
    otherwise specified. Since each output requires us to create a buffer to receive it, this
    represents a fixed memory overhead per reduce task, so keep it small unless you have a
    large amount of memory.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.reducer.maxReqsInFlight</code></td>
  <td>Int.MaxValue</td>
  <td>
    This configuration limits the number of remote requests to fetch blocks at any given point.
    When the number of hosts in the cluster increase, it might lead to very large number
    of inbound connections to one or more nodes, causing the workers to fail under load.
    By allowing it to limit the number of fetch requests, this scenario can be mitigated.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.reducer.maxBlocksInFlightPerAddress</code></td>
  <td>Int.MaxValue</td>
  <td>
    This configuration limits the number of remote blocks being fetched per reduce task from a
    given host port. When a large number of blocks are being requested from a given address in a
    single fetch or simultaneously, this could crash the serving executor or Node Manager. This
    is especially useful to reduce the load on the Node Manager when external shuffle is enabled.
    You can mitigate this issue by setting it to a lower value.
  </td>
  <td>2.2.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress map output files. Generally a good idea. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.file.buffer</code></td>
  <td>32k</td>
  <td>
    Size of the in-memory buffer for each shuffle file output stream, in KiB unless otherwise
    specified. These buffers reduce the number of disk seeks and system calls made in creating
    intermediate shuffle files.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.unsafe.file.output.buffer</code></td>
  <td>32k</td>
  <td>
    Deprecated since Spark 4.0, please use <code>spark.shuffle.localDisk.file.output.buffer</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.localDisk.file.output.buffer</code></td>
  <td>32k</td>
  <td>
    The file system for this buffer size after each partition is written in all local disk shuffle writers.
    In KiB unless otherwise specified.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.diskWriteBufferSize</code></td>
  <td>1024 * 1024</td>
  <td>
    The buffer size, in bytes, to use when writing the sorted records to an on-disk file.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.maxRetries</code></td>
  <td>3</td>
  <td>
    (Netty only) Fetches that fail due to IO-related exceptions are automatically retried if this is
    set to a non-zero value. This retry logic helps stabilize large shuffles in the face of long GC
    pauses or transient network connectivity issues.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.numConnectionsPerPeer</code></td>
  <td>1</td>
  <td>
    (Netty only) Connections between hosts are reused in order to reduce connection buildup for
    large clusters. For clusters with many hard disks and few hosts, this may result in insufficient
    concurrency to saturate all disks, and so users may consider increasing this value.
  </td>
  <td>1.2.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.preferDirectBufs</code></td>
  <td>true</td>
  <td>
    (Netty only) Off-heap buffers are used to reduce garbage collection during shuffle and cache
    block transfer. For environments where off-heap memory is tightly limited, users may wish to
    turn this off to force all allocations from Netty to be on-heap.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.retryWait</code></td>
  <td>5s</td>
  <td>
    (Netty only) How long to wait between retries of fetches. The maximum delay caused by retrying
    is 15 seconds by default, calculated as <code>maxRetries * retryWait</code>.
  </td>
  <td>1.2.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.backLog</code></td>
  <td>-1</td>
  <td>
    Length of the accept queue for the shuffle service. For large applications, this value may
    need to be increased, so that incoming connections are not dropped if the service cannot keep
    up with a large number of connections arriving in a short period of time. This needs to
    be configured wherever the shuffle service itself is running, which may be outside of the
    application (see <code>spark.shuffle.service.enabled</code> option below). If set below 1,
    will fallback to OS default defined by Netty's <code>io.netty.util.NetUtil#SOMAXCONN</code>.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.connectionTimeout</code></td>
  <td>value of <code>spark.network.timeout</code></td>
  <td>
    Timeout for the established connections between shuffle servers and clients to be marked
    as idled and closed if there are still outstanding fetch requests but no traffic no the channel
    for at least <code>connectionTimeout</code>.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.io.connectionCreationTimeout</code></td>
  <td>value of <code>spark.shuffle.io.connectionTimeout</code></td>
  <td>
    Timeout for establishing a connection between the shuffle servers and clients.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.enabled</code></td>
  <td>false</td>
  <td>
    Enables the external shuffle service. This service preserves the shuffle files written by
    executors e.g. so that executors can be safely removed, or so that shuffle fetches can continue in
    the event of executor failure. The external shuffle service must be set up in order to enable it. See
    <a href="job-scheduling.html#configuration-and-setup">dynamic allocation
    configuration and setup documentation</a> for more information.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.port</code></td>
  <td>7337</td>
  <td>
    Port on which the external shuffle service will run.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.name</code></td>
  <td>spark_shuffle</td>
  <td>
    The configured name of the Spark shuffle service the client should communicate with.
    This must match the name used to configure the Shuffle within the YARN NodeManager configuration
    (<code>yarn.nodemanager.aux-services</code>). Only takes effect
    when <code>spark.shuffle.service.enabled</code> is set to true.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.index.cache.size</code></td>
  <td>100m</td>
  <td>
    Cache entries limited to the specified memory footprint, in bytes unless otherwise specified.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.removeShuffle</code></td>
  <td>true</td>
  <td>
    Whether to use the ExternalShuffleService for deleting shuffle blocks for
    deallocated executors when the shuffle is no longer needed. Without this enabled,
    shuffle data on executors that are deallocated will remain on disk until the
    application ends.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.maxChunksBeingTransferred</code></td>
  <td>Long.MAX_VALUE</td>
  <td>
    The max number of chunks allowed to be transferred at the same time on shuffle service.
    Note that new incoming connections will be closed when the max number is hit. The client will
    retry according to the shuffle retry configs (see <code>spark.shuffle.io.maxRetries</code> and
    <code>spark.shuffle.io.retryWait</code>), if those limits are reached the task will fail with
    fetch failure.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.sort.bypassMergeThreshold</code></td>
  <td>200</td>
  <td>
    (Advanced) In the sort-based shuffle manager, avoid merge-sorting data if there is no
    map-side aggregation and there are at most this many reduce partitions.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.sort.io.plugin.class</code></td>
  <td>org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO</td>
  <td>
    Name of the class to use for shuffle IO.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress data spilled during shuffles. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.accurateBlockThreshold</code></td>
  <td>100 * 1024 * 1024</td>
  <td>
    Threshold in bytes above which the size of shuffle blocks in HighlyCompressedMapStatus is
    accurately recorded. This helps to prevent OOM by avoiding underestimating shuffle
    block size when fetch shuffle blocks.
  </td>
  <td>2.2.1</td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.timeout</code></td>
  <td>5000</td>
  <td>
    Timeout in milliseconds for registration to the external shuffle service.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.maxAttempts</code></td>
  <td>3</td>
  <td>
    When we fail to register to the external shuffle service, we will retry for maxAttempts times.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.reduceLocality.enabled</code></td>
  <td>true</td>
  <td>
    Whether to compute locality preferences for reduce tasks.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.mapOutput.minSizeForBroadcast</code></td>
  <td>512k</td>
  <td>
    The size at which we use Broadcast to send the map output statuses to the executors.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.detectCorrupt</code></td>
  <td>true</td>
  <td>
    Whether to detect any corruption in fetched blocks.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.detectCorrupt.useExtraMemory</code></td>
  <td>false</td>
  <td>
    If enabled, part of a compressed/encrypted stream will be de-compressed/de-crypted by using extra memory
    to detect early corruption. Any IOException thrown will cause the task to be retried once
    and if it fails again with same exception, then FetchFailedException will be thrown to retry previous stage.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.useOldFetchProtocol</code></td>
  <td>false</td>
  <td>
    Whether to use the old protocol while doing the shuffle block fetching. It is only enabled while we need the
    compatibility in the scenario of new Spark version job fetching shuffle blocks from old version external shuffle service.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.readHostLocalDisk</code></td>
  <td>true</td>
  <td>
    If enabled (and <code>spark.shuffle.useOldFetchProtocol</code> is disabled, shuffle blocks requested from those block managers
    which are running on the same host are read from the disk directly instead of being fetched as remote blocks over the network.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.files.io.connectionTimeout</code></td>
  <td>value of <code>spark.network.timeout</code></td>
  <td>
    Timeout for the established connections for fetching files in Spark RPC environments to be marked
    as idled and closed if there are still outstanding files being downloaded but no traffic no the channel
    for at least <code>connectionTimeout</code>.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.files.io.connectionCreationTimeout</code></td>
  <td>value of <code>spark.files.io.connectionTimeout</code></td>
  <td>
    Timeout for establishing a connection for fetching files in Spark RPC environments.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.checksum.enabled</code></td>
  <td>true</td>
  <td>
    Whether to calculate the checksum of shuffle data. If enabled, Spark will calculate the checksum values for each partition
    data within the map output file and store the values in a checksum file on the disk. When there's shuffle data corruption
    detected, Spark will try to diagnose the cause (e.g., network issue, disk issue, etc.) of the corruption by using the checksum file.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.checksum.algorithm</code></td>
  <td>ADLER32</td>
  <td>
    The algorithm is used to calculate the shuffle checksum. Currently, it only supports built-in algorithms of JDK, e.g., ADLER32, CRC32.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.fetch.rdd.enabled</code></td>
  <td>false</td>
  <td>
    Whether to use the ExternalShuffleService for fetching disk persisted RDD blocks.
    In case of dynamic allocation if this feature is enabled executors having only disk
    persisted blocks are considered idle after
    <code>spark.dynamicAllocation.executorIdleTimeout</code> and will be released accordingly.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.enabled</code></td>
  <td>true</td>
  <td>
    Whether to use db in ExternalShuffleService. Note that this only affects standalone mode.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.backend</code></td>
  <td>ROCKSDB</td>
  <td>
    Specifies a disk-based store used in shuffle service local db. Setting as ROCKSDB or LEVELDB (deprecated).
  </td>
  <td>3.4.0</td>
</tr>
</table>

### Spark UI

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.eventLog.logBlockUpdates.enabled</code></td>
  <td>false</td>
  <td>
    Whether to log events for every block update, if <code>spark.eventLog.enabled</code> is true.
    *Warning*: This will increase the size of the event log considerably.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.longForm.enabled</code></td>
  <td>false</td>
  <td>
    If true, use the long form of call sites in the event log. Otherwise use the short form.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress logged events, if <code>spark.eventLog.enabled</code> is true.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.compression.codec</code></td>
  <td>zstd</td>
  <td>
    The codec to compress logged events. By default, Spark provides four codecs:
    <code>lz4</code>, <code>lzf</code>, <code>snappy</code>, and <code>zstd</code>.
    You can also use fully qualified class names to specify the codec, e.g.
    <code>org.apache.spark.io.LZ4CompressionCodec</code>,
    <code>org.apache.spark.io.LZFCompressionCodec</code>,
    <code>org.apache.spark.io.SnappyCompressionCodec</code>,
    and <code>org.apache.spark.io.ZStdCompressionCodec</code>.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.erasureCoding.enabled</code></td>
  <td>false</td>
  <td>
    Whether to allow event logs to use erasure coding, or turn erasure coding off, regardless of
    filesystem defaults.  On HDFS, erasure coded files will not update as quickly as regular
    replicated files, so the application updates will take longer to appear in the History Server.
    Note that even if this is true, Spark will still not force the file to use erasure coding, it
    will simply use filesystem defaults.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.dir</code></td>
  <td>file:///tmp/spark-events</td>
  <td>
    Base directory in which Spark events are logged, if <code>spark.eventLog.enabled</code> is true.
    Within this base directory, Spark creates a sub-directory for each application, and logs the
    events specific to the application in this directory. Users may want to set this to
    a unified location like an HDFS directory so history files can be read by the history server.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.enabled</code></td>
  <td>false</td>
  <td>
    Whether to log Spark events, useful for reconstructing the Web UI after the application has
    finished.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.overwrite</code></td>
  <td>false</td>
  <td>
    Whether to overwrite any existing files.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.buffer.kb</code></td>
  <td>100k</td>
  <td>
    Buffer size to use when writing to output streams, in KiB unless otherwise specified.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.rolling.enabled</code></td>
  <td>false</td>
  <td>
    Whether rolling over event log files is enabled. If set to true, it cuts down each event
    log file to the configured size.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.rolling.maxFileSize</code></td>
  <td>128m</td>
  <td>
    When <code>spark.eventLog.rolling.enabled=true</code>, specifies the max size of event log file before it's rolled over.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.dagGraph.retainedRootRDDs</code></td>
  <td>Int.MaxValue</td>
  <td>
    How many DAG graph nodes the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.ui.groupSQLSubExecutionEnabled</code></td>
  <td>true</td>
  <td>
    Whether to group sub executions together in SQL UI when they belong to the same root execution
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.ui.enabled</code></td>
  <td>true</td>
  <td>
    Whether to run the web UI for the Spark application.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.ui.store.path</code></td>
  <td>None</td>
  <td>
    Local directory where to cache application information for live UI.
    By default this is not set, meaning all application information will be kept in memory.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.ui.killEnabled</code></td>
  <td>true</td>
  <td>
    Allows jobs and stages to be killed from the web UI.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.threadDumpsEnabled</code></td>
  <td>true</td>
  <td>
    Whether to show a link for executor thread dumps in Stages and Executor pages.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.ui.threadDump.flamegraphEnabled</code></td>
  <td>true</td>
  <td>
    Whether to render the Flamegraph for executor thread dumps.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.heapHistogramEnabled</code></td>
  <td>true</td>
  <td>
    Whether to show a link for executor heap histogram in Executor page.
  </td>
  <td>3.5.0</td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.period</code></td>
  <td>100ms</td>
  <td>
    How often to update live entities. -1 means "never update" when replaying applications,
    meaning only the last write will happen. For live applications, this avoids a few
    operations that we can live without when rapidly processing incoming task events.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.minFlushPeriod</code></td>
  <td>1s</td>
  <td>
    Minimum time elapsed before stale UI data is flushed. This avoids UI staleness when incoming
    task events are not fired frequently.
  </td>
  <td>2.4.2</td>
</tr>
<tr>
  <td><code>spark.ui.port</code></td>
  <td>4040</td>
  <td>
    Port for your application's dashboard, which shows memory and workload data.
  </td>
  <td>0.7.0</td>
</tr>
<tr>
  <td><code>spark.ui.retainedJobs</code></td>
  <td>1000</td>
  <td>
    How many jobs the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.ui.retainedStages</code></td>
  <td>1000</td>
  <td>
    How many stages the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.ui.retainedTasks</code></td>
  <td>100000</td>
  <td>
    How many tasks in one stage the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
  <td>2.0.1</td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxy</code></td>
  <td>false</td>
  <td>
    Enable running Spark Master as reverse proxy for worker and application UIs. In this mode, Spark master will reverse proxy the worker and application UIs to enable access without requiring direct access to their hosts. Use it with caution, as worker and application UI will not be accessible directly, you will only be able to access them through spark master/proxy public URL. This setting affects all the workers and application UIs running in the cluster and must be set on all the workers, drivers and masters.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxyUrl</code></td>
  <td></td>
  <td>
    If the Spark UI should be served through another front-end reverse proxy, this is the URL
    for accessing the Spark master UI through that reverse proxy.
    This is useful when running proxy for authentication e.g. an OAuth proxy. The URL may contain
    a path prefix, like <code>http://mydomain.com/path/to/spark/</code>, allowing you to serve the
    UI for multiple Spark clusters and other web applications through the same virtual host and
    port.
    Normally, this should be an absolute URL including scheme (http/https), host and port.
    It is possible to specify a relative URL starting with "/" here. In this case, all URLs
    generated by the Spark UI and Spark REST APIs will be server-relative links -- this will still
    work, as the entire Spark UI is served through the same host and port.
    <br/>The setting affects link generation in the Spark UI, but the front-end reverse proxy
    is responsible for
    <ul>
      <li>stripping a path prefix before forwarding the request,</li>
      <li>rewriting redirects which point directly to the Spark master,</li>
      <li>redirecting access from <code>http://mydomain.com/path/to/spark</code> to
      <code>http://mydomain.com/path/to/spark/</code> (trailing slash after path prefix); otherwise
      relative links on the master page do not work correctly.</li>
    </ul>
    This setting affects all the workers and application UIs running in the cluster and must be set
    identically on all the workers, drivers and masters. In is only effective when
    <code>spark.ui.reverseProxy</code> is turned on. This setting is not needed when the Spark
    master web UI is directly reachable.<br/>
    Note that the value of the setting can't contain the keyword <code>proxy</code> or <code>history</code> after split by "/". Spark UI relies on both keywords for getting REST API endpoints from URIs.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.ui.proxyRedirectUri</code></td>
  <td></td>
  <td>
    Where to address redirects when Spark is running behind a proxy. This will make Spark
    modify redirect responses so they point to the proxy server, instead of the Spark UI's own
    address. This should be only the address of the server, without any prefix paths for the
    application; the prefix should be set either by the proxy server itself (by adding the
    <code>X-Forwarded-Context</code> request header), or by setting the proxy base in the Spark
    app's configuration.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.showConsoleProgress</code></td>
  <td>false</td>
  <td>
    Show the progress bar in the console. The progress bar shows the progress of stages
    that run for longer than 500ms. If multiple stages run at the same time, multiple
    progress bars will be displayed on the same line.
    <br/>
    <em>Note:</em> In shell environment, the default value of spark.ui.showConsoleProgress is true.
  </td>
  <td>1.2.1</td>
</tr>
<tr>
  <td><code>spark.ui.consoleProgress.update.interval</code></td>
  <td>200</td>
  <td>
    An interval in milliseconds to update the progress bar in the console.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.ui.custom.executor.log.url</code></td>
  <td>(none)</td>
  <td>
    Specifies custom spark executor log URL for supporting external log service instead of using cluster
    managers' application log URLs in Spark UI. Spark will support some path variables via patterns
    which can vary on cluster manager. Please check the documentation for your cluster manager to
    see which patterns are supported, if any. <p/>
    Please note that this configuration also replaces original log urls in event log,
    which will be also effective when accessing the application on history server. The new log urls must be
    permanent, otherwise you might have dead link for executor log urls.
    <p/>
    For now, only YARN and K8s cluster manager supports this configuration
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.prometheus.enabled</code></td>
  <td>true</td>
  <td>
    Expose executor metrics at /metrics/executors/prometheus at driver web page. 
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.worker.ui.retainedExecutors</code></td>
  <td>1000</td>
  <td>
    How many finished executors the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.worker.ui.retainedDrivers</code></td>
  <td>1000</td>
  <td>
    How many finished drivers the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.sql.ui.retainedExecutions</code></td>
  <td>1000</td>
  <td>
    How many finished executions the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.streaming.ui.retainedBatches</code></td>
  <td>1000</td>
  <td>
    How many finished batches the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.retainedDeadExecutors</code></td>
  <td>100</td>
  <td>
    How many dead executors the Spark UI and status APIs remember before garbage collecting.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.filters</code></td>
  <td>None</td>
  <td>
    Comma separated list of filter class names to apply to the Spark Web UI. The filter should be a
    standard <a href="http://docs.oracle.com/javaee/6/api/javax/servlet/Filter.html">
    javax servlet Filter</a>.

    <br />Filter parameters can also be specified in the configuration, by setting config entries
    of the form <code>spark.&lt;class name of filter&gt;.param.&lt;param name&gt;=&lt;value&gt;</code>

    <br />For example:
    <br /><code>spark.ui.filters=com.test.filter1</code>
    <br /><code>spark.com.test.filter1.param.name1=foo</code>
    <br /><code>spark.com.test.filter1.param.name2=bar</code>
    <br />
    <br />Note that some filter requires additional dependencies. For example,
    the built-in <code>org.apache.spark.ui.JWSFilter</code> requires
    <code>jjwt-impl</code> and <code>jjwt-jackson</code> jar files.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.requestHeaderSize</code></td>
  <td>8k</td>
  <td>
    The maximum allowed size for a HTTP request header, in bytes unless otherwise specified.
    This setting applies for the Spark History Server too.
  </td>
  <td>2.2.3</td>
</tr>
<tr>
  <td><code>spark.ui.timelineEnabled</code></td>
  <td>true</td>
  <td>
    Whether to display event timeline data on UI pages.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.ui.timeline.executors.maximum</code></td>
  <td>250</td>
  <td>
    The maximum number of executors shown in the event timeline.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.ui.timeline.jobs.maximum</code></td>
  <td>500</td>
  <td>
    The maximum number of jobs shown in the event timeline.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.ui.timeline.stages.maximum</code></td>
  <td>500</td>
  <td>
    The maximum number of stages shown in the event timeline.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.ui.timeline.tasks.maximum</code></td>
  <td>1000</td>
  <td>
    The maximum number of tasks shown in the event timeline.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.appStatusStore.diskStoreDir</code></td>
  <td>None</td>
  <td>
    Local directory where to store diagnostic information of SQL executions. This configuration is only for live UI.
  </td>
  <td>3.4.0</td>
</tr>
</table>

### Compression and Serialization

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.broadcast.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress broadcast variables before sending them. Generally a good idea.
    Compression will use <code>spark.io.compression.codec</code>.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.checkpoint.dir</code></td>
  <td>(none)</td>
  <td>
    Set the default directory for checkpointing. It can be overwritten by
    SparkContext.setCheckpointDir.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.checkpoint.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress RDD checkpoints. Generally a good idea.
    Compression will use <code>spark.io.compression.codec</code>.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.codec</code></td>
  <td>lz4</td>
  <td>
    The codec used to compress internal data such as RDD partitions, event log, broadcast variables
    and shuffle outputs. By default, Spark provides four codecs: <code>lz4</code>, <code>lzf</code>,
    <code>snappy</code>, and <code>zstd</code>. You can also use fully qualified class names to specify the codec,
    e.g.
    <code>org.apache.spark.io.LZ4CompressionCodec</code>,
    <code>org.apache.spark.io.LZFCompressionCodec</code>,
    <code>org.apache.spark.io.SnappyCompressionCodec</code>,
    and <code>org.apache.spark.io.ZStdCompressionCodec</code>.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.lz4.blockSize</code></td>
  <td>32k</td>
  <td>
    Block size used in LZ4 compression, in the case when LZ4 compression codec
    is used. Lowering this block size will also lower shuffle memory usage when LZ4 is used.
    Default unit is bytes, unless otherwise specified. This configuration only applies to
    <code>spark.io.compression.codec</code>.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.snappy.blockSize</code></td>
  <td>32k</td>
  <td>
    Block size in Snappy compression, in the case when Snappy compression codec is used.
    Lowering this block size will also lower shuffle memory usage when Snappy is used.
    Default unit is bytes, unless otherwise specified. This configuration only applies
    to <code>spark.io.compression.codec</code>.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.level</code></td>
  <td>1</td>
  <td>
    Compression level for Zstd compression codec. Increasing the compression level will result in better
    compression at the expense of more CPU and memory. This configuration only applies to
    <code>spark.io.compression.codec</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.bufferSize</code></td>
  <td>32k</td>
  <td>
    Buffer size in bytes used in Zstd compression, in the case when Zstd compression codec
    is used. Lowering this size will lower the shuffle memory usage when Zstd is used, but it
    might increase the compression cost because of excessive JNI call overhead. This
    configuration only applies to <code>spark.io.compression.codec</code>.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.bufferPool.enabled</code></td>
  <td>true</td>
  <td>
    If true, enable buffer pool of ZSTD JNI library.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.workers</code></td>
  <td>0</td>
  <td>
    Thread size spawned to compress in parallel when using Zstd. When value is 0
    no worker is spawned, it works in single-threaded mode. When value > 0, it triggers
    asynchronous mode, corresponding number of threads are spawned. More workers improve
    performance, but also increase memory cost.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.io.compression.lzf.parallel.enabled</code></td>
  <td>false</td>
  <td>
    When true, LZF compression will use multiple threads to compress data in parallel.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.kryo.classesToRegister</code></td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, give a comma-separated list of custom class names to register
    with Kryo.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.kryo.referenceTracking</code></td>
  <td>true</td>
  <td>
    Whether to track references to the same object when serializing data with Kryo, which is
    necessary if your object graphs have loops and useful for efficiency if they contain multiple
    copies of the same object. Can be disabled to improve performance if you know this is not the
    case.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.kryo.registrationRequired</code></td>
  <td>false</td>
  <td>
    Whether to require registration with Kryo. If set to 'true', Kryo will throw an exception
    if an unregistered class is serialized. If set to false (the default), Kryo will write
    unregistered class names along with each object. Writing class names can cause
    significant performance overhead, so enabling this option can enforce strictly that a
    user has not omitted classes from registration.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.kryo.registrator</code></td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, give a comma-separated list of classes that register your custom classes with Kryo. This
    property is useful if you need to register your classes in a custom way, e.g. to specify a custom
    field serializer. Otherwise <code>spark.kryo.classesToRegister</code> is simpler. It should be
    set to classes that extend
    <a href="api/scala/org/apache/spark/serializer/KryoRegistrator.html">
    <code>KryoRegistrator</code></a>.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.kryo.unsafe</code></td>
  <td>true</td>
  <td>
    Whether to use unsafe based Kryo serializer. Can be
    substantially faster by using Unsafe Based IO.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer.max</code></td>
  <td>64m</td>
  <td>
    Maximum allowable size of Kryo serialization buffer, in MiB unless otherwise specified.
    This must be larger than any object you attempt to serialize and must be less than 2048m.
    Increase this if you get a "buffer limit exceeded" exception inside Kryo.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer</code></td>
  <td>64k</td>
  <td>
    Initial size of Kryo's serialization buffer, in KiB unless otherwise specified.
    Note that there will be one buffer <i>per core</i> on each worker. This buffer will grow up to
    <code>spark.kryoserializer.buffer.max</code> if needed.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.rdd.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress serialized RDD partitions (e.g. for
    <code>StorageLevel.MEMORY_ONLY_SER</code> in Java
    and Scala or <code>StorageLevel.MEMORY_ONLY</code> in Python).
    Can save substantial space at the cost of some extra CPU time.
    Compression will use <code>spark.io.compression.codec</code>.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.serializer</code></td>
  <td>
    org.apache.spark.serializer.<br />JavaSerializer
  </td>
  <td>
    Class to use for serializing objects that will be sent over the network or need to be cached
    in serialized form. The default of Java serialization works with any Serializable Java object
    but is quite slow, so we recommend <a href="tuning.html">using
    <code>org.apache.spark.serializer.KryoSerializer</code> and configuring Kryo serialization</a>
    when speed is necessary. Can be any subclass of
    <a href="api/scala/org/apache/spark/serializer/Serializer.html">
    <code>org.apache.spark.Serializer</code></a>.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.serializer.objectStreamReset</code></td>
  <td>100</td>
  <td>
    When serializing using org.apache.spark.serializer.JavaSerializer, the serializer caches
    objects to prevent writing redundant data, however that stops garbage collection of those
    objects. By calling 'reset' you flush that info from the serializer, and allow old
    objects to be collected. To turn off this periodic reset set it to -1.
    By default it will reset the serializer every 100 objects.
  </td>
  <td>1.0.0</td>
</tr>
</table>

### Memory Management

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.memory.fraction</code></td>
  <td>0.6</td>
  <td>
    Fraction of (heap space - 300MB) used for execution and storage. The lower this is, the
    more frequently spills and cached data eviction occur. The purpose of this config is to set
    aside memory for internal metadata, user data structures, and imprecise size estimation
    in the case of sparse, unusually large records. Leaving this at the default value is
    recommended. For more detail, including important information about correctly tuning JVM
    garbage collection when increasing this value, see
    <a href="tuning.html#memory-management-overview">this description</a>.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.memory.storageFraction</code></td>
  <td>0.5</td>
  <td>
    Amount of storage memory immune to eviction, expressed as a fraction of the size of the
    region set aside by <code>spark.memory.fraction</code>. The higher this is, the less
    working memory may be available to execution and tasks may spill to disk more often.
    Leaving this at the default value is recommended. For more detail, see
    <a href="tuning.html#memory-management-overview">this description</a>.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.memory.offHeap.enabled</code></td>
  <td>false</td>
  <td>
    If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory
    use is enabled, then <code>spark.memory.offHeap.size</code> must be positive.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.memory.offHeap.size</code></td>
  <td>0</td>
  <td>
    The absolute amount of memory which can be used for off-heap allocation, in bytes unless otherwise specified.
    This setting has no impact on heap memory usage, so if your executors' total memory consumption
    must fit within some hard limit then be sure to shrink your JVM heap size accordingly.
    This must be set to a positive value when <code>spark.memory.offHeap.enabled=true</code>.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.storage.unrollMemoryThreshold</code></td>
  <td>1024 * 1024</td>
  <td>
    Initial memory to request before unrolling any block.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.replication.proactive</code></td>
  <td>false</td>
  <td>
    Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to
    executor failures are replenished if there are any existing available replicas. This tries
    to get the replication level of the block to the initial number.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.storage.localDiskByExecutors.cacheSize</code></td>
  <td>1000</td>
  <td>
    The max number of executors for which the local dirs are stored. This size is both applied for the driver and
    both for the executors side to avoid having an unbounded store. This cache will be used to avoid the network
    in case of fetching disk persisted RDD blocks or shuffle blocks (when <code>spark.shuffle.readHostLocalDisk</code> is set) from the same host.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.cleaner.periodicGC.interval</code></td>
  <td>30min</td>
  <td>
    Controls how often to trigger a garbage collection.<br><br>
    This context cleaner triggers cleanups only when weak references are garbage collected.
    In long-running applications with large driver JVMs, where there is little memory pressure
    on the driver, this may happen very occasionally or not at all. Not cleaning at all may
    lead to executors running out of disk space after a while.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking</code></td>
  <td>true</td>
  <td>
    Enables or disables context cleaning.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking</code></td>
  <td>true</td>
  <td>
    Controls whether the cleaning thread should block on cleanup tasks (other than shuffle, which is controlled by
    <code>spark.cleaner.referenceTracking.blocking.shuffle</code> Spark property).
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking.shuffle</code></td>
  <td>false</td>
  <td>
    Controls whether the cleaning thread should block on shuffle cleanup tasks.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.cleanCheckpoints</code></td>
  <td>false</td>
  <td>
    Controls whether to clean checkpoint files if the reference is out of scope.
  </td>
  <td>1.4.0</td>
</tr>
</table>

### Execution Behavior

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.broadcast.blockSize</code></td>
  <td>4m</td>
  <td>
    Size of each piece of a block for <code>TorrentBroadcastFactory</code>, in KiB unless otherwise
    specified. Too large a value decreases parallelism during broadcast (makes it slower); however,
    if it is too small, <code>BlockManager</code> might take a performance hit.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.broadcast.checksum</code></td>
  <td>true</td>
  <td>
    Whether to enable checksum for broadcast. If enabled, broadcasts will include a checksum, which can
    help detect corrupted blocks, at the cost of computing and sending a little more data. It's possible
    to disable it if the network has other mechanisms to guarantee data won't be corrupted during broadcast.
  </td>
  <td>2.1.1</td>
</tr>
<tr>
  <td><code>spark.broadcast.UDFCompressionThreshold</code></td>
  <td>1 * 1024 * 1024</td>
  <td>
    The threshold at which user-defined functions (UDFs) and Python RDD commands are compressed by broadcast in bytes unless otherwise specified.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.cores</code></td>
  <td>
    1 in YARN mode, all the available cores on the worker in standalone mode.
  </td>
  <td>
    The number of cores to use on each executor.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.default.parallelism</code></td>
  <td>
    For distributed shuffle operations like <code>reduceByKey</code> and <code>join</code>, the
    largest number of partitions in a parent RDD.  For operations like <code>parallelize</code>
    with no parent RDDs, it depends on the cluster manager:
    <ul>
      <li>Local mode: number of cores on the local machine</li>
      <li>Others: total number of cores on all executor nodes or 2, whichever is larger</li>
    </ul>
  </td>
  <td>
    Default number of partitions in RDDs returned by transformations like <code>join</code>,
    <code>reduceByKey</code>, and <code>parallelize</code> when not set by user.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.executor.heartbeatInterval</code></td>
  <td>10s</td>
  <td>
    Interval between each executor's heartbeats to the driver.  Heartbeats let
    the driver know that the executor is still alive and update it with metrics for in-progress
    tasks. spark.executor.heartbeatInterval should be significantly less than
    spark.network.timeout
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.files.fetchTimeout</code></td>
  <td>60s</td>
  <td>
    Communication timeout to use when fetching files added through SparkContext.addFile() from
    the driver.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.files.useFetchCache</code></td>
  <td>true</td>
  <td>
    If set to true (default), file fetching will use a local cache that is shared by executors
    that belong to the same application, which can improve task launching performance when
    running many executors on the same host. If set to false, these caching optimizations will
    be disabled and all executors will fetch their own copies of files. This optimization may be
    disabled in order to use Spark local directories that reside on NFS filesystems (see
    <a href="https://issues.apache.org/jira/browse/SPARK-6313">SPARK-6313</a> for more details).
  </td>
  <td>1.2.2</td>
</tr>
<tr>
  <td><code>spark.files.overwrite</code></td>
  <td>false</td>
  <td>
    Whether to overwrite any files which exist at the startup. Users can not overwrite the files added by
    <code>SparkContext.addFile</code> or <code>SparkContext.addJar</code> before even if this option is set
    <code>true</code>.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.files.ignoreCorruptFiles</code></td>
  <td>false</td>
  <td>
    Whether to ignore corrupt files. If true, the Spark jobs will continue to run when encountering corrupted or
    non-existing files and contents that have been read will still be returned.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.files.ignoreMissingFiles</code></td>
  <td>false</td>
  <td>
    Whether to ignore missing files. If true, the Spark jobs will continue to run when encountering missing files and
    the contents that have been read will still be returned.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.files.maxPartitionBytes</code></td>
  <td>134217728 (128 MiB)</td>
  <td>
    The maximum number of bytes to pack into a single partition when reading files.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.files.openCostInBytes</code></td>
  <td>4194304 (4 MiB)</td>
  <td>
    The estimated cost to open a file, measured by the number of bytes could be scanned at the same
    time. This is used when putting multiple files into a partition. It is better to overestimate,
    then the partitions with small files will be faster than partitions with bigger files.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.hadoop.cloneConf</code></td>
  <td>false</td>
  <td>
    If set to true, clones a new Hadoop <code>Configuration</code> object for each task.  This
    option should be enabled to work around <code>Configuration</code> thread-safety issues (see
    <a href="https://issues.apache.org/jira/browse/SPARK-2546">SPARK-2546</a> for more details).
    This is disabled by default in order to avoid unexpected performance regressions for jobs that
    are not affected by these issues.
  </td>
  <td>1.0.3</td>
</tr>
<tr>
  <td><code>spark.hadoop.validateOutputSpecs</code></td>
  <td>true</td>
  <td>
    If set to true, validates the output specification (e.g. checking if the output directory already exists)
    used in saveAsHadoopFile and other variants. This can be disabled to silence exceptions due to pre-existing
    output directories. We recommend that users do not disable this except if trying to achieve compatibility
    with previous versions of Spark. Simply use Hadoop's FileSystem API to delete output directories by hand.
    This setting is ignored for jobs generated through Spark Streaming's StreamingContext, since data may
    need to be rewritten to pre-existing output directories during checkpoint recovery.
  </td>
  <td>1.0.1</td>
</tr>
<tr>
  <td><code>spark.storage.memoryMapThreshold</code></td>
  <td>2m</td>
  <td>
    Size of a block above which Spark memory maps when reading a block from disk. Default unit is bytes,
    unless specified otherwise. This prevents Spark from memory mapping very small blocks. In general,
    memory mapping has high overhead for blocks close to or below the page size of the operating system.
  </td>
  <td>0.9.2</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.enabled</code></td>
  <td>false</td>
  <td>
    Whether to decommission the block manager when decommissioning executor.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.enabled</code></td>
  <td>true</td>
  <td>
    Whether to transfer shuffle blocks during block manager decommissioning. Requires a migratable shuffle resolver
    (like sort based shuffle).
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.maxThreads</code></td>
  <td>8</td>
  <td>
    Maximum number of threads to use in migrating shuffle files.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.rddBlocks.enabled</code></td>
  <td>true</td>
  <td>
    Whether to transfer RDD blocks during block manager decommissioning.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.fallbackStorage.path</code></td>
  <td>(none)</td>
  <td>
    The location for fallback storage during block manager decommissioning. For example, <code>s3a://spark-storage/</code>.
    In case of empty, fallback storage is disabled. The storage should be managed by TTL because Spark will not clean it up.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.fallbackStorage.cleanUp</code></td>
  <td>false</td>
  <td>
    If true, Spark cleans up its fallback storage data during shutting down.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.maxDiskSize</code></td>
  <td>(none)</td>
  <td>
    Maximum disk space to use to store shuffle blocks before rejecting remote shuffle blocks.
    Rejecting remote shuffle blocks means that an executor will not receive any shuffle migrations,
    and if there are no other executors available for migration then shuffle blocks will be lost unless
    <code>spark.storage.decommission.fallbackStorage.path</code> is configured.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version</code></td>
  <td>1</td>
  <td>
    The file output committer algorithm version, valid algorithm version number: 1 or 2.
    Note that 2 may cause a correctness issue like MAPREDUCE-7282.
  </td>
  <td>2.2.0</td>
</tr>
</table>

### Executor Metrics

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.eventLog.logStageExecutorMetrics</code></td>
  <td>false</td>
  <td>
    Whether to write per-stage peaks of executor metrics (for each executor) to the event log.
    <br />
    <em>Note:</em> The metrics are polled (collected) and sent in the executor heartbeat,
    and this is always done; this configuration is only to determine if aggregated metric peaks
    are written to the event log.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.processTreeMetrics.enabled</code></td>
  <td>false</td>
  <td>
    Whether to collect process tree metrics (from the /proc filesystem) when collecting
    executor metrics.
    <br />
    <em>Note:</em> The process tree metrics are collected only if the /proc filesystem
    exists.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.metrics.pollingInterval</code></td>
  <td>0</td>
  <td>
    How often to collect executor metrics (in milliseconds).
    <br />
    If 0, the polling is done on executor heartbeats (thus at the heartbeat interval,
    specified by <code>spark.executor.heartbeatInterval</code>).
    If positive, the polling is done at this interval.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.gcMetrics.youngGenerationGarbageCollectors</code></td>
  <td>Copy,PS Scavenge,ParNew,G1 Young Generation</td>
  <td>
    Names of supported young generation garbage collector. A name usually is the return of GarbageCollectorMXBean.getName.
    The built-in young generation garbage collectors are Copy,PS Scavenge,ParNew,G1 Young Generation.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.eventLog.gcMetrics.oldGenerationGarbageCollectors</code></td>
  <td>MarkSweepCompact,PS MarkSweep,ConcurrentMarkSweep,G1 Old Generation</td>
  <td>
    Names of supported old generation garbage collector. A name usually is the return of GarbageCollectorMXBean.getName.
    The built-in old generation garbage collectors are MarkSweepCompact,PS MarkSweep,ConcurrentMarkSweep,G1 Old Generation.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.metrics.fileSystemSchemes</code></td>
  <td>file,hdfs</td>
  <td>
    The file system schemes to report in executor metrics.
  </td>
  <td>3.1.0</td>
</tr>
</table>

### Networking

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.rpc.message.maxSize</code></td>
  <td>128</td>
  <td>
    Maximum message size (in MiB) to allow in "control plane" communication; generally only applies to map
    output size information sent between executors and the driver. Increase this if you are running
    jobs with many thousands of map and reduce tasks and see messages about the RPC message size.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.blockManager.port</code></td>
  <td>(random)</td>
  <td>
    Port for all block managers to listen on. These exist on both the driver and the executors.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.driver.blockManager.port</code></td>
  <td>(value of spark.blockManager.port)</td>
  <td>
    Driver-specific port for the block manager to listen on, for cases where it cannot use the same
    configuration as executors.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.driver.bindAddress</code></td>
  <td>(value of spark.driver.host)</td>
  <td>
    Hostname or IP address where to bind listening sockets. This config overrides the SPARK_LOCAL_IP
    environment variable (see below).

    <br />It also allows a different address from the local one to be advertised to executors or external systems.
    This is useful, for example, when running containers with bridged networking. For this to properly work,
    the different ports used by the driver (RPC, block manager and UI) need to be forwarded from the
    container's host.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.driver.host</code></td>
  <td>(local hostname)</td>
  <td>
    Hostname or IP address for the driver.
    This is used for communicating with the executors and the standalone Master.
  </td>
  <td>0.7.0</td>
</tr>
<tr>
  <td><code>spark.driver.port</code></td>
  <td>(random)</td>
  <td>
    Port for the driver to listen on.
    This is used for communicating with the executors and the standalone Master.
  </td>
  <td>0.7.0</td>
</tr>
<tr>
  <td><code>spark.rpc.io.backLog</code></td>
  <td>64</td>
  <td>
    Length of the accept queue for the RPC server. For large applications, this value may
    need to be increased, so that incoming connections are not dropped when a large number of
    connections arrives in a short period of time.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.network.timeout</code></td>
  <td>120s</td>
  <td>
    Default timeout for all network interactions. This config will be used in place of
    <code>spark.storage.blockManagerHeartbeatTimeoutMs</code>,
    <code>spark.shuffle.io.connectionTimeout</code>, <code>spark.rpc.askTimeout</code> or
    <code>spark.rpc.lookupTimeout</code> if they are not configured.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.network.timeoutInterval</code></td>
  <td>60s</td>
  <td>
    Interval for the driver to check and expire dead executors.
  </td>
  <td>1.3.2</td>
</tr>
<tr>
  <td><code>spark.network.io.preferDirectBufs</code></td>
  <td>true</td>
  <td>
    If enabled then off-heap buffer allocations are preferred by the shared allocators.
    Off-heap buffers are used to reduce garbage collection during shuffle and cache
    block transfer. For environments where off-heap memory is tightly limited, users may wish to
    turn this off to force all allocations to be on-heap.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.port.maxRetries</code></td>
  <td>16</td>
  <td>
    Maximum number of retries when binding to a port before giving up.
    When a port is given a specific value (non 0), each subsequent retry will
    increment the port used in the previous attempt by 1 before retrying. This
    essentially allows it to try a range of ports from the start port specified
    to port + maxRetries.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.rpc.askTimeout</code></td>
  <td><code>spark.network.timeout</code></td>
  <td>
    Duration for an RPC ask operation to wait before timing out.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.rpc.lookupTimeout</code></td>
  <td>120s</td>
  <td>
    Duration for an RPC remote endpoint lookup operation to wait before timing out.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.network.maxRemoteBlockSizeFetchToMem</code></td>
  <td>200m</td>
  <td>
    Remote block will be fetched to disk when size of the block is above this threshold
    in bytes. This is to avoid a giant request takes too much memory. Note this
    configuration will affect both shuffle fetch and block manager remote block fetch.
    For users who enabled external shuffle service, this feature can only work when
    external shuffle service is at least 2.3.0.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.rpc.io.connectionTimeout</code></td>
  <td>value of <code>spark.network.timeout</code></td>
  <td>
    Timeout for the established connections between RPC peers to be marked as idled and closed
    if there are outstanding RPC requests but no traffic on the channel for at least
    <code>connectionTimeout</code>.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.rpc.io.connectionCreationTimeout</code></td>
  <td>value of <code>spark.rpc.io.connectionTimeout</code></td>
  <td>
    Timeout for establishing a connection between RPC peers.
  </td>
  <td>3.2.0</td>
</tr>
</table>

### Scheduling

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.cores.max</code></td>
  <td>(not set)</td>
  <td>
    When running on a <a href="spark-standalone.html">standalone deploy cluster</a>, 
    the maximum amount of CPU cores to request for the application from
    across the cluster (not from each machine). If not set, the default will be
    <code>spark.deploy.defaultCores</code> on Spark's standalone cluster manager.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.locality.wait</code></td>
  <td>3s</td>
  <td>
    How long to wait to launch a data-local task before giving up and launching it
    on a less-local node. The same wait will be used to step through multiple locality levels
    (process-local, node-local, rack-local and then any). It is also possible to customize the
    waiting time for each level by setting <code>spark.locality.wait.node</code>, etc.
    You should increase this setting if your tasks are long and see poor locality, but the
    default usually works well.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.locality.wait.node</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for node locality. For example, you can set this to 0 to skip
    node locality and search immediately for rack locality (if your cluster has rack information).
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.locality.wait.process</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for process locality. This affects tasks that attempt to access
    cached data in a particular executor process.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.locality.wait.rack</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for rack locality.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.maxRegisteredResourcesWaitingTime</code></td>
  <td>30s</td>
  <td>
    Maximum amount of time to wait for resources to register before scheduling begins.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.scheduler.minRegisteredResourcesRatio</code></td>
  <td>0.8 for KUBERNETES mode; 0.8 for YARN mode; 0.0 for standalone mode</td>
  <td>
    The minimum ratio of registered resources (registered resources / total expected resources)
    (resources are executors in yarn mode and Kubernetes mode, CPU cores in standalone mode)
    to wait for before scheduling begins. Specified as a double between 0.0 and 1.0.
    Regardless of whether the minimum ratio of resources has been reached,
    the maximum amount of time it will wait before scheduling begins is controlled by config
    <code>spark.scheduler.maxRegisteredResourcesWaitingTime</code>.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.scheduler.mode</code></td>
  <td>FIFO</td>
  <td>
    The <a href="job-scheduling.html#scheduling-within-an-application">scheduling mode</a> between
    jobs submitted to the same SparkContext. Can be set to <code>FAIR</code>
    to use fair sharing instead of queueing jobs one after another. Useful for
    multi-user services.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.revive.interval</code></td>
  <td>1s</td>
  <td>
    The interval length for the scheduler to revive the worker resource offers to run tasks.
  </td>
  <td>0.8.1</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>10000</td>
  <td>
    The default capacity for event queues. Spark will try to initialize an event queue
    using capacity specified by <code>spark.scheduler.listenerbus.eventqueue.queueName.capacity</code>
    first. If it's not configured, Spark will use the default capacity specified by this
    config. Note that capacity must be greater than 0. Consider increasing value (e.g. 20000)
    if listener events are dropped. Increasing this value may result in the driver using more memory.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.shared.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>
    Capacity for shared event queue in Spark listener bus, which hold events for external listener(s)
    that register to the listener bus. Consider increasing value, if the listener events corresponding
    to shared queue are dropped. Increasing this value may result in the driver using more memory.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.appStatus.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>
    Capacity for appStatus event queue, which hold events for internal application status listeners.
    Consider increasing value, if the listener events corresponding to appStatus queue are dropped.
    Increasing this value may result in the driver using more memory.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.executorManagement.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>
    Capacity for executorManagement event queue in Spark listener bus, which hold events for internal
    executor management listeners. Consider increasing value if the listener events corresponding to
    executorManagement queue are dropped. Increasing this value may result in the driver using more memory.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.eventLog.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>
    Capacity for eventLog queue in Spark listener bus, which hold events for Event logging listeners
    that write events to eventLogs. Consider increasing value if the listener events corresponding to eventLog queue
    are dropped. Increasing this value may result in the driver using more memory.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.streams.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>
    Capacity for streams queue in Spark listener bus, which hold events for internal streaming listener.
    Consider increasing value if the listener events corresponding to streams queue are dropped. Increasing
    this value may result in the driver using more memory.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.resource.profileMergeConflicts</code></td>
  <td>false</td>
  <td>
    If set to "true", Spark will merge ResourceProfiles when different profiles are specified
    in RDDs that get combined into a single stage. When they are merged, Spark chooses the maximum of
    each resource and creates a new ResourceProfile. The default of false results in Spark throwing
    an exception if multiple different ResourceProfiles are found in RDDs going into the same stage.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout</code></td>
  <td>120s</td>
  <td>
    The timeout in seconds to wait to acquire a new executor and schedule a task before aborting a
    TaskSet which is unschedulable because all executors are excluded due to task failures.
  </td>
  <td>2.4.1</td>
</tr>
<tr>
  <td><code>spark.standalone.submit.waitAppCompletion</code></td>
  <td>false</td>
  <td>
    If set to true, Spark will merge ResourceProfiles when different profiles are specified in RDDs that get combined into a single stage.
    When they are merged, Spark chooses the maximum of each resource and creates a new ResourceProfile.
    The default of false results in Spark throwing an exception if multiple different ResourceProfiles are found in RDDs going into the same stage.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.enabled</code></td>
  <td>
    false
  </td>
  <td>
    If set to "true", prevent Spark from scheduling tasks on executors that have been excluded
    due to too many task failures. The algorithm used to exclude executors and nodes can be further
    controlled by the other "spark.excludeOnFailure" configuration options.
    This config will be overriden by "spark.excludeOnFailure.application.enabled" and 
    "spark.excludeOnFailure.taskAndStage.enabled" to specify exclusion enablement on individual
    levels.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.enabled</code></td>
  <td>
    false
  </td>
  <td>
    If set to "true", enables excluding executors for the entire application due to too many task
    failures and prevent Spark from scheduling tasks on them.
    This config overrides "spark.excludeOnFailure.enabled". 
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.taskAndStage.enabled</code></td>
  <td>
    false
  </td>
  <td>
    If set to "true", enables excluding executors on a task set level due to too many task
    failures and prevent Spark from scheduling tasks on them.
    This config overrides "spark.excludeOnFailure.enabled". 
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.timeout</code></td>
  <td>1h</td>
  <td>
    (Experimental) How long a node or executor is excluded for the entire application, before it
    is unconditionally removed from the excludelist to attempt running new tasks.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor</code></td>
  <td>1</td>
  <td>
    (Experimental) For a given task, how many times it can be retried on one executor before the
    executor is excluded for that task.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.task.maxTaskAttemptsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) For a given task, how many times it can be retried on one node, before the entire
    node is excluded for that task.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.stage.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different tasks must fail on one executor, within one stage, before the
    executor is excluded for that stage.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.stage.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different executors are marked as excluded for a given stage, before
    the entire node is marked as failed for the stage.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different tasks must fail on one executor, in successful task sets,
    before the executor is excluded for the entire application.  Excluded executors will
    be automatically added back to the pool of available resources after the timeout specified by
    <code>spark.excludeOnFailure.timeout</code>.  Note that with dynamic allocation, though, the executors
    may get marked as idle and be reclaimed by the cluster manager.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different executors must be excluded for the entire application,
    before the node is excluded for the entire application.  Excluded nodes will
    be automatically added back to the pool of available resources after the timeout specified by
    <code>spark.excludeOnFailure.timeout</code>.  Note that with dynamic allocation, though, the
    executors on the node may get marked as idle and be reclaimed by the cluster manager.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.killExcludedExecutors</code></td>
  <td>false</td>
  <td>
    (Experimental) If set to "true", allow Spark to automatically kill the executors
    when they are excluded on fetch failure or excluded for the entire application,
    as controlled by spark.killExcludedExecutors.application.*. Note that, when an entire node is added
    excluded, all of the executors on that node will be killed.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.fetchFailure.enabled</code></td>
  <td>false</td>
  <td>
    (Experimental) If set to "true", Spark will exclude the executor immediately when a fetch
    failure happens. If external shuffle service is enabled, then the whole node will be
    excluded.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.speculation</code></td>
  <td>false</td>
  <td>
    If set to "true", performs speculative execution of tasks. This means if one or more tasks are
    running slowly in a stage, they will be re-launched.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.speculation.interval</code></td>
  <td>100ms</td>
  <td>
    How often Spark will check for tasks to speculate.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.speculation.multiplier</code></td>
  <td>3</td>
  <td>
    How many times slower a task is than the median to be considered for speculation.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.speculation.quantile</code></td>
  <td>0.9</td>
  <td>
    Fraction of tasks which must be complete before speculation is enabled for a particular stage.
  </td>
  <td>0.6.0</td>
</tr>
<tr>
  <td><code>spark.speculation.minTaskRuntime</code></td>
  <td>100ms</td>
  <td>
    Minimum amount of time a task runs before being considered for speculation.
    This can be used to avoid launching speculative copies of tasks that are very short.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.speculation.task.duration.threshold</code></td>
  <td>None</td>
  <td>
    Task duration after which scheduler would try to speculative run the task. If provided, tasks
    would be speculatively run if current stage contains less tasks than or equal to the number of
    slots on a single executor and the task is taking longer time than the threshold. This config
    helps speculate stage with very few tasks. Regular speculation configs may also apply if the
    executor slots are large enough. E.g. tasks might be re-launched if there are enough successful
    runs even though the threshold hasn't been reached. The number of slots is computed based on
    the conf values of spark.executor.cores and spark.task.cpus minimum 1.
    Default unit is bytes, unless otherwise specified.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.processRateMultiplier</code></td>
  <td>0.75</td>
  <td>
    A multiplier that used when evaluating inefficient tasks. The higher the multiplier
    is, the more tasks will be possibly considered as inefficient.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.longRunTaskFactor</code></td>
  <td>2</td>
  <td>
    A task will be speculated anyway as long as its duration has exceeded the value of multiplying
    the factor and the time threshold (either be <code>spark.speculation.multiplier</code>
    * successfulTaskDurations.median or <code>spark.speculation.minTaskRuntime</code>) regardless
    of it's data process rate is good or not. This avoids missing the inefficient tasks when task
    slow isn't related to data process rate.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.enabled</code></td>
  <td>true</td>
  <td>
    When set to true, spark will evaluate the efficiency of task processing through the stage task
    metrics or its duration, and only need to speculate the inefficient tasks. A task is inefficient
    when 1)its data process rate is less than the average data process rate of all successful tasks
    in the stage multiplied by a multiplier or 2)its duration has exceeded the value of multiplying
     <code>spark.speculation.efficiency.longRunTaskFactor</code> and the time threshold (either be
     <code>spark.speculation.multiplier</code> * successfulTaskDurations.median or
    <code>spark.speculation.minTaskRuntime</code>).
  </td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.task.cpus</code></td>
  <td>1</td>
  <td>
    Number of cores to allocate for each task.
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><code>spark.task.resource.{resourceName}.amount</code></td>
  <td>1</td>
  <td>
    Amount of a particular resource type to allocate for each task, note that this can be a double.
    If this is specified you must also provide the executor config
    <code>spark.executor.resource.{resourceName}.amount</code> and any corresponding discovery configs
    so that your executors are created with that resource type. In addition to whole amounts,
    a fractional amount (for example, 0.25, which means 1/4th of a resource) may be specified.
    Fractional amounts must be less than or equal to 0.5, or in other words, the minimum amount of
    resource sharing is 2 tasks per resource. Additionally, fractional amounts are floored
    in order to assign resource slots (e.g. a 0.2222 configuration, or 1/0.2222 slots will become
    4 tasks/resource, not 5).
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.task.maxFailures</code></td>
  <td>4</td>
  <td>
    Number of continuous failures of any particular task before giving up on the job.
    The total number of failures spread across different tasks will not cause the job
    to fail; a particular task has to fail this number of attempts continuously.
    If any attempt succeeds, the failure count for the task will be reset.
    Should be greater than or equal to 1. Number of allowed retries = this value - 1.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.task.reaper.enabled</code></td>
  <td>false</td>
  <td>
    Enables monitoring of killed / interrupted tasks. When set to true, any task which is killed
    will be monitored by the executor until that task actually finishes executing. See the other
    <code>spark.task.reaper.*</code> configurations for details on how to control the exact behavior
    of this monitoring. When set to false (the default), task killing will use an older code
    path which lacks such monitoring.
  </td>
  <td>2.0.3</td>
</tr>
<tr>
  <td><code>spark.task.reaper.pollingInterval</code></td>
  <td>10s</td>
  <td>
    When <code>spark.task.reaper.enabled = true</code>, this setting controls the frequency at which
    executors will poll the status of killed tasks. If a killed task is still running when polled
    then a warning will be logged and, by default, a thread-dump of the task will be logged
    (this thread dump can be disabled via the <code>spark.task.reaper.threadDump</code> setting,
    which is documented below).
  </td>
  <td>2.0.3</td>
</tr>
<tr>
  <td><code>spark.task.reaper.threadDump</code></td>
  <td>true</td>
  <td>
    When <code>spark.task.reaper.enabled = true</code>, this setting controls whether task thread
    dumps are logged during periodic polling of killed tasks. Set this to false to disable
    collection of thread dumps.
  </td>
  <td>2.0.3</td>
</tr>
<tr>
  <td><code>spark.task.reaper.killTimeout</code></td>
  <td>-1</td>
  <td>
    When <code>spark.task.reaper.enabled = true</code>, this setting specifies a timeout after
    which the executor JVM will kill itself if a killed task has not stopped running. The default
    value, -1, disables this mechanism and prevents the executor from self-destructing. The purpose
    of this setting is to act as a safety-net to prevent runaway noncancellable tasks from rendering
    an executor unusable.
  </td>
  <td>2.0.3</td>
</tr>
<tr>
  <td><code>spark.stage.maxConsecutiveAttempts</code></td>
  <td>4</td>
  <td>
    Number of consecutive stage attempts allowed before a stage is aborted.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.stage.ignoreDecommissionFetchFailure</code></td>
  <td>true</td>
  <td>
    Whether ignore stage fetch failure caused by executor decommission when
    count <code>spark.stage.maxConsecutiveAttempts</code>
  </td>
  <td>3.4.0</td>
</tr>
</table>

### Barrier Execution Mode

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.barrier.sync.timeout</code></td>
  <td>365d</td>
  <td>
    The timeout in seconds for each <code>barrier()</code> call from a barrier task. If the
    coordinator didn't receive all the sync messages from barrier tasks within the
    configured time, throw a SparkException to fail all the tasks. The default value is set
    to 31536000(3600 * 24 * 365) so the <code>barrier()</code> call shall wait for one year.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.barrier.maxConcurrentTasksCheck.interval</code></td>
  <td>15s</td>
  <td>
    Time in seconds to wait between a max concurrent tasks check failure and the next
    check. A max concurrent tasks check ensures the cluster can launch more concurrent
    tasks than required by a barrier stage on job submitted. The check can fail in case
    a cluster has just started and not enough executors have registered, so we wait for a
    little while and try to perform the check again. If the check fails more than a
    configured max failure times for a job then fail current job submission. Note this
    config only applies to jobs that contain one or more barrier stages, we won't perform
    the check on non-barrier jobs.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures</code></td>
  <td>40</td>
  <td>
    Number of max concurrent tasks check failures allowed before fail a job submission.
    A max concurrent tasks check ensures the cluster can launch more concurrent tasks than
    required by a barrier stage on job submitted. The check can fail in case a cluster
    has just started and not enough executors have registered, so we wait for a little
    while and try to perform the check again. If the check fails more than a configured
    max failure times for a job then fail current job submission. Note this config only
    applies to jobs that contain one or more barrier stages, we won't perform the check on
    non-barrier jobs.
  </td>
  <td>2.4.0</td>
</tr>
</table>

### Dynamic Allocation

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.dynamicAllocation.enabled</code></td>
  <td>false</td>
  <td>
    Whether to use dynamic resource allocation, which scales the number of executors registered
    with this application up and down based on the workload.
    For more detail, see the description
    <a href="job-scheduling.html#dynamic-resource-allocation">here</a>.
    <br><br>
    This requires one of the following conditions: 
    1) enabling external shuffle service through <code>spark.shuffle.service.enabled</code>, or
    2) enabling shuffle tracking through <code>spark.dynamicAllocation.shuffleTracking.enabled</code>, or
    3) enabling shuffle blocks decommission through <code>spark.decommission.enabled</code> and <code>spark.storage.decommission.shuffleBlocks.enabled</code>, or
    4) (Experimental) configuring <code>spark.shuffle.sort.io.plugin.class</code> to use a custom <code>ShuffleDataIO</code> who's <code>ShuffleDriverComponents</code> supports reliable storage.
    The following configurations are also relevant:
    <code>spark.dynamicAllocation.minExecutors</code>,
    <code>spark.dynamicAllocation.maxExecutors</code>, and
    <code>spark.dynamicAllocation.initialExecutors</code>
    <code>spark.dynamicAllocation.executorAllocationRatio</code>
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.executorIdleTimeout</code></td>
  <td>60s</td>
  <td>
    If dynamic allocation is enabled and an executor has been idle for more than this duration,
    the executor will be removed. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.cachedExecutorIdleTimeout</code></td>
  <td>infinity</td>
  <td>
    If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration,
    the executor will be removed. For more details, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.initialExecutors</code></td>
  <td><code>spark.dynamicAllocation.minExecutors</code></td>
  <td>
    Initial number of executors to run if dynamic allocation is enabled.
    <br /><br />
    If <code>--num-executors</code> (or <code>spark.executor.instances</code>) is set and larger than this value, it will
    be used as the initial number of executors.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.maxExecutors</code></td>
  <td>infinity</td>
  <td>
    Upper bound for the number of executors if dynamic allocation is enabled.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.minExecutors</code></td>
  <td>0</td>
  <td>
    Lower bound for the number of executors if dynamic allocation is enabled.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.executorAllocationRatio</code></td>
  <td>1</td>
  <td>
    By default, the dynamic allocation will request enough executors to maximize the
    parallelism according to the number of tasks to process. While this minimizes the
    latency of the job, with small tasks this setting can waste a lot of resources due to
    executor allocation overhead, as some executor might not even do any work.
    This setting allows to set a ratio that will be used to reduce the number of
    executors w.r.t. full parallelism.
    Defaults to 1.0 to give maximum parallelism.
    0.5 will divide the target number of executors by 2
    The target number of executors computed by the dynamicAllocation can still be overridden
    by the <code>spark.dynamicAllocation.minExecutors</code> and
    <code>spark.dynamicAllocation.maxExecutors</code> settings
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.schedulerBacklogTimeout</code></td>
  <td>1s</td>
  <td>
    If dynamic allocation is enabled and there have been pending tasks backlogged for more than
    this duration, new executors will be requested. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.sustainedSchedulerBacklogTimeout</code></td>
  <td><code>schedulerBacklogTimeout</code></td>
  <td>
    Same as <code>spark.dynamicAllocation.schedulerBacklogTimeout</code>, but used only for
    subsequent executor requests. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.shuffleTracking.enabled</code></td>
  <td><code>true</code></td>
  <td>
    Enables shuffle file tracking for executors, which allows dynamic allocation
    without the need for an external shuffle service. This option will try to keep alive executors
    that are storing shuffle data for active jobs.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.shuffleTracking.timeout</code></td>
  <td><code>infinity</code></td>
  <td>
    When shuffle tracking is enabled, controls the timeout for executors that are holding shuffle
    data. The default value means that Spark will rely on the shuffles being garbage collected to be
    able to release executors. If for some reason garbage collection is not cleaning up shuffles
    quickly enough, this option can be used to control when to time out executors even when they are
    storing shuffle data.
  </td>
  <td>3.0.0</td>
</tr>
</table>

### Thread Configurations

Depending on jobs and cluster configurations, we can set number of threads in several places in Spark to utilize
available resources efficiently to get better performance. Prior to Spark 3.0, these thread configurations apply
to all roles of Spark, such as driver, executor, worker and master. From Spark 3.0, we can configure threads in
finer granularity starting from driver and executor. Take RPC module as example in below table. For other modules,
like shuffle, just replace "rpc" with "shuffle" in the property names except
<code>spark.{driver|executor}.rpc.netty.dispatcher.numThreads</code>, which is only for RPC module.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.{driver|executor}.rpc.io.serverThreads</code></td>
  <td>
    Fall back on <code>spark.rpc.io.serverThreads</code>
  </td>
  <td>Number of threads used in the server thread pool</td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.{driver|executor}.rpc.io.clientThreads</code></td>
  <td>
    Fall back on <code>spark.rpc.io.clientThreads</code>
  </td>
  <td>Number of threads used in the client thread pool</td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.{driver|executor}.rpc.netty.dispatcher.numThreads</code></td>
  <td>
    Fall back on <code>spark.rpc.netty.dispatcher.numThreads</code>
  </td>
  <td>Number of threads used in RPC message dispatcher thread pool</td>
  <td>3.0.0</td>
</tr>
</table>

The default value for number of thread-related config keys is the minimum of the number of cores requested for
the driver or executor, or, in the absence of that value, the number of cores available for the JVM (with a hardcoded upper limit of 8).

### Spark Connect

#### Server Configuration

Server configurations are set in Spark Connect server, for example, when you start the Spark Connect server with `./sbin/start-connect-server.sh`.
They are typically set via the config file and command-line options with `--conf/-c`.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.connect.grpc.binding.port</code></td>
  <td>
    15002
  </td>
  <td>Port for Spark Connect server to bind.</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.grpc.interceptor.classes</code></td>
  <td>
    (none)
  </td>
  <td>Comma separated list of class names that must implement the <code>io.grpc.ServerInterceptor</code> interface</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.grpc.arrow.maxBatchSize</code></td>
  <td>
    4m
  </td>
  <td>When using Apache Arrow, limit the maximum size of one arrow batch that can be sent from server side to client side. Currently, we conservatively use 70% of it because the size is not accurate but estimated.</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.grpc.maxInboundMessageSize</code></td>
  <td>
    134217728
  </td>
  <td>Sets the maximum inbound message size for the gRPC requests. Requests with a larger payload will fail.</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.extensions.relation.classes</code></td>
  <td>
    (none)
  </td>
  <td>Comma separated list of classes that implement the trait <code>org.apache.spark.sql.connect.plugin.RelationPlugin</code> to support custom
Relation types in proto.</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.extensions.expression.classes</code></td>
  <td>
    (none)
  </td>
  <td>Comma separated list of classes that implement the trait
<code>org.apache.spark.sql.connect.plugin.ExpressionPlugin</code> to support custom
Expression types in proto.</td>
  <td>3.4.0</td>
</tr>
<tr>
  <td><code>spark.connect.extensions.command.classes</code></td>
  <td>
    (none)
  </td>
  <td>Comma separated list of classes that implement the trait
<code>org.apache.spark.sql.connect.plugin.CommandPlugin</code> to support custom
Command types in proto.</td>
  <td>3.4.0</td>
</tr>
</table>

### Security

Please refer to the [Security](security.html) page for available options on how to secure different
Spark subsystems.


### Spark SQL

#### Runtime SQL Configuration

Runtime SQL configurations are per-session, mutable Spark SQL configurations. They can be set with initial values by the config file
and command-line options with `--conf/-c` prefixed, or by setting `SparkConf` that are used to create `SparkSession`.
Also, they can be set and queried by SET commands and reset to their initial values by RESET command,
or by `SparkSession.conf`'s setter and getter methods in runtime.

{% include_api_gen generated-runtime-sql-config-table.html %}

#### Static SQL Configuration

Static SQL configurations are cross-session, immutable Spark SQL configurations. They can be set with final values by the config file
and command-line options with `--conf/-c` prefixed, or by setting `SparkConf` that are used to create `SparkSession`.
External users can query the static sql config values via `SparkSession.conf` or via set command, e.g. `SET spark.sql.extensions;`, but cannot set/unset them.

{% include_api_gen generated-static-sql-config-table.html %}

### Spark Streaming

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.streaming.backpressure.enabled</code></td>
  <td>false</td>
  <td>
    Enables or disables Spark Streaming's internal backpressure mechanism (since 1.5).
    This enables the Spark Streaming to control the receiving rate based on the
    current batch scheduling delays and processing times so that the system receives
    only as fast as the system can process. Internally, this dynamically sets the
    maximum receiving rate of receivers. This rate is upper bounded by the values
    <code>spark.streaming.receiver.maxRate</code> and <code>spark.streaming.kafka.maxRatePerPartition</code>
    if they are set (see below).
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.streaming.backpressure.initialRate</code></td>
  <td>not set</td>
  <td>
    This is the initial maximum receiving rate at which each receiver will receive data for the
    first batch when the backpressure mechanism is enabled.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.streaming.blockInterval</code></td>
  <td>200ms</td>
  <td>
    Interval at which data received by Spark Streaming receivers is chunked
    into blocks of data before storing them in Spark. Minimum recommended - 50 ms. See the
    <a href="streaming-programming-guide.html#level-of-parallelism-in-data-receiving">performance
     tuning</a> section in the Spark Streaming programming guide for more details.
  </td>
  <td>0.8.0</td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.maxRate</code></td>
  <td>not set</td>
  <td>
    Maximum rate (number of records per second) at which each receiver will receive data.
    Effectively, each stream will consume at most this number of records per second.
    Setting this configuration to 0 or a negative number will put no limit on the rate.
    See the <a href="streaming-programming-guide.html#deploying-applications">deployment guide</a>
    in the Spark Streaming programming guide for mode details.
  </td>
  <td>1.0.2</td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.writeAheadLog.enable</code></td>
  <td>false</td>
  <td>
    Enable write-ahead logs for receivers. All the input data received through receivers
    will be saved to write-ahead logs that will allow it to be recovered after driver failures.
    See the <a href="streaming-programming-guide.html#deploying-applications">deployment guide</a>
    in the Spark Streaming programming guide for more details.
  </td>
  <td>1.2.1</td>
</tr>
<tr>
  <td><code>spark.streaming.unpersist</code></td>
  <td>true</td>
  <td>
    Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from
    Spark's memory. The raw input data received by Spark Streaming is also automatically cleared.
    Setting this to false will allow the raw data and persisted RDDs to be accessible outside the
    streaming application as they will not be cleared automatically. But it comes at the cost of
    higher memory usage in Spark.
  </td>
  <td>0.9.0</td>
</tr>
<tr>
  <td><code>spark.streaming.stopGracefullyOnShutdown</code></td>
  <td>false</td>
  <td>
    If <code>true</code>, Spark shuts down the <code>StreamingContext</code> gracefully on JVM
    shutdown rather than immediately.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.streaming.kafka.maxRatePerPartition</code></td>
  <td>not set</td>
  <td>
    Maximum rate (number of records per second) at which data will be read from each Kafka
    partition when using the new Kafka direct stream API. See the
    <a href="streaming-kafka-0-10-integration.html">Kafka Integration guide</a>
    for more details.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.streaming.kafka.minRatePerPartition</code></td>
  <td>1</td>
  <td>
    Minimum rate (number of records per second) at which data will be read from each Kafka
    partition when using the new Kafka direct stream API.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.streaming.ui.retainedBatches</code></td>
  <td>1000</td>
  <td>
    How many batches the Spark Streaming UI and status APIs remember before garbage collecting.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.streaming.driver.writeAheadLog.closeFileAfterWrite</code></td>
  <td>false</td>
  <td>
    Whether to close the file after writing a write-ahead log record on the driver. Set this to 'true'
    when you want to use S3 (or any file system that does not support flushing) for the metadata WAL
    on the driver.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.writeAheadLog.closeFileAfterWrite</code></td>
  <td>false</td>
  <td>
    Whether to close the file after writing a write-ahead log record on the receivers. Set this to 'true'
    when you want to use S3 (or any file system that does not support flushing) for the data WAL
    on the receivers.
  </td>
  <td>1.6.0</td>
</tr>
</table>

### SparkR

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.r.numRBackendThreads</code></td>
  <td>2</td>
  <td>
    Number of threads used by RBackend to handle RPC calls from SparkR package.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.r.command</code></td>
  <td>Rscript</td>
  <td>
    Executable for executing R scripts in cluster modes for both driver and workers.
  </td>
  <td>1.5.3</td>
</tr>
<tr>
  <td><code>spark.r.driver.command</code></td>
  <td>spark.r.command</td>
  <td>
    Executable for executing R scripts in client modes for driver. Ignored in cluster modes.
  </td>
  <td>1.5.3</td>
</tr>
<tr>
  <td><code>spark.r.shell.command</code></td>
  <td>R</td>
  <td>
    Executable for executing sparkR shell in client modes for driver. Ignored in cluster modes. It is the same as environment variable <code>SPARKR_DRIVER_R</code>, but take precedence over it.
    <code>spark.r.shell.command</code> is used for sparkR shell while <code>spark.r.driver.command</code> is used for running R script.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.r.backendConnectionTimeout</code></td>
  <td>6000</td>
  <td>
    Connection timeout set by R process on its connection to RBackend in seconds.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.r.heartBeatInterval</code></td>
  <td>100</td>
  <td>
    Interval for heartbeats sent from SparkR backend to R process to prevent connection timeout.
  </td>
  <td>2.1.0</td>
</tr>

</table>

### GraphX

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.graphx.pregel.checkpointInterval</code></td>
  <td>-1</td>
  <td>
    Checkpoint interval for graph and message in Pregel. It used to avoid stackOverflowError due to long lineage chains
  after lots of iterations. The checkpoint is disabled by default.
  </td>
  <td>2.2.0</td>
</tr>
</table>

### Cluster Managers

Each cluster manager in Spark has additional configuration options. Configurations
can be found on the pages for each mode:

#### [YARN](running-on-yarn.html#configuration)

#### [Kubernetes](running-on-kubernetes.html#configuration)

#### [Standalone Mode](spark-standalone.html#cluster-launch-scripts)

# Environment Variables

Certain Spark settings can be configured through environment variables, which are read from the
`conf/spark-env.sh` script in the directory where Spark is installed (or `conf/spark-env.cmd` on
Windows). In Standalone mode, this file can give machine specific information such as
hostnames. It is also sourced when running local Spark applications or submission scripts.

Note that `conf/spark-env.sh` does not exist by default when Spark is installed. However, you can
copy `conf/spark-env.sh.template` to create it. Make sure you make the copy executable.

The following variables can be set in `spark-env.sh`:


<table>
  <thead><tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr></thead>
  <tr>
    <td><code>JAVA_HOME</code></td>
    <td>Location where Java is installed (if it's not on your default <code>PATH</code>).</td>
  </tr>
  <tr>
    <td><code>PYSPARK_PYTHON</code></td>
    <td>Python binary executable to use for PySpark in both driver and workers (default is <code>python3</code> if available, otherwise <code>python</code>).
    Property <code>spark.pyspark.python</code> take precedence if it is set</td>
  </tr>
  <tr>
    <td><code>PYSPARK_DRIVER_PYTHON</code></td>
    <td>Python binary executable to use for PySpark in driver only (default is <code>PYSPARK_PYTHON</code>).
    Property <code>spark.pyspark.driver.python</code> take precedence if it is set</td>
  </tr>
  <tr>
    <td><code>SPARKR_DRIVER_R</code></td>
    <td>R binary executable to use for SparkR shell (default is <code>R</code>).
    Property <code>spark.r.shell.command</code> take precedence if it is set</td>
  </tr>
  <tr>
    <td><code>SPARK_LOCAL_IP</code></td>
    <td>IP address of the machine to bind to.</td>
  </tr>
  <tr>
    <td><code>SPARK_PUBLIC_DNS</code></td>
    <td>Hostname your Spark program will advertise to other machines.</td>
  </tr>
</table>

In addition to the above, there are also options for setting up the Spark
[standalone cluster scripts](spark-standalone.html#cluster-launch-scripts), such as number of cores
to use on each machine and maximum memory.

Since `spark-env.sh` is a shell script, some of these can be set programmatically -- for example, you might
compute `SPARK_LOCAL_IP` by looking up the IP of a specific network interface.

Note: When running Spark on YARN in `cluster` mode, environment variables need to be set using the `spark.yarn.appMasterEnv.[EnvironmentVariableName]` property in your `conf/spark-defaults.conf` file.  Environment variables that are set in `spark-env.sh` will not be reflected in the YARN Application Master process in `cluster` mode.  See the [YARN-related Spark Properties](running-on-yarn.html#spark-properties) for more information.

# Configuring Logging

Spark uses [log4j](http://logging.apache.org/log4j/) for logging. You can configure it by adding a
`log4j2.properties` file in the `conf` directory. One way to start is to copy the existing templates `log4j2.properties.template` or `log4j2.properties.pattern-layout-template` located there.

## Structured Logging
Starting from version 4.0.0, `spark-submit` has adopted the [JSON Template Layout](https://logging.apache.org/log4j/2.x/manual/json-template-layout.html) for logging, which outputs logs in JSON format. This format facilitates querying logs using Spark SQL with the JSON data source. Additionally, the logs include all Mapped Diagnostic Context (MDC) information for search and debugging purposes.

To configure the layout of structured logging, start with the `log4j2.properties.template` file.

To query Spark logs using Spark SQL, you can use the following Python code snippet:

```python
from pyspark.util import LogUtils

logDf = spark.read.schema(LogUtils.LOG_SCHEMA).json("path/to/logs")
```

Or using the following Scala code snippet:
```scala
import org.apache.spark.util.LogUtils.LOG_SCHEMA

val logDf = spark.read.schema(LOG_SCHEMA).json("path/to/logs")
```

## Plain Text Logging
If you prefer plain text logging, you have two options:
- Disable structured JSON logging by setting the Spark configuration `spark.log.structuredLogging.enabled` to `false`.
- Use a custom log4j configuration file. Rename `conf/log4j2.properties.pattern-layout-template` to `conf/log4j2.properties`. This reverts to the default configuration prior to Spark 4.0, which utilizes [PatternLayout](https://logging.apache.org/log4j/2.x/manual/layouts.html#PatternLayout) for logging all messages in plain text.

MDC information is not included by default when with plain text logging. In order to print it in the logs, you can update the patternLayout in the file. For example, you can add `%X{task_name}` to print the task name in the logs.
Moreover, you can use `spark.sparkContext.setLocalProperty(s"mdc.$name", "value")` to add user specific data into MDC.
The key in MDC will be the string of `mdc.$name`.

# Overriding configuration directory

To specify a different configuration directory other than the default "SPARK_HOME/conf",
you can set SPARK_CONF_DIR. Spark will use the configuration files (spark-defaults.conf, spark-env.sh, log4j2.properties, etc)
from this directory.

# Inheriting Hadoop Cluster Configuration

If you plan to read and write from HDFS using Spark, there are two Hadoop configuration files that
should be included on Spark's classpath:

* `hdfs-site.xml`, which provides default behaviors for the HDFS client.
* `core-site.xml`, which sets the default filesystem name.

The location of these configuration files varies across Hadoop versions, but
a common location is inside of `/etc/hadoop/conf`. Some tools create
configurations on-the-fly, but offer a mechanism to download copies of them.

To make these files visible to Spark, set `HADOOP_CONF_DIR` in `$SPARK_HOME/conf/spark-env.sh`
to a location containing the configuration files.

# Custom Hadoop/Hive Configuration

If your Spark application is interacting with Hadoop, Hive, or both, there are probably Hadoop/Hive
configuration files in Spark's classpath.

Multiple running applications might require different Hadoop/Hive client side configurations.
You can copy and modify `hdfs-site.xml`, `core-site.xml`, `yarn-site.xml`, `hive-site.xml` in
Spark's classpath for each application. In a Spark cluster running on YARN, these configuration
files are set cluster-wide, and cannot safely be changed by the application.

The better choice is to use spark hadoop properties in the form of `spark.hadoop.*`, and use
spark hive properties in the form of `spark.hive.*`.
For example, adding configuration "spark.hadoop.abc.def=xyz" represents adding hadoop property "abc.def=xyz",
and adding configuration "spark.hive.abc=xyz" represents adding hive property "hive.abc=xyz".
They can be considered as same as normal spark properties which can be set in `$SPARK_HOME/conf/spark-defaults.conf`

In some cases, you may want to avoid hard-coding certain configurations in a `SparkConf`. For
instance, Spark allows you to simply create an empty conf and set spark/spark hadoop/spark hive properties.

{% highlight scala %}
val conf = new SparkConf().set("spark.hadoop.abc.def", "xyz")
val sc = new SparkContext(conf)
{% endhighlight %}

Also, you can modify or add configurations at runtime:
{% highlight bash %}
./bin/spark-submit \
  --name "My app" \
  --master "local[4]" \
  --conf spark.eventLog.enabled=false \
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  --conf spark.hadoop.abc.def=xyz \
  --conf spark.hive.abc=xyz
  myApp.jar
{% endhighlight %}

# Custom Resource Scheduling and Configuration Overview

GPUs and other accelerators have been widely used for accelerating special workloads, e.g.,
deep learning and signal processing. Spark now supports requesting and scheduling generic resources, such as GPUs, with a few caveats. The current implementation requires that the resource have addresses that can be allocated by the scheduler. It requires your cluster manager to support and be properly configured with the resources.

There are configurations available to request resources for the driver: `spark.driver.resource.{resourceName}.amount`, request resources for the executor(s): `spark.executor.resource.{resourceName}.amount` and specify the requirements for each task: `spark.task.resource.{resourceName}.amount`. The `spark.driver.resource.{resourceName}.discoveryScript` config is required on YARN, Kubernetes and a client side Driver on Spark Standalone. `spark.executor.resource.{resourceName}.discoveryScript` config is required for YARN and Kubernetes. Kubernetes also requires `spark.driver.resource.{resourceName}.vendor` and/or `spark.executor.resource.{resourceName}.vendor`. See the config descriptions above for more information on each.

Spark will use the configurations specified to first request containers with the corresponding resources from the cluster manager. Once it gets the container, Spark launches an Executor in that container which will discover what resources the container has and the addresses associated with each resource. The Executor will register with the Driver and report back the resources available to that Executor. The Spark scheduler can then schedule tasks to each Executor and assign specific resource addresses based on the resource requirements the user specified. The user can see the resources assigned to a task using the `TaskContext.get().resources` api. On the driver, the user can see the resources assigned with the SparkContext `resources` call. It's then up to the user to use the assigned addresses to do the processing they want or pass those into the ML/AI framework they are using.

See your cluster manager specific page for requirements and details on each of - [YARN](running-on-yarn.html#resource-allocation-and-configuration-overview), [Kubernetes](running-on-kubernetes.html#resource-allocation-and-configuration-overview) and [Standalone Mode](spark-standalone.html#resource-allocation-and-configuration-overview). It is currently not available with local mode. And please also note that local-cluster mode with multiple workers is not supported(see Standalone documentation).

# Stage Level Scheduling Overview

The stage level scheduling feature allows users to specify task and executor resource requirements at the stage level. This allows for different stages to run with executors that have different resources. A prime example of this is one ETL stage runs with executors with just CPUs, the next stage is an ML stage that needs GPUs. Stage level scheduling allows for user to request different executors that have GPUs when the ML stage runs rather then having to acquire executors with GPUs at the start of the application and them be idle while the ETL stage is being run.
This is only available for the RDD API in Scala, Java, and Python.  It is available on YARN, Kubernetes and Standalone when dynamic allocation is enabled. When dynamic allocation is disabled, it allows users to specify different task resource requirements at stage level, and this is supported on YARN, Kubernetes and Standalone cluster right now. See the [YARN](running-on-yarn.html#stage-level-scheduling-overview) page or [Kubernetes](running-on-kubernetes.html#stage-level-scheduling-overview) page or [Standalone](spark-standalone.html#stage-level-scheduling-overview) page for more implementation details.

See the `RDD.withResources` and `ResourceProfileBuilder` API's for using this feature. When dynamic allocation is disabled, tasks with different task resource requirements will share executors with `DEFAULT_RESOURCE_PROFILE`. While when dynamic allocation is enabled, the current implementation acquires new executors for each `ResourceProfile`  created and currently has to be an exact match. Spark does not try to fit tasks into an executor that require a different ResourceProfile than the executor was created with. Executors that are not in use will idle timeout with the dynamic allocation logic. The default configuration for this feature is to only allow one ResourceProfile per stage. If the user associates more then 1 ResourceProfile to an RDD, Spark will throw an exception by default. See config `spark.scheduler.resource.profileMergeConflicts` to control that behavior. The current merge strategy Spark implements when `spark.scheduler.resource.profileMergeConflicts` is enabled is a simple max of each resource within the conflicting ResourceProfiles. Spark will create a new ResourceProfile with the max of each of the resources.

# Push-based shuffle overview

Push-based shuffle helps improve the reliability and performance of spark shuffle. It takes a best-effort approach to push the shuffle blocks generated by the map tasks to remote external shuffle services to be merged per shuffle partition. Reduce tasks fetch a combination of merged shuffle partitions and original shuffle blocks as their input data, resulting in converting small random disk reads by external shuffle services into large sequential reads. Possibility of better data locality for reduce tasks additionally helps minimize network IO. Push-based shuffle takes priority over batch fetch for some scenarios, like partition coalesce when merged output is available.

<p> Push-based shuffle improves performance for long running jobs/queries which involves large disk I/O during shuffle. Currently it is not well suited for jobs/queries which runs quickly dealing with lesser amount of shuffle data. This will be further improved in the future releases.</p>

<p> <b> Currently push-based shuffle is only supported for Spark on YARN with external shuffle service. </b></p>

### External Shuffle service(server) side configuration options

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.push.server.mergedShuffleFileManagerImpl</code></td>
  <td>
    <code>org.apache.spark.network.shuffle.<br />NoOpMergedShuffleFileManager</code>
  </td>
  <td>
    Class name of the implementation of <code>MergedShuffleFileManager</code> that manages push-based shuffle. This acts as a server side config to disable or enable push-based shuffle. By default, push-based shuffle is disabled at the server side. <p> To enable push-based shuffle on the server side, set this config to <code>org.apache.spark.network.shuffle.RemoteBlockPushResolver</code></p>
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.server.minChunkSizeInMergedShuffleFile</code></td>
  <td><code>2m</code></td>
  <td>
    <p> The minimum size of a chunk when dividing a merged shuffle file into multiple chunks during push-based shuffle. A merged shuffle file consists of multiple small shuffle blocks. Fetching the complete merged shuffle file in a single disk I/O increases the memory requirements for both the clients and the external shuffle services. Instead, the external shuffle service serves the merged file in <code>MB-sized chunks</code>.<br /> This configuration controls how big a chunk can get. A corresponding index file for each merged shuffle file will be generated indicating chunk boundaries. </p>
    <p> Setting this too high would increase the memory requirements on both the clients and the external shuffle service. </p>
    <p> Setting this too low would increase the overall number of RPC requests to external shuffle service unnecessarily.</p>
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.server.mergedIndexCacheSize</code></td>
  <td><code>100m</code></td>
  <td>
    The maximum size of cache in memory which could be used in push-based shuffle for storing merged index files. This cache is in addition to the one configured via <code>spark.shuffle.service.index.cache.size</code>.
  </td>
  <td>3.2.0</td>
</tr>
</table>

### Client side configuration options

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.push.enabled</code></td>
  <td><code>false</code></td>
  <td>
    Set to true to enable push-based shuffle on the client side and works in conjunction with the server side flag <code>spark.shuffle.push.server.mergedShuffleFileManagerImpl</code>.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.finalize.timeout</code></td>
  <td><code>10s</code></td>
  <td>
    The amount of time driver waits in seconds, after all mappers have finished for a given shuffle map stage, before it sends merge finalize requests to remote external shuffle services. This gives the external shuffle services extra time to merge blocks. Setting this too long could potentially lead to performance regression.
  </td>
 <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxRetainedMergerLocations</code></td>
  <td><code>500</code></td>
  <td>
    Maximum number of merger locations cached for push-based shuffle. Currently, merger locations are hosts of external shuffle services responsible for handling pushed blocks, merging them and serving merged blocks for later shuffle fetch.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinThresholdRatio</code></td>
  <td><code>0.05</code></td>
  <td>
    Ratio used to compute the minimum number of shuffle merger locations required for a stage based on the number of partitions for the reducer stage. For example, a reduce stage which has 100 partitions and uses the default value 0.05 requires at least 5 unique merger locations to enable push-based shuffle.
  </td>
 <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinStaticThreshold</code></td>
  <td><code>5</code></td>
  <td>
    The static threshold for number of shuffle push merger locations should be available in order to enable push-based shuffle for a stage. Note this config works in conjunction with <code>spark.shuffle.push.mergersMinThresholdRatio</code>. Maximum of <code>spark.shuffle.push.mergersMinStaticThreshold</code> and <code>spark.shuffle.push.mergersMinThresholdRatio</code> ratio number of mergers needed to enable push-based shuffle for a stage. For example: with 1000 partitions for the child stage with spark.shuffle.push.mergersMinStaticThreshold as 5 and spark.shuffle.push.mergersMinThresholdRatio set to 0.05, we would need at least 50 mergers to enable push-based shuffle for that stage.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.numPushThreads</code></td>
  <td>(none)</td>
  <td>
    Specify the number of threads in the block pusher pool. These threads assist in creating connections and pushing blocks to remote external shuffle services.
    By default, the threadpool size is equal to the number of spark executor cores.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockSizeToPush</code></td>
  <td><code>1m</code></td>
  <td>
    <p> The max size of an individual block to push to the remote external shuffle services. Blocks larger than this threshold are not pushed to be merged remotely. These shuffle blocks will be fetched in the original manner. </p>
    <p> Setting this too high would result in more blocks to be pushed to remote external shuffle services but those are already efficiently fetched with the existing mechanisms resulting in additional overhead of pushing the large blocks to remote external shuffle services. It is recommended to set <code>spark.shuffle.push.maxBlockSizeToPush</code> lesser than <code>spark.shuffle.push.maxBlockBatchSize</code> config's value. </p>
    <p> Setting this too low would result in lesser number of blocks getting merged and directly fetched from mapper external shuffle service results in higher small random reads affecting overall disk I/O performance. </p>
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockBatchSize</code></td>
  <td><code>3m</code></td>
  <td>
    The max size of a batch of shuffle blocks to be grouped into a single push request. Default is set to <code>3m</code> in order to keep it slightly higher than <code>spark.storage.memoryMapThreshold</code> default which is <code>2m</code> as it is very likely that each batch of block gets memory mapped which incurs higher overhead.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.merge.finalizeThreads</code></td>
  <td>8</td>
  <td>
    Number of threads used by driver to finalize shuffle merge. Since it could potentially take seconds for a large shuffle to finalize,
    having multiple threads helps driver to handle concurrent shuffle merge finalize requests when push-based shuffle is enabled.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.minShuffleSizeToWait</code></td>
  <td><code>500m</code></td>
  <td>
    Driver will wait for merge finalization to complete only if total shuffle data size is more than this threshold. If total shuffle size is less, driver will immediately finalize the shuffle output.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.minCompletedPushRatio</code></td>
  <td><code>1.0</code></td>
  <td>
    Fraction of minimum map partitions that should be push complete before driver starts shuffle merge finalization during push based shuffle.
  </td>
  <td>3.3.0</td>
</tr>
</table>
