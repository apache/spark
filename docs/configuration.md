---
layout: global
displayTitle: Spark Configuration
title: Configuration
---
* This will become a table of contents (this text will be scraped).
{:toc}

Spark provides three locations to configure the system:

* [Spark properties](#spark-properties) control most application parameters and can be set by using
  a [SparkConf](api/scala/index.html#org.apache.spark.SparkConf) object, or through Java
  system properties.
* [Environment variables](#environment-variables) can be used to set per-machine settings, such as
  the IP address, through the `conf/spark-env.sh` script on each node.
* [Logging](#configuring-logging) can be configured through `log4j.properties`.

# Spark Properties

Spark properties control most application settings and are configured separately for each
application. These properties can be set directly on a
[SparkConf](api/scala/index.html#org.apache.spark.SparkConf) passed to your
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
{% highlight bash %}
./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar
{% endhighlight %}

The Spark shell and [`spark-submit`](submitting-applications.html)
tool support two ways to load configurations dynamically. The first is command line options,
such as `--master`, as shown above. `spark-submit` can accept any Spark property using the `--conf/-c`
flag, but uses special flags for properties that play a part in launching the Spark application.
Running `./bin/spark-submit --help` will show the entire list of these options.

`bin/spark-submit` will also read configuration options from `conf/spark-defaults.conf`, in which
each line consists of a key and a value separated by whitespace. For example:

    spark.master            spark://5.6.7.8:7077
    spark.executor.memory   4g
    spark.eventLog.enabled  true
    spark.serializer        org.apache.spark.serializer.KryoSerializer

Any values specified as flags or in the properties file will be passed on to the application
and merged with those specified through SparkConf. Properties set directly on the SparkConf
take highest precedence, then flags passed to `spark-submit` or `spark-shell`, then options
in the `spark-defaults.conf` file. A few configuration keys have been renamed since earlier
versions of Spark; in such cases, the older key names are still accepted, but take lower
precedence than any instance of the newer key.

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

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.app.name</code></td>
  <td>(none)</td>
  <td>
    The name of your application. This will appear in the UI and in log data.
  </td>
</tr>
<tr>
  <td><code>spark.driver.cores</code></td>
  <td>1</td>
  <td>
    Number of cores to use for the driver process, only in cluster mode.
  </td>
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
</tr>
<tr>
  <td><code>spark.driver.memoryOverhead</code></td>
  <td>driverMemory * 0.10, with minimum of 384 </td>
  <td>
    The amount of off-heap memory to be allocated per driver in cluster mode, in MiB unless
    otherwise specified. This is memory that accounts for things like VM overheads, interned strings, 
    other native overheads, etc. This tends to grow with the container size (typically 6-10%). 
    This option is currently supported on YARN and Kubernetes.
  </td>
</tr>
<tr>
  <td><code>spark.executor.memory</code></td>
  <td>1g</td>
  <td>
    Amount of memory to use per executor process, in the same format as JVM memory strings with
    a size unit suffix ("k", "m", "g" or "t") (e.g. <code>512m</code>, <code>2g</code>).
  </td>
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

    NOTE: Python memory usage may not be limited on platforms that do not support resource limiting, such as Windows.
  </td>
</tr>
<tr>
 <td><code>spark.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.10, with minimum of 384 </td>
  <td>
    The amount of off-heap memory to be allocated per executor, in MiB unless otherwise specified.
    This is memory that accounts for things like VM overheads, interned strings, other native 
    overheads, etc. This tends to grow with the executor size (typically 6-10%).
    This option is currently supported on YARN and Kubernetes.
  </td>
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
</tr>
<tr>
  <td><code>spark.local.dir</code></td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get
    stored on disk. This should be on a fast, local disk in your system. It can also be a
    comma-separated list of multiple directories on different disks.

    NOTE: In Spark 1.0 and later this will be overridden by SPARK_LOCAL_DIRS (Standalone), MESOS_SANDBOX (Mesos) or
    LOCAL_DIRS (YARN) environment variables set by the cluster manager.
  </td>
</tr>
<tr>
  <td><code>spark.logConf</code></td>
  <td>false</td>
  <td>
    Logs the effective SparkConf as INFO when a SparkContext is started.
  </td>
</tr>
<tr>
  <td><code>spark.master</code></td>
  <td>(none)</td>
  <td>
    The cluster manager to connect to. See the list of
    <a href="submitting-applications.html#master-urls"> allowed master URL's</a>.
  </td>
</tr>
<tr>
  <td><code>spark.submit.deployMode</code></td>
  <td>(none)</td>
  <td>
    The deploy mode of Spark driver program, either "client" or "cluster",
    Which means to launch driver program locally ("client")
    or remotely ("cluster") on one of the nodes inside the cluster.
  </td>
</tr>
<tr>
  <td><code>spark.log.callerContext</code></td>
  <td>(none)</td>
  <td>
    Application information that will be written into Yarn RM log/HDFS audit log when running on Yarn/HDFS.
    Its length depends on the Hadoop configuration <code>hadoop.caller.context.max.size</code>. It should be concise,
    and typically can have up to 50 characters.
  </td>
</tr>
<tr>
  <td><code>spark.driver.supervise</code></td>
  <td>false</td>
  <td>
    If true, restarts the driver automatically if it fails with a non-zero exit status.
    Only has effect in Spark standalone mode or Mesos cluster deploy mode.
  </td>
</tr>
</table>

Apart from these, the following properties are also available, and may be useful in some situations:

### Runtime Environment

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
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
</tr>
<tr>
  <td><code>spark.driver.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of extra JVM options to pass to the driver. For instance, GC settings or other logging.
    Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap
    size settings can be set with <code>spark.driver.memory</code> in the cluster mode and through
    the <code>--driver-memory</code> command line option in the client mode.

    <br /><em>Note:</em> In client mode, this config must not be set through the <code>SparkConf</code>
    directly in your application, because the driver JVM has already started at that point.
    Instead, please set this through the <code>--driver-java-options</code> command line option or in
    your default properties file.
  </td>
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
</tr>
<tr>
  <td><code>spark.executor.extraClassPath</code></td>
  <td>(none)</td>
  <td>
    Extra classpath entries to prepend to the classpath of executors. This exists primarily for
    backwards-compatibility with older versions of Spark. Users typically should not need to set
    this option.
  </td>
</tr>
<tr>
  <td><code>spark.executor.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of extra JVM options to pass to executors. For instance, GC settings or other logging.
    Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this
    option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file
    used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory.

    The following symbols, if present will be interpolated: {{APP_ID}} will be replaced by
    application ID and {{EXECUTOR_ID}} will be replaced by executor ID. For example, to enable
    verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of:
    <code>-verbose:gc -Xloggc:/tmp/{{APP_ID}}-{{EXECUTOR_ID}}.gc</code>
  </td>
</tr>
<tr>
  <td><code>spark.executor.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>
    Set a special library path to use when launching executor JVM's.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxRetainedFiles</code></td>
  <td>(none)</td>
  <td>
    Sets the number of latest rolling log files that are going to be retained by the system.
    Older log files will be deleted. Disabled by default.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.enableCompression</code></td>
  <td>false</td>
  <td>
    Enable executor log compression. If it is enabled, the rolled executor logs will be compressed.
    Disabled by default.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxSize</code></td>
  <td>(none)</td>
  <td>
    Set the max size of the file in bytes by which the executor logs will be rolled over.
    Rolling is disabled by default. See <code>spark.executor.logs.rolling.maxRetainedFiles</code>
    for automatic cleaning of old logs.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.strategy</code></td>
  <td>(none)</td>
  <td>
    Set the strategy of rolling of executor logs. By default it is disabled. It can
    be set to "time" (time-based rolling) or "size" (size-based rolling). For "time",
    use <code>spark.executor.logs.rolling.time.interval</code> to set the rolling interval.
    For "size", use <code>spark.executor.logs.rolling.maxSize</code> to set
    the maximum file size for rolling.
  </td>
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
</tr>
<tr>
  <td><code>spark.executor.userClassPathFirst</code></td>
  <td>false</td>
  <td>
    (Experimental) Same functionality as <code>spark.driver.userClassPathFirst</code>, but
    applied to executor instances.
  </td>
</tr>
<tr>
  <td><code>spark.executorEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>
    Add the environment variable specified by <code>EnvironmentVariableName</code> to the Executor
    process. The user can specify multiple of these to set multiple environment variables.
  </td>
</tr>
<tr>
  <td><code>spark.redaction.regex</code></td>
  <td>(?i)secret|password</td>
  <td>
    Regex to decide which Spark configuration properties and environment variables in driver and
    executor environments contain sensitive information. When this regex matches a property key or
    value, the value is redacted from the environment UI and various logs like YARN and event logs.
  </td>
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
</tr>
<tr>
  <td><code>spark.python.worker.reuse</code></td>
  <td>true</td>
  <td>
    Reuse Python worker or not. If yes, it will use a fixed number of Python workers,
    does not need to fork() a Python process for every task. It will be very useful
    if there is large broadcast, then the broadcast will not be needed to transferred
    from JVM to Python worker for every task.
  </td>
</tr>
<tr>
  <td><code>spark.files</code></td>
  <td></td>
  <td>
    Comma-separated list of files to be placed in the working directory of each executor. Globs are allowed.
  </td>
</tr>
<tr>
  <td><code>spark.submit.pyFiles</code></td>
  <td></td>
  <td>
    Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Globs are allowed.
  </td>
</tr>
<tr>
  <td><code>spark.jars</code></td>
  <td></td>
  <td>
    Comma-separated list of jars to include on the driver and executor classpaths. Globs are allowed.
  </td>
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
</tr>
<tr>
  <td><code>spark.jars.excludes</code></td>
  <td></td>
  <td>
    Comma-separated list of groupId:artifactId, to exclude while resolving the dependencies
    provided in <code>spark.jars.packages</code> to avoid dependency conflicts.
  </td>
</tr>
<tr>
  <td><code>spark.jars.ivy</code></td>
  <td></td>
  <td>
    Path to specify the Ivy user directory, used for the local Ivy cache and package files from
    <code>spark.jars.packages</code>. This will override the Ivy property <code>ivy.default.ivy.user.dir</code>
    which defaults to ~/.ivy2.
  </td>
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
    found at <a href="http://ant.apache.org/ivy/history/latest-milestone/settings.html">Settings Files</a>
  </td>
</tr>
 <tr>
  <td><code>spark.jars.repositories</code></td>
  <td></td>
  <td>
    Comma-separated list of additional remote repositories to search for the maven coordinates
    given with <code>--packages</code> or <code>spark.jars.packages</code>.
  </td>
</tr>
<tr>
  <td><code>spark.pyspark.driver.python</code></td>
  <td></td>
  <td>
    Python binary executable to use for PySpark in driver.
    (default is <code>spark.pyspark.python</code>)
  </td>
</tr>
<tr>
  <td><code>spark.pyspark.python</code></td>
  <td></td>
  <td>
    Python binary executable to use for PySpark in both driver and executors.
  </td>
</tr>
</table>

### Shuffle Behavior

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.reducer.maxSizeInFlight</code></td>
  <td>48m</td>
  <td>
    Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless 
    otherwise specified. Since each output requires us to create a buffer to receive it, this 
    represents a fixed memory overhead per reduce task, so keep it small unless you have a
    large amount of memory.
  </td>
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
</tr>
<tr>
  <td><code>spark.maxRemoteBlockSizeFetchToMem</code></td>
  <td>Int.MaxValue - 512</td>
  <td>
    The remote block will be fetched to disk when size of the block is above this threshold in bytes.
    This is to avoid a giant request that takes too much memory.  By default, this is only enabled
    for blocks > 2GB, as those cannot be fetched directly into memory, no matter what resources are
    available.  But it can be turned down to a much lower value (eg. 200m) to avoid using too much
    memory on smaller blocks as well. Note this configuration will affect both shuffle fetch
    and block manager remote block fetch. For users who enabled external shuffle service,
    this feature can only be used when external shuffle service is newer than Spark 2.2.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress map output files. Generally a good idea. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.file.buffer</code></td>
  <td>32k</td>
  <td>
    Size of the in-memory buffer for each shuffle file output stream, in KiB unless otherwise 
    specified. These buffers reduce the number of disk seeks and system calls made in creating 
    intermediate shuffle files.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.io.maxRetries</code></td>
  <td>3</td>
  <td>
    (Netty only) Fetches that fail due to IO-related exceptions are automatically retried if this is
    set to a non-zero value. This retry logic helps stabilize large shuffles in the face of long GC
    pauses or transient network connectivity issues.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.io.numConnectionsPerPeer</code></td>
  <td>1</td>
  <td>
    (Netty only) Connections between hosts are reused in order to reduce connection buildup for
    large clusters. For clusters with many hard disks and few hosts, this may result in insufficient
    concurrency to saturate all disks, and so users may consider increasing this value.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.io.preferDirectBufs</code></td>
  <td>true</td>
  <td>
    (Netty only) Off-heap buffers are used to reduce garbage collection during shuffle and cache
    block transfer. For environments where off-heap memory is tightly limited, users may wish to
    turn this off to force all allocations from Netty to be on-heap.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.io.retryWait</code></td>
  <td>5s</td>
  <td>
    (Netty only) How long to wait between retries of fetches. The maximum delay caused by retrying
    is 15 seconds by default, calculated as <code>maxRetries * retryWait</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.service.enabled</code></td>
  <td>false</td>
  <td>
    Enables the external shuffle service. This service preserves the shuffle files written by
    executors so the executors can be safely removed. This must be enabled if
    <code>spark.dynamicAllocation.enabled</code> is "true". The external shuffle service
    must be set up in order to enable it. See
    <a href="job-scheduling.html#configuration-and-setup">dynamic allocation
    configuration and setup documentation</a> for more information.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.service.port</code></td>
  <td>7337</td>
  <td>
    Port on which the external shuffle service will run.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.service.index.cache.size</code></td>
  <td>100m</td>
  <td>
    Cache entries limited to the specified memory footprint in bytes.
  </td>
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
</tr>
<tr>
  <td><code>spark.shuffle.sort.bypassMergeThreshold</code></td>
  <td>200</td>
  <td>
    (Advanced) In the sort-based shuffle manager, avoid merge-sorting data if there is no
    map-side aggregation and there are at most this many reduce partitions.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress data spilled during shuffles. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.accurateBlockThreshold</code></td>
  <td>100 * 1024 * 1024</td>
  <td>
    Threshold in bytes above which the size of shuffle blocks in HighlyCompressedMapStatus is 
    accurately recorded. This helps to prevent OOM by avoiding underestimating shuffle 
    block size when fetch shuffle blocks.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.timeout</code></td>
  <td>5000</td>
  <td>
    Timeout in milliseconds for registration to the external shuffle service.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.maxAttempts</code></td>
  <td>3</td>
  <td>
    When we fail to register to the external shuffle service, we will retry for maxAttempts times.
  </td>
</tr>
</table>

### Spark UI

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.eventLog.logBlockUpdates.enabled</code></td>
  <td>false</td>
  <td>
    Whether to log events for every block update, if <code>spark.eventLog.enabled</code> is true.
    *Warning*: This will increase the size of the event log considerably.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.longForm.enabled</code></td>
  <td>false</td>
  <td>
    If true, use the long form of call sites in the event log. Otherwise use the short form.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress logged events, if <code>spark.eventLog.enabled</code> is true.
    Compression will use <code>spark.io.compression.codec</code>.
  </td>
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
</tr>
<tr>
  <td><code>spark.eventLog.enabled</code></td>
  <td>false</td>
  <td>
    Whether to log Spark events, useful for reconstructing the Web UI after the application has
    finished.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.overwrite</code></td>
  <td>false</td>
  <td>
    Whether to overwrite any existing files.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.buffer.kb</code></td>
  <td>100k</td>
  <td>
    Buffer size to use when writing to output streams, in KiB unless otherwise specified.
  </td>
</tr>
<tr>
  <td><code>spark.ui.dagGraph.retainedRootRDDs</code></td>
  <td>Int.MaxValue</td>
  <td>
    How many DAG graph nodes the Spark UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.ui.enabled</code></td>
  <td>true</td>
  <td>
    Whether to run the web UI for the Spark application.
  </td>
</tr>
<tr>
  <td><code>spark.ui.killEnabled</code></td>
  <td>true</td>
  <td>
    Allows jobs and stages to be killed from the web UI.
  </td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.period</code></td>
  <td>100ms</td>
  <td>
    How often to update live entities. -1 means "never update" when replaying applications,
    meaning only the last write will happen. For live applications, this avoids a few
    operations that we can live without when rapidly processing incoming task events.
  </td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.minFlushPeriod</code></td>
  <td>1s</td>
  <td>
    Minimum time elapsed before stale UI data is flushed. This avoids UI staleness when incoming
    task events are not fired frequently.
  </td>
</tr>
<tr>
  <td><code>spark.ui.port</code></td>
  <td>4040</td>
  <td>
    Port for your application's dashboard, which shows memory and workload data.
  </td>
</tr>
<tr>
  <td><code>spark.ui.retainedJobs</code></td>
  <td>1000</td>
  <td>
    How many jobs the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
</tr>
<tr>
  <td><code>spark.ui.retainedStages</code></td>
  <td>1000</td>
  <td>
    How many stages the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
</tr>
<tr>
  <td><code>spark.ui.retainedTasks</code></td>
  <td>100000</td>
  <td>
    How many tasks in one stage the Spark UI and status APIs remember before garbage collecting.
    This is a target maximum, and fewer elements may be retained in some circumstances.
  </td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxy</code></td>
  <td>false</td>
  <td>
    Enable running Spark Master as reverse proxy for worker and application UIs. In this mode, Spark master will reverse proxy the worker and application UIs to enable access without requiring direct access to their hosts. Use it with caution, as worker and application UI will not be accessible directly, you will only be able to access them through spark master/proxy public URL. This setting affects all the workers and application UIs running in the cluster and must be set on all the workers, drivers and masters.
  </td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxyUrl</code></td>
  <td></td>
  <td>
    This is the URL where your proxy is running. This URL is for proxy which is running in front of Spark Master. This is useful when running proxy for authentication e.g. OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to reach your proxy.
  </td>
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
</tr>
<tr>
  <td><code>spark.worker.ui.retainedExecutors</code></td>
  <td>1000</td>
  <td>
    How many finished executors the Spark UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.worker.ui.retainedDrivers</code></td>
  <td>1000</td>
  <td>
    How many finished drivers the Spark UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.sql.ui.retainedExecutions</code></td>
  <td>1000</td>
  <td>
    How many finished executions the Spark UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.ui.retainedBatches</code></td>
  <td>1000</td>
  <td>
    How many finished batches the Spark UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.ui.retainedDeadExecutors</code></td>
  <td>100</td>
  <td>
    How many dead executors the Spark UI and status APIs remember before garbage collecting.
  </td>
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
  </td>
</tr>
<tr>
  <td><code>spark.ui.requestHeaderSize</code></td>
  <td>8k</td>
  <td>
    The maximum allowed size for a HTTP request header, in bytes unless otherwise specified.
    This setting applies for the Spark History Server too.
  </td>
</tr>
</table>

### Compression and Serialization

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.broadcast.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress broadcast variables before sending them. Generally a good idea.
    Compression will use <code>spark.io.compression.codec</code>.
  </td>
</tr>
<tr>
  <td><code>spark.checkpoint.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress RDD checkpoints. Generally a good idea.
    Compression will use <code>spark.io.compression.codec</code>.
   </td>
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
</tr>
<tr>
  <td><code>spark.io.compression.lz4.blockSize</code></td>
  <td>32k</td>
  <td>
    Block size in bytes used in LZ4 compression, in the case when LZ4 compression codec
    is used. Lowering this block size will also lower shuffle memory usage when LZ4 is used.
  </td>
</tr>
<tr>
  <td><code>spark.io.compression.snappy.blockSize</code></td>
  <td>32k</td>
  <td>
    Block size in bytes used in Snappy compression, in the case when Snappy compression codec
    is used. Lowering this block size will also lower shuffle memory usage when Snappy is used.
  </td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.level</code></td>
  <td>1</td>
  <td>
    Compression level for Zstd compression codec. Increasing the compression level will result in better
    compression at the expense of more CPU and memory.
  </td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.bufferSize</code></td>
  <td>32k</td>
  <td>
    Buffer size in bytes used in Zstd compression, in the case when Zstd compression codec
    is used. Lowering this size will lower the shuffle memory usage when Zstd is used, but it
    might increase the compression cost because of excessive JNI call overhead.
  </td>
</tr>
<tr>
  <td><code>spark.kryo.classesToRegister</code></td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, give a comma-separated list of custom class names to register
    with Kryo.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
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
</tr>
<tr>
  <td><code>spark.kryo.registrator</code></td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, give a comma-separated list of classes that register your custom classes with Kryo. This
    property is useful if you need to register your classes in a custom way, e.g. to specify a custom
    field serializer. Otherwise <code>spark.kryo.classesToRegister</code> is simpler. It should be
    set to classes that extend
    <a href="api/scala/index.html#org.apache.spark.serializer.KryoRegistrator">
    <code>KryoRegistrator</code></a>.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
</tr>
<tr>
  <td><code>spark.kryo.unsafe</code></td>
  <td>false</td>
  <td>
    Whether to use unsafe based Kryo serializer. Can be
    substantially faster by using Unsafe Based IO.
  </td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer.max</code></td>
  <td>64m</td>
  <td>
    Maximum allowable size of Kryo serialization buffer, in MiB unless otherwise specified.
    This must be larger than any object you attempt to serialize and must be less than 2048m.
    Increase this if you get a "buffer limit exceeded" exception inside Kryo.
  </td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer</code></td>
  <td>64k</td>
  <td>
    Initial size of Kryo's serialization buffer, in KiB unless otherwise specified. 
    Note that there will be one buffer <i>per core</i> on each worker. This buffer will grow up to
    <code>spark.kryoserializer.buffer.max</code> if needed.
  </td>
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
    <a href="api/scala/index.html#org.apache.spark.serializer.Serializer">
    <code>org.apache.spark.Serializer</code></a>.
  </td>
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
</tr>
</table>

### Memory Management

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
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
</tr>
<tr>
  <td><code>spark.memory.offHeap.enabled</code></td>
  <td>false</td>
  <td>
    If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory 
    use is enabled, then <code>spark.memory.offHeap.size</code> must be positive.
  </td>
</tr>
<tr>
  <td><code>spark.memory.offHeap.size</code></td>
  <td>0</td>
  <td>
    The absolute amount of memory in bytes which can be used for off-heap allocation.
    This setting has no impact on heap memory usage, so if your executors' total memory consumption 
    must fit within some hard limit then be sure to shrink your JVM heap size accordingly.
    This must be set to a positive value when <code>spark.memory.offHeap.enabled=true</code>.
  </td>
</tr>
<tr>
  <td><code>spark.memory.useLegacyMode</code></td>
  <td>false</td>
  <td>
    Whether to enable the legacy memory management mode used in Spark 1.5 and before.
    The legacy mode rigidly partitions the heap space into fixed-size regions,
    potentially leading to excessive spilling if the application was not tuned.
    The following deprecated memory fraction configurations are not read unless this is enabled:
    <code>spark.shuffle.memoryFraction</code><br>
    <code>spark.storage.memoryFraction</code><br>
    <code>spark.storage.unrollFraction</code>
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.memoryFraction</code></td>
  <td>0.2</td>
  <td>
    (deprecated) This is read only if <code>spark.memory.useLegacyMode</code> is enabled.
    Fraction of Java heap to use for aggregation and cogroups during shuffles.
    At any given time, the collective size of
    all in-memory maps used for shuffles is bounded by this limit, beyond which the contents will
    begin to spill to disk. If spills are often, consider increasing this value at the expense of
    <code>spark.storage.memoryFraction</code>.
  </td>
</tr>
<tr>
  <td><code>spark.storage.memoryFraction</code></td>
  <td>0.6</td>
  <td>
    (deprecated) This is read only if <code>spark.memory.useLegacyMode</code> is enabled.
    Fraction of Java heap to use for Spark's memory cache. This should not be larger than the "old"
    generation of objects in the JVM, which by default is given 0.6 of the heap, but you can
    increase it if you configure your own old generation size.
  </td>
</tr>
<tr>
  <td><code>spark.storage.unrollFraction</code></td>
  <td>0.2</td>
  <td>
    (deprecated) This is read only if <code>spark.memory.useLegacyMode</code> is enabled.
    Fraction of <code>spark.storage.memoryFraction</code> to use for unrolling blocks in memory.
    This is dynamically allocated by dropping existing blocks when there is not enough free
    storage space to unroll the new block in its entirety.
  </td>
</tr>
<tr>
  <td><code>spark.storage.replication.proactive</code></td>
  <td>false</td>
  <td>
    Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to
    executor failures are replenished if there are any existing available replicas. This tries
    to get the replication level of the block to the initial number.
  </td>
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
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking</code></td>
  <td>true</td>
  <td>
    Enables or disables context cleaning.
  </td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking</code></td>
  <td>true</td>
  <td>
    Controls whether the cleaning thread should block on cleanup tasks (other than shuffle, which is controlled by
    <code>spark.cleaner.referenceTracking.blocking.shuffle</code> Spark property).
  </td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking.shuffle</code></td>
  <td>false</td>
  <td>
    Controls whether the cleaning thread should block on shuffle cleanup tasks.
  </td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.cleanCheckpoints</code></td>
  <td>false</td>
  <td>
    Controls whether to clean checkpoint files if the reference is out of scope.
  </td>
</tr>
</table>

### Execution Behavior

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.broadcast.blockSize</code></td>
  <td>4m</td>
  <td>
    Size of each piece of a block for <code>TorrentBroadcastFactory</code>, in KiB unless otherwise 
    specified. Too large a value decreases parallelism during broadcast (makes it slower); however, 
    if it is too small, <code>BlockManager</code> might take a performance hit.
  </td>
</tr>
<tr>
  <td><code>spark.broadcast.checksum</code></td>
  <td>true</td>
  <td>
    Whether to enable checksum for broadcast. If enabled, broadcasts will include a checksum, which can
    help detect corrupted blocks, at the cost of computing and sending a little more data. It's possible
    to disable it if the network has other mechanisms to guarantee data won't be corrupted during broadcast.
  </td>
</tr>
<tr>
  <td><code>spark.executor.cores</code></td>
  <td>
    1 in YARN mode, all the available cores on the worker in
    standalone and Mesos coarse-grained modes.
  </td>
  <td>
    The number of cores to use on each executor.

    In standalone and Mesos coarse-grained modes, for more detail, see
    <a href="spark-standalone.html#Executors Scheduling">this description</a>.
  </td>
</tr>
<tr>
  <td><code>spark.default.parallelism</code></td>
  <td>
    For distributed shuffle operations like <code>reduceByKey</code> and <code>join</code>, the
    largest number of partitions in a parent RDD.  For operations like <code>parallelize</code>
    with no parent RDDs, it depends on the cluster manager:
    <ul>
      <li>Local mode: number of cores on the local machine</li>
      <li>Mesos fine grained mode: 8</li>
      <li>Others: total number of cores on all executor nodes or 2, whichever is larger</li>
    </ul>
  </td>
  <td>
    Default number of partitions in RDDs returned by transformations like <code>join</code>,
    <code>reduceByKey</code>, and <code>parallelize</code> when not set by user.
  </td>
</tr>
<tr>
    <td><code>spark.executor.heartbeatInterval</code></td>
    <td>10s</td>
    <td>Interval between each executor's heartbeats to the driver.  Heartbeats let
    the driver know that the executor is still alive and update it with metrics for in-progress
    tasks. spark.executor.heartbeatInterval should be significantly less than
    spark.network.timeout</td>
</tr>
<tr>
  <td><code>spark.files.fetchTimeout</code></td>
  <td>60s</td>
  <td>
    Communication timeout to use when fetching files added through SparkContext.addFile() from
    the driver.
  </td>
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
</tr>
<tr>
  <td><code>spark.files.overwrite</code></td>
  <td>false</td>
  <td>
    Whether to overwrite files added through SparkContext.addFile() when the target file exists and
    its contents do not match those of the source.
  </td>
</tr>
<tr>
  <td><code>spark.files.maxPartitionBytes</code></td>
  <td>134217728 (128 MB)</td>
  <td>
    The maximum number of bytes to pack into a single partition when reading files.
  </td>
</tr>
<tr>
  <td><code>spark.files.openCostInBytes</code></td>
  <td>4194304 (4 MB)</td>
  <td>
    The estimated cost to open a file, measured by the number of bytes could be scanned at the same
    time. This is used when putting multiple files into a partition. It is better to overestimate,
    then the partitions with small files will be faster than partitions with bigger files.
  </td>
</tr>
<tr>
    <td><code>spark.hadoop.cloneConf</code></td>
    <td>false</td>
    <td>If set to true, clones a new Hadoop <code>Configuration</code> object for each task.  This
    option should be enabled to work around <code>Configuration</code> thread-safety issues (see
    <a href="https://issues.apache.org/jira/browse/SPARK-2546">SPARK-2546</a> for more details).
    This is disabled by default in order to avoid unexpected performance regressions for jobs that
    are not affected by these issues.</td>
</tr>
<tr>
    <td><code>spark.hadoop.validateOutputSpecs</code></td>
    <td>true</td>
    <td>If set to true, validates the output specification (e.g. checking if the output directory already exists)
    used in saveAsHadoopFile and other variants. This can be disabled to silence exceptions due to pre-existing
    output directories. We recommend that users do not disable this except if trying to achieve compatibility with
    previous versions of Spark. Simply use Hadoop's FileSystem API to delete output directories by hand.
    This setting is ignored for jobs generated through Spark Streaming's StreamingContext, since
    data may need to be rewritten to pre-existing output directories during checkpoint recovery.</td>
</tr>
<tr>
  <td><code>spark.storage.memoryMapThreshold</code></td>
  <td>2m</td>
  <td>
    Size in bytes of a block above which Spark memory maps when reading a block from disk.
    This prevents Spark from memory mapping very small blocks. In general, memory
    mapping has high overhead for blocks close to or below the page size of the operating system.
  </td>
</tr>
<tr>
  <td><code>spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version</code></td>
  <td>1</td>
  <td>
    The file output committer algorithm version, valid algorithm version number: 1 or 2.
    Version 2 may have better performance, but version 1 may handle failures better in certain situations,
    as per <a href="https://issues.apache.org/jira/browse/MAPREDUCE-4815">MAPREDUCE-4815</a>.
  </td>
</tr>
</table>

### Networking

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.rpc.message.maxSize</code></td>
  <td>128</td>
  <td>
    Maximum message size (in MB) to allow in "control plane" communication; generally only applies to map
    output size information sent between executors and the driver. Increase this if you are running
    jobs with many thousands of map and reduce tasks and see messages about the RPC message size.
  </td>
</tr>
<tr>
  <td><code>spark.blockManager.port</code></td>
  <td>(random)</td>
  <td>
    Port for all block managers to listen on. These exist on both the driver and the executors.
  </td>
</tr>
<tr>
  <td><code>spark.driver.blockManager.port</code></td>
  <td>(value of spark.blockManager.port)</td>
  <td>
    Driver-specific port for the block manager to listen on, for cases where it cannot use the same
    configuration as executors.
  </td>
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
</tr>
<tr>
  <td><code>spark.driver.host</code></td>
  <td>(local hostname)</td>
  <td>
    Hostname or IP address for the driver.
    This is used for communicating with the executors and the standalone Master.
  </td>
</tr>
<tr>
  <td><code>spark.driver.port</code></td>
  <td>(random)</td>
  <td>
    Port for the driver to listen on.
    This is used for communicating with the executors and the standalone Master.
  </td>
</tr>
<tr>
  <td><code>spark.network.timeout</code></td>
  <td>120s</td>
  <td>
    Default timeout for all network interactions. This config will be used in place of
    <code>spark.storage.blockManagerSlaveTimeoutMs</code>,
    <code>spark.shuffle.io.connectionTimeout</code>, <code>spark.rpc.askTimeout</code> or
    <code>spark.rpc.lookupTimeout</code> if they are not configured.
  </td>
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
</tr>
<tr>
  <td><code>spark.rpc.numRetries</code></td>
  <td>3</td>
  <td>
    Number of times to retry before an RPC task gives up.
    An RPC task will run at most times of this number.
  </td>
</tr>
<tr>
  <td><code>spark.rpc.retry.wait</code></td>
  <td>3s</td>
  <td>
    Duration for an RPC ask operation to wait before retrying.
  </td>
</tr>
<tr>
  <td><code>spark.rpc.askTimeout</code></td>
  <td><code>spark.network.timeout</code></td>
  <td>
    Duration for an RPC ask operation to wait before timing out.
  </td>
</tr>
<tr>
  <td><code>spark.rpc.lookupTimeout</code></td>
  <td>120s</td>
  <td>
    Duration for an RPC remote endpoint lookup operation to wait before timing out.
  </td>
</tr>
</table>

### Scheduling

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.cores.max</code></td>
  <td>(not set)</td>
  <td>
    When running on a <a href="spark-standalone.html">standalone deploy cluster</a> or a
    <a href="running-on-mesos.html#mesos-run-modes">Mesos cluster in "coarse-grained"
    sharing mode</a>, the maximum amount of CPU cores to request for the application from
    across the cluster (not from each machine). If not set, the default will be
    <code>spark.deploy.defaultCores</code> on Spark's standalone cluster manager, or
    infinite (all available cores) on Mesos.
  </td>
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
</tr>
<tr>
  <td><code>spark.locality.wait.node</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for node locality. For example, you can set this to 0 to skip
    node locality and search immediately for rack locality (if your cluster has rack information).
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait.process</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for process locality. This affects tasks that attempt to access
    cached data in a particular executor process.
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait.rack</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for rack locality.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.maxRegisteredResourcesWaitingTime</code></td>
  <td>30s</td>
  <td>
    Maximum amount of time to wait for resources to register before scheduling begins.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.minRegisteredResourcesRatio</code></td>
  <td>0.8 for KUBERNETES mode; 0.8 for YARN mode; 0.0 for standalone mode and Mesos coarse-grained mode</td>
  <td>
    The minimum ratio of registered resources (registered resources / total expected resources)
    (resources are executors in yarn mode and Kubernetes mode, CPU cores in standalone mode and Mesos coarse-grained
     mode ['spark.cores.max' value is total expected resources for Mesos coarse-grained mode] )
    to wait for before scheduling begins. Specified as a double between 0.0 and 1.0.
    Regardless of whether the minimum ratio of resources has been reached,
    the maximum amount of time it will wait before scheduling begins is controlled by config
    <code>spark.scheduler.maxRegisteredResourcesWaitingTime</code>.
  </td>
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
</tr>
<tr>
  <td><code>spark.scheduler.revive.interval</code></td>
  <td>1s</td>
  <td>
    The interval length for the scheduler to revive the worker resource offers to run tasks.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>10000</td>
  <td>
    Capacity for event queue in Spark listener bus, must be greater than 0. Consider increasing
    value (e.g. 20000) if listener events are dropped. Increasing this value may result in the
    driver using more memory.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.blacklist.unschedulableTaskSetTimeout</code></td>
  <td>120s</td>
  <td>
    The timeout in seconds to wait to acquire a new executor and schedule a task before aborting a
    TaskSet which is unschedulable because of being completely blacklisted.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.enabled</code></td>
  <td>
    false
  </td>
  <td>
    If set to "true", prevent Spark from scheduling tasks on executors that have been blacklisted
    due to too many task failures. The blacklisting algorithm can be further controlled by the
    other "spark.blacklist" configuration options.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.timeout</code></td>
  <td>1h</td>
  <td>
    (Experimental) How long a node or executor is blacklisted for the entire application, before it
    is unconditionally removed from the blacklist to attempt running new tasks.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.task.maxTaskAttemptsPerExecutor</code></td>
  <td>1</td>
  <td>
    (Experimental) For a given task, how many times it can be retried on one executor before the
    executor is blacklisted for that task.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.task.maxTaskAttemptsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) For a given task, how many times it can be retried on one node, before the entire
    node is blacklisted for that task.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.stage.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different tasks must fail on one executor, within one stage, before the
    executor is blacklisted for that stage.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.stage.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different executors are marked as blacklisted for a given stage, before
    the entire node is marked as failed for the stage.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.application.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different tasks must fail on one executor, in successful task sets,
    before the executor is blacklisted for the entire application.  Blacklisted executors will
    be automatically added back to the pool of available resources after the timeout specified by
    <code>spark.blacklist.timeout</code>.  Note that with dynamic allocation, though, the executors
    may get marked as idle and be reclaimed by the cluster manager.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.application.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>
    (Experimental) How many different executors must be blacklisted for the entire application,
    before the node is blacklisted for the entire application.  Blacklisted nodes will
    be automatically added back to the pool of available resources after the timeout specified by
    <code>spark.blacklist.timeout</code>.  Note that with dynamic allocation, though, the executors
    on the node may get marked as idle and be reclaimed by the cluster manager.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.killBlacklistedExecutors</code></td>
  <td>false</td>
  <td>
    (Experimental) If set to "true", allow Spark to automatically kill the executors 
    when they are blacklisted on fetch failure or blacklisted for the entire application, 
    as controlled by spark.blacklist.application.*. Note that, when an entire node is added 
    to the blacklist, all of the executors on that node will be killed.
  </td>
</tr>
<tr>
  <td><code>spark.blacklist.application.fetchFailure.enabled</code></td>
  <td>false</td>
  <td>
    (Experimental) If set to "true", Spark will blacklist the executor immediately when a fetch
    failure happens. If external shuffle service is enabled, then the whole node will be
    blacklisted.
  </td>
</tr>
<tr>
  <td><code>spark.speculation</code></td>
  <td>false</td>
  <td>
    If set to "true", performs speculative execution of tasks. This means if one or more tasks are
    running slowly in a stage, they will be re-launched.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.interval</code></td>
  <td>100ms</td>
  <td>
    How often Spark will check for tasks to speculate.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.multiplier</code></td>
  <td>1.5</td>
  <td>
    How many times slower a task is than the median to be considered for speculation.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.quantile</code></td>
  <td>0.75</td>
  <td>
    Fraction of tasks which must be complete before speculation is enabled for a particular stage.
  </td>
</tr>
<tr>
  <td><code>spark.task.cpus</code></td>
  <td>1</td>
  <td>
    Number of cores to allocate for each task.
  </td>
</tr>
<tr>
  <td><code>spark.task.maxFailures</code></td>
  <td>4</td>
  <td>
    Number of failures of any particular task before giving up on the job.
    The total number of failures spread across different tasks will not cause the job
    to fail; a particular task has to fail this number of attempts.
    Should be greater than or equal to 1. Number of allowed retries = this value - 1.
  </td>
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
</tr>
<tr>
  <td><code>spark.task.reaper.threadDump</code></td>
  <td>true</td>
  <td>
    When <code>spark.task.reaper.enabled = true</code>, this setting controls whether task thread
    dumps are logged during periodic polling of killed tasks. Set this to false to disable
    collection of thread dumps.
  </td>
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
</tr>
<tr>
  <td><code>spark.stage.maxConsecutiveAttempts</code></td>
  <td>4</td>
  <td>
    Number of consecutive stage attempts allowed before a stage is aborted.
  </td>
</tr>
</table>

### Dynamic Allocation

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.dynamicAllocation.enabled</code></td>
  <td>false</td>
  <td>
    Whether to use dynamic resource allocation, which scales the number of executors registered
    with this application up and down based on the workload.
    For more detail, see the description
    <a href="job-scheduling.html#dynamic-resource-allocation">here</a>.
    <br><br>
    This requires <code>spark.shuffle.service.enabled</code> to be set.
    The following configurations are also relevant:
    <code>spark.dynamicAllocation.minExecutors</code>,
    <code>spark.dynamicAllocation.maxExecutors</code>, and
    <code>spark.dynamicAllocation.initialExecutors</code>
    <code>spark.dynamicAllocation.executorAllocationRatio</code>
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.executorIdleTimeout</code></td>
  <td>60s</td>
  <td>
    If dynamic allocation is enabled and an executor has been idle for more than this duration,
    the executor will be removed. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.cachedExecutorIdleTimeout</code></td>
  <td>infinity</td>
  <td>
    If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration,
    the executor will be removed. For more details, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.initialExecutors</code></td>
  <td><code>spark.dynamicAllocation.minExecutors</code></td>
  <td>
    Initial number of executors to run if dynamic allocation is enabled.
    <br /><br />
    If `--num-executors` (or `spark.executor.instances`) is set and larger than this value, it will
    be used as the initial number of executors.
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.maxExecutors</code></td>
  <td>infinity</td>
  <td>
    Upper bound for the number of executors if dynamic allocation is enabled.
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.minExecutors</code></td>
  <td>0</td>
  <td>
    Lower bound for the number of executors if dynamic allocation is enabled.
  </td>
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
</tr>
<tr>
  <td><code>spark.dynamicAllocation.schedulerBacklogTimeout</code></td>
  <td>1s</td>
  <td>
    If dynamic allocation is enabled and there have been pending tasks backlogged for more than
    this duration, new executors will be requested. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
</tr>
<tr>
  <td><code>spark.dynamicAllocation.sustainedSchedulerBacklogTimeout</code></td>
  <td><code>schedulerBacklogTimeout</code></td>
  <td>
    Same as <code>spark.dynamicAllocation.schedulerBacklogTimeout</code>, but used only for
    subsequent executor requests. For more detail, see this
    <a href="job-scheduling.html#resource-allocation-policy">description</a>.
  </td>
</tr>
</table>

### Security

Please refer to the [Security](security.html) page for available options on how to secure different
Spark subsystems.

### Spark SQL

Running the <code>SET -v</code> command will show the entire list of the SQL configuration.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
// spark is an existing SparkSession
spark.sql("SET -v").show(numRows = 200, truncate = false)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// spark is an existing SparkSession
spark.sql("SET -v").show(200, false);
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
# spark is an existing SparkSession
spark.sql("SET -v").show(n=200, truncate=False)
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
sparkR.session()
properties <- sql("SET -v")
showDF(properties, numRows = 200, truncate = FALSE)
{% endhighlight %}

</div>
</div>


### Spark Streaming

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
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
</tr>
<tr>
  <td><code>spark.streaming.backpressure.initialRate</code></td>
  <td>not set</td>
  <td>
    This is the initial maximum receiving rate at which each receiver will receive data for the
    first batch when the backpressure mechanism is enabled.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.blockInterval</code></td>
  <td>200ms</td>
  <td>
    Interval at which data received by Spark Streaming receivers is chunked
    into blocks of data before storing them in Spark. Minimum recommended - 50 ms. See the
    <a href="streaming-programming-guide.html#level-of-parallelism-in-data-receiving">performance
     tuning</a> section in the Spark Streaming programing guide for more details.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.maxRate</code></td>
  <td>not set</td>
  <td>
    Maximum rate (number of records per second) at which each receiver will receive data.
    Effectively, each stream will consume at most this number of records per second.
    Setting this configuration to 0 or a negative number will put no limit on the rate.
    See the <a href="streaming-programming-guide.html#deploying-applications">deployment guide</a>
    in the Spark Streaming programing guide for mode details.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.writeAheadLog.enable</code></td>
  <td>false</td>
  <td>
    Enable write-ahead logs for receivers. All the input data received through receivers
    will be saved to write-ahead logs that will allow it to be recovered after driver failures.
    See the <a href="streaming-programming-guide.html#deploying-applications">deployment guide</a>
    in the Spark Streaming programing guide for more details.
  </td>
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
</tr>
<tr>
  <td><code>spark.streaming.stopGracefullyOnShutdown</code></td>
  <td>false</td>
  <td>
    If <code>true</code>, Spark shuts down the <code>StreamingContext</code> gracefully on JVM
    shutdown rather than immediately.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.kafka.maxRatePerPartition</code></td>
  <td>not set</td>
  <td>
    Maximum rate (number of records per second) at which data will be read from each Kafka
    partition when using the new Kafka direct stream API. See the
    <a href="streaming-kafka-integration.html">Kafka Integration guide</a>
    for more details.
  </td>
</tr>
<tr>
    <td><code>spark.streaming.kafka.minRatePerPartition</code></td>
    <td>1</td>
    <td>
      Minimum rate (number of records per second) at which data will be read from each Kafka
      partition when using the new Kafka direct stream API.
    </td>
</tr>
<tr>
  <td><code>spark.streaming.kafka.maxRetries</code></td>
  <td>1</td>
  <td>
    Maximum number of consecutive retries the driver will make in order to find
    the latest offsets on the leader of each partition (a default value of 1
    means that the driver will make a maximum of 2 attempts). Only applies to
    the new Kafka direct stream API.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.ui.retainedBatches</code></td>
  <td>1000</td>
  <td>
    How many batches the Spark Streaming UI and status APIs remember before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.driver.writeAheadLog.closeFileAfterWrite</code></td>
  <td>false</td>
  <td>
    Whether to close the file after writing a write-ahead log record on the driver. Set this to 'true'
    when you want to use S3 (or any file system that does not support flushing) for the metadata WAL
    on the driver.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.receiver.writeAheadLog.closeFileAfterWrite</code></td>
  <td>false</td>
  <td>
    Whether to close the file after writing a write-ahead log record on the receivers. Set this to 'true'
    when you want to use S3 (or any file system that does not support flushing) for the data WAL
    on the receivers.
  </td>
</tr>
</table>

### SparkR

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.r.numRBackendThreads</code></td>
  <td>2</td>
  <td>
    Number of threads used by RBackend to handle RPC calls from SparkR package.
  </td>
</tr>
<tr>
  <td><code>spark.r.command</code></td>
  <td>Rscript</td>
  <td>
    Executable for executing R scripts in cluster modes for both driver and workers.
  </td>
</tr>
<tr>
  <td><code>spark.r.driver.command</code></td>
  <td>spark.r.command</td>
  <td>
    Executable for executing R scripts in client modes for driver. Ignored in cluster modes.
  </td>
</tr>
<tr>
  <td><code>spark.r.shell.command</code></td>
  <td>R</td>
  <td>
    Executable for executing sparkR shell in client modes for driver. Ignored in cluster modes. It is the same as environment variable <code>SPARKR_DRIVER_R</code>, but take precedence over it.
    <code>spark.r.shell.command</code> is used for sparkR shell while <code>spark.r.driver.command</code> is used for running R script.
  </td>
</tr>
<tr>
  <td><code>spark.r.backendConnectionTimeout</code></td>
  <td>6000</td>
  <td>
    Connection timeout set by R process on its connection to RBackend in seconds.
  </td>
</tr>
<tr>
  <td><code>spark.r.heartBeatInterval</code></td>
  <td>100</td>
  <td>
    Interval for heartbeats sent from SparkR backend to R process to prevent connection timeout.
  </td>
</tr>

</table>

### GraphX

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.graphx.pregel.checkpointInterval</code></td>
  <td>-1</td>
  <td>
    Checkpoint interval for graph and message in Pregel. It used to avoid stackOverflowError due to long lineage chains
  after lots of iterations. The checkpoint is disabled by default.
  </td>
</tr>
</table>

### Deploy

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.deploy.recoveryMode</code></td>
    <td>NONE</td>
    <td>The recovery mode setting to recover submitted Spark jobs with cluster mode when it failed and relaunches.
    This is only applicable for cluster mode when running with Standalone or Mesos.</td>
  </tr>
  <tr>
    <td><code>spark.deploy.zookeeper.url</code></td>
    <td>None</td>
    <td>When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.</td>
  </tr>
  <tr>
    <td><code>spark.deploy.zookeeper.dir</code></td>
    <td>None</td>
    <td>When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state.</td>
  </tr>
</table>


### Cluster Managers

Each cluster manager in Spark has additional configuration options. Configurations
can be found on the pages for each mode:

#### [YARN](running-on-yarn.html#configuration)

#### [Mesos](running-on-mesos.html#configuration)

#### [Kubernetes](running-on-kubernetes.html#configuration)

#### [Standalone Mode](spark-standalone.html#cluster-launch-scripts)

# Environment Variables

Certain Spark settings can be configured through environment variables, which are read from the
`conf/spark-env.sh` script in the directory where Spark is installed (or `conf/spark-env.cmd` on
Windows). In Standalone and Mesos modes, this file can give machine specific information such as
hostnames. It is also sourced when running local Spark applications or submission scripts.

Note that `conf/spark-env.sh` does not exist by default when Spark is installed. However, you can
copy `conf/spark-env.sh.template` to create it. Make sure you make the copy executable.

The following variables can be set in `spark-env.sh`:


<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>JAVA_HOME</code></td>
    <td>Location where Java is installed (if it's not on your default <code>PATH</code>).</td>
  </tr>
  <tr>
    <td><code>PYSPARK_PYTHON</code></td>
    <td>Python binary executable to use for PySpark in both driver and workers (default is <code>python2.7</code> if available, otherwise <code>python</code>).
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
`log4j.properties` file in the `conf` directory. One way to start is to copy the existing
`log4j.properties.template` located there.

# Overriding configuration directory

To specify a different configuration directory other than the default "SPARK_HOME/conf",
you can set SPARK_CONF_DIR. Spark will use the configuration files (spark-defaults.conf, spark-env.sh, log4j.properties, etc)
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

The better choice is to use spark hadoop properties in the form of `spark.hadoop.*`. 
They can be considered as same as normal spark properties which can be set in `$SPARK_HOME/conf/spark-defaults.conf`

In some cases, you may want to avoid hard-coding certain configurations in a `SparkConf`. For
instance, Spark allows you to simply create an empty conf and set spark/spark hadoop properties.

{% highlight scala %}
val conf = new SparkConf().set("spark.hadoop.abc.def","xyz")
val sc = new SparkContext(conf)
{% endhighlight %}

Also, you can modify or add configurations at runtime:
{% highlight bash %}
./bin/spark-submit \ 
  --name "My app" \ 
  --master local[4] \  
  --conf spark.eventLog.enabled=false \ 
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \ 
  --conf spark.hadoop.abc.def=xyz \ 
  myApp.jar
{% endhighlight %}
