---
layout: global
title: Spark Configuration
---

Spark provides three main locations to configure the system:

* [Java system properties](#system-properties), which control internal configuration parameters and can be set
  either programmatically (by calling `System.setProperty` *before* creating a `SparkContext`) or through
  JVM arguments.
* [Environment variables](#environment-variables) for configuring per-machine settings such as the IP address,
  which can be set in the `conf/spark-env.sh` script.
* [Logging configuration](#configuring-logging), which is done through `log4j.properties`.


# System Properties

To set a system property for configuring Spark, you need to either pass it with a -D flag to the JVM (for example `java -Dspark.cores.max=5 MyProgram`) or call `System.setProperty` in your code *before* creating your Spark context, as follows:

{% highlight scala %}
System.setProperty("spark.cores.max", "5")
val sc = new SparkContext(...)
{% endhighlight %}

Most of the configurable system properties control internal settings that have reasonable default values. However,
there are at least five properties that you will commonly want to control:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>spark.executor.memory</td>
  <td>512m</td>
  <td>
    Amount of memory to use per executor process, in the same format as JVM memory strings (e.g. <code>512m</code>, <code>2g</code>).
  </td>
</tr>
<tr>
  <td>spark.serializer</td>
  <td>org.apache.spark.serializer.<br />JavaSerializer</td>
  <td>
    Class to use for serializing objects that will be sent over the network or need to be cached
    in serialized form. The default of Java serialization works with any Serializable Java object but is
    quite slow, so we recommend <a href="tuning.html">using <code>org.apache.spark.serializer.KryoSerializer</code>
    and configuring Kryo serialization</a> when speed is necessary. Can be any subclass of
    <a href="api/core/index.html#org.apache.spark.serializer.Serializer"><code>org.apache.spark.Serializer</code></a>.
  </td>
</tr>
<tr>
  <td>spark.kryo.registrator</td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, set this class to register your custom classes with Kryo.
    It should be set to a class that extends
    <a href="api/core/index.html#org.apache.spark.serializer.KryoRegistrator"><code>KryoRegistrator</code></a>.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
</tr>
<tr>
  <td>spark.local.dir</td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored
    on disk. This should be on a fast, local disk in your system. It can also be a comma-separated
    list of multiple directories on different disks.
  </td>
</tr>
<tr>
  <td>spark.cores.max</td>
  <td>(infinite)</td>
  <td>
    When running on a <a href="spark-standalone.html">standalone deploy cluster</a> or a
    <a href="running-on-mesos.html#mesos-run-modes">Mesos cluster in "coarse-grained"
    sharing mode</a>, how many CPU cores to request at most. The default will use all available cores
    offered by the cluster manager.
  </td>
</tr>
</table>


Apart from these, the following properties are also available, and may be useful in some situations:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>spark.default.parallelism</td>
  <td>8</td>
  <td>
    Default number of tasks to use for distributed shuffle operations (<code>groupByKey</code>,
    <code>reduceByKey</code>, etc) when not set by user.
  </td>
</tr>
<tr>
  <td>spark.storage.memoryFraction</td>
  <td>0.66</td>
  <td>
    Fraction of Java heap to use for Spark's memory cache. This should not be larger than the "old"
    generation of objects in the JVM, which by default is given 2/3 of the heap, but you can increase
    it if you configure your own old generation size.
  </td>
</tr>
<tr>
  <td>spark.mesos.coarse</td>
  <td>false</td>
  <td>
    If set to "true", runs over Mesos clusters in
    <a href="running-on-mesos.html#mesos-run-modes">"coarse-grained" sharing mode</a>,
    where Spark acquires one long-lived Mesos task on each machine instead of one Mesos task per Spark task.
    This gives lower-latency scheduling for short queries, but leaves resources in use for the whole
    duration of the Spark job.
  </td>
</tr>
<tr>
  <td>spark.ui.port</td>
  <td>4040</td>
  <td>
    Port for your application's dashboard, which shows memory and workload data
  </td>
</tr>
<tr>
  <td>spark.ui.retained_stages</td>
  <td>1000</td>
  <td>
    How many stages the Spark UI remembers before garbage collecting.
  </td>
</tr>
<tr>
  <td>spark.shuffle.compress</td>
  <td>true</td>
  <td>
    Whether to compress map output files. Generally a good idea.
  </td>
</tr>
<tr>
  <td>spark.broadcast.compress</td>
  <td>true</td>
  <td>
    Whether to compress broadcast variables before sending them. Generally a good idea.
  </td>
</tr>
<tr>
  <td>spark.rdd.compress</td>
  <td>false</td>
  <td>
    Whether to compress serialized RDD partitions (e.g. for <code>StorageLevel.MEMORY_ONLY_SER</code>).
    Can save substantial space at the cost of some extra CPU time.
  </td>
</tr>
<tr>
  <td>spark.io.compression.codec</td>
  <td>org.apache.spark.io.<br />LZFCompressionCodec</td>
  <td>
    The codec used to compress internal data such as RDD partitions and shuffle outputs. By default, Spark provides two
    codecs: <code>org.apache.spark.io.LZFCompressionCodec</code> and <code>org.apache.spark.io.SnappyCompressionCodec</code>.
  </td>
</tr>
<tr>
  <td>spark.io.compression.snappy.block.size</td>
  <td>32768</td>
  <td>
    Block size (in bytes) used in Snappy compression, in the case when Snappy compression codec is used.
  </td>
</tr>
<tr>
  <td>spark.scheduler.mode</td>
  <td>FIFO</td>
  <td>
    The <a href="job-scheduling.html#scheduling-within-an-application">scheduling mode</a> between
    jobs submitted to the same SparkContext. Can be set to <code>FAIR</code>
    to use fair sharing instead of queueing jobs one after another. Useful for
    multi-user services.
  </td>
</tr>
<tr>
  <td>spark.reducer.maxMbInFlight</td>
  <td>48</td>
  <td>
    Maximum size (in megabytes) of map outputs to fetch simultaneously from each reduce task. Since
    each output requires us to create a buffer to receive it, this represents a fixed memory overhead
    per reduce task, so keep it small unless you have a large amount of memory.
  </td>
</tr>
<tr>
  <td>spark.closure.serializer</td>
  <td>org.apache.spark.serializer.<br />JavaSerializer</td>
  <td>
    Serializer class to use for closures. Generally Java is fine unless your distributed functions
    (e.g. map functions) reference large objects in the driver program.
  </td>
</tr>
<tr>
  <td>spark.kryo.referenceTracking</td>
  <td>true</td>
  <td>
    Whether to track references to the same object when serializing data with Kryo, which is
    necessary if your object graphs have loops and useful for efficiency if they contain multiple
    copies of the same object. Can be disabled to improve performance if you know this is not the
    case.
  </td>
</tr>
<tr>
  <td>spark.kryoserializer.buffer.mb</td>
  <td>2</td>
  <td>
    Maximum object size to allow within Kryo (the library needs to create a buffer at least as
    large as the largest single object you'll serialize). Increase this if you get a "buffer limit
    exceeded" exception inside Kryo. Note that there will be one buffer <i>per core</i> on each worker.
  </td>
</tr>
<tr>
  <td>spark.broadcast.factory</td>
  <td>org.apache.spark.broadcast.<br />HttpBroadcastFactory</td>
  <td>
    Which broadcast implementation to use.
  </td>
</tr>
<tr>
  <td>spark.locality.wait</td>
  <td>3000</td>
  <td>
    Number of milliseconds to wait to launch a data-local task before giving up and launching it
    on a less-local node. The same wait will be used to step through multiple locality levels
    (process-local, node-local, rack-local and then any). It is also possible to customize the
    waiting time for each level by setting <code>spark.locality.wait.node</code>, etc.
    You should increase this setting if your tasks are long and see poor locality, but the
    default usually works well.
  </td>
</tr>
<tr>
  <td>spark.locality.wait.process</td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for process locality. This affects tasks that attempt to access
    cached data in a particular executor process.
  </td>
</tr>
<tr>
  <td>spark.locality.wait.node</td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for node locality. For example, you can set this to 0 to skip
    node locality and search immediately for rack locality (if your cluster has rack information).
  </td>
</tr>
<tr>
  <td>spark.locality.wait.rack</td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for rack locality.
  </td>
</tr>
<tr>
  <td>spark.worker.timeout</td>
  <td>60</td>
  <td>
    Number of seconds after which the standalone deploy master considers a worker lost if it
    receives no heartbeats.
  </td>
</tr>
<tr>
  <td>spark.akka.frameSize</td>
  <td>10</td>
  <td>
    Maximum message size to allow in "control plane" communication (for serialized tasks and task
    results), in MB. Increase this if your tasks need to send back large results to the driver
    (e.g. using <code>collect()</code> on a large dataset).
  </td>
</tr>
<tr>
  <td>spark.akka.threads</td>
  <td>4</td>
  <td>
    Number of actor threads to use for communication. Can be useful to increase on large clusters
    when the driver has a lot of CPU cores.
  </td>
</tr>
<tr>
  <td>spark.akka.timeout</td>
  <td>100</td>
  <td>
    Communication timeout between Spark nodes, in seconds.
  </td>
</tr>
<tr>
  <td>spark.akka.heartbeat.pauses</td>
  <td>600</td>
  <td>
     This is set to a larger value to disable failure detector that comes inbuilt akka. It can be enabled again, if you plan to use this feature (Not recommended). Acceptable heart beat pause in seconds for akka. This can be used to control sensitivity to gc pauses. Tune this in combination of `spark.akka.heartbeat.interval` and `spark.akka.failure-detector.threshold` if you need to.
  </td>
</tr>
<tr>
  <td>spark.akka.failure-detector.threshold</td>
  <td>300.0</td>
  <td>
     This is set to a larger value to disable failure detector that comes inbuilt akka. It can be enabled again, if you plan to use this feature (Not recommended). This maps to akka's `akka.remote.transport-failure-detector.threshold`. Tune this in combination of `spark.akka.heartbeat.pauses` and `spark.akka.heartbeat.interval` if you need to.
  </td>
</tr>
<tr>
  <td>spark.akka.heartbeat.interval</td>
  <td>1000</td>
  <td>
    This is set to a larger value to disable failure detector that comes inbuilt akka. It can be enabled again, if you plan to use this feature (Not recommended). A larger interval value in seconds reduces network overhead and a smaller value ( ~ 1 s) might be more informative for akka's failure detector. Tune this in combination of `spark.akka.heartbeat.pauses` and `spark.akka.failure-detector.threshold` if you need to. Only positive use case for using failure detector can be, a sensistive failure detector can help evict rogue executors really quick. However this is usually not the case as gc pauses and network lags are expected in a real spark cluster. Apart from that enabling this leads to a lot of exchanges of heart beats between nodes leading to flooding the network with those. 
  </td>
</tr>
<tr>
  <td>spark.driver.host</td>
  <td>(local hostname)</td>
  <td>
    Hostname or IP address for the driver to listen on.
  </td>
</tr>
<tr>
  <td>spark.driver.port</td>
  <td>(random)</td>
  <td>
    Port for the driver to listen on.
  </td>
</tr>
<tr>
  <td>spark.cleaner.ttl</td>
  <td>(infinite)</td>
  <td>
    Duration (seconds) of how long Spark will remember any metadata (stages generated, tasks generated, etc.).
    Periodic cleanups will ensure that metadata older than this duration will be forgetten. This is
    useful for running Spark for many hours / days (for example, running 24/7 in case of Spark Streaming
    applications). Note that any RDD that persists in memory for more than this duration will be cleared as well.
  </td>
</tr>
<tr>
  <td>spark.streaming.blockInterval</td>
  <td>200</td>
  <td>
    Duration (milliseconds) of how long to batch new objects coming from network receivers.
  </td>
</tr>
<tr>
  <td>spark.task.maxFailures</td>
  <td>4</td>
  <td>
    Number of individual task failures before giving up on the job.
    Should be greater than or equal to 1. Number of allowed retries = this value - 1.
  </td>
</tr>
<tr>
  <td>spark.broadcast.blockSize</td>
  <td>4096</td>
  <td>
    Size of each piece of a block in kilobytes for <code>TorrentBroadcastFactory</code>. 
    Too large a value decreases parallelism during broadcast (makes it slower); however, if it is too small, <code>BlockManager</code> might take a performance hit.
  </td>
</tr>
<tr>
  <td>spark.shuffle.consolidateFiles</td>
  <td>false</td>
  <td>
    If set to "true", consolidates intermediate files created during a shuffle. Creating fewer files can improve filesystem performance for shuffles with large numbers of reduce tasks. It is recommended to set this to "true" when using ext4 or xfs filesystems. On ext3, this option might degrade performance on machines with many (>8) cores due to filesystem limitations.
  </td>
</tr>
<tr>
  <td>spark.speculation</td>
  <td>false</td>
  <td>
    If set to "true", performs speculative execution of tasks. This means if one or more tasks are running slowly in a stage, they will be re-launched.
  </td>
</tr>
<tr>
  <td>spark.speculation.interval</td>
  <td>100</td>
  <td>
    How often Spark will check for tasks to speculate, in milliseconds.
  </td>
</tr>
<tr>
  <td>spark.speculation.quantile</td>
  <td>0.75</td>
  <td>
    Percentage of tasks which must be complete before speculation is enabled for a particular stage.
  </td>
</tr>
<tr>
  <td>spark.speculation.multiplier</td>
  <td>1.5</td>
  <td>
    How many times slower a task is than the median to be considered for speculation.
  </td>
</tr>
</table>

# Environment Variables

Certain Spark settings can also be configured through environment variables, which are read from the `conf/spark-env.sh`
script in the directory where Spark is installed (or `conf/spark-env.cmd` on Windows). These variables are meant to be for machine-specific settings, such
as library search paths. While Java system properties can also be set here, for application settings, we recommend setting
these properties within the application instead of in `spark-env.sh` so that different applications can use different
settings.

Note that `conf/spark-env.sh` does not exist by default when Spark is installed. However, you can copy
`conf/spark-env.sh.template` to create it. Make sure you make the copy executable.

The following variables can be set in `spark-env.sh`:

* `JAVA_HOME`, the location where Java is installed (if it's not on your default `PATH`)
* `PYSPARK_PYTHON`, the Python binary to use for PySpark
* `SPARK_LOCAL_IP`, to configure which IP address of the machine to bind to.
* `SPARK_LIBRARY_PATH`, to add search directories for native libraries.
* `SPARK_CLASSPATH`, to add elements to Spark's classpath that you want to be present for _all_ applications.
   Note that applications can also add dependencies for themselves through `SparkContext.addJar` -- we recommend
   doing that when possible.
* `SPARK_JAVA_OPTS`, to add JVM options. This includes Java options like garbage collector settings and any system
   properties that you'd like to pass with `-D` (e.g., `-Dspark.local.dir=/disk1,/disk2`). 
* Options for the Spark [standalone cluster scripts](spark-standalone.html#cluster-launch-scripts), such as number of cores
  to use on each machine and maximum memory.

Since `spark-env.sh` is a shell script, some of these can be set programmatically -- for example, you might
compute `SPARK_LOCAL_IP` by looking up the IP of a specific network interface.

# Configuring Logging

Spark uses [log4j](http://logging.apache.org/log4j/) for logging. You can configure it by adding a `log4j.properties`
file in the `conf` directory. One way to start is to copy the existing `log4j.properties.template` located there.
