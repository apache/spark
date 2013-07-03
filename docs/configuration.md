---
layout: global
title: Spark Configuration
---

Spark provides three main locations to configure the system:

* [Environment variables](#environment-variables) for launching Spark workers, which can
  be set either in your driver program or in the `conf/spark-env.sh` script.
* [Java system properties](#system-properties), which control internal configuration parameters and can be set either
  programmatically (by calling `System.setProperty` *before* creating a `SparkContext`) or through the
  `SPARK_JAVA_OPTS` environment variable in `spark-env.sh`.
* [Logging configuration](#configuring-logging), which is done through `log4j.properties`.


# Environment Variables

Spark determines how to initialize the JVM on worker nodes, or even on the local node when you run `spark-shell`,
by running the `conf/spark-env.sh` script in the directory where it is installed. This script does not exist by default
in the Git repository, but but you can create it by copying `conf/spark-env.sh.template`. Make sure that you make
the copy executable.

Inside `spark-env.sh`, you *must* set at least the following two variables:

* `SCALA_HOME`, to point to your Scala installation, or `SCALA_LIBRARY_PATH` to point to the directory for Scala
  library JARs (if you install Scala as a Debian or RPM package, there is no `SCALA_HOME`, but these libraries
  are in a separate path, typically /usr/share/java; look for `scala-library.jar`).
* `MESOS_NATIVE_LIBRARY`, if you are [running on a Mesos cluster](running-on-mesos.html).

In addition, there are four other variables that control execution. These should be set *in the environment that
launches the job's driver program* instead of `spark-env.sh`, because they will be automatically propagated to
workers. Setting these per-job instead of in `spark-env.sh` ensures that different jobs can have different settings
for these variables.

* `SPARK_JAVA_OPTS`, to add JVM options. This includes any system properties that you'd like to pass with `-D`.
* `SPARK_CLASSPATH`, to add elements to Spark's classpath.
* `SPARK_LIBRARY_PATH`, to add search directories for native libraries.
* `SPARK_MEM`, to set the amount of memory used per node. This should be in the same format as the 
   JVM's -Xmx option, e.g. `300m` or `1g`. Note that this option will soon be deprecated in favor of
   the `spark.executor.memory` system property, so we recommend using that in new code.

Beware that if you do set these variables in `spark-env.sh`, they will override the values set by user programs,
which is undesirable; if you prefer, you can choose to have `spark-env.sh` set them only if the user program
hasn't, as follows:

{% highlight bash %}
if [ -z "$SPARK_JAVA_OPTS" ] ; then
  SPARK_JAVA_OPTS="-verbose:gc"
fi
{% endhighlight %}

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
    Amount of memory to use per executor process, in the same format as JVM memory strings (e.g. `512m`, `2g`).
  </td>
</tr>
<tr>
  <td>spark.serializer</td>
  <td>spark.JavaSerializer</td>
  <td>
    Class to use for serializing objects that will be sent over the network or need to be cached
    in serialized form. The default of Java serialization works with any Serializable Java object but is
    quite slow, so we recommend <a href="tuning.html">using <code>spark.KryoSerializer</code>
    and configuring Kryo serialization</a> when speed is necessary. Can be any subclass of 
    <a href="api/core/index.html#spark.Serializer"><code>spark.Serializer</code></a>).
  </td>
</tr>
<tr>
  <td>spark.kryo.registrator</td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, set this class to register your custom classes with Kryo.
    You need to set it to a class that extends 
    <a href="api/core/index.html#spark.KryoRegistrator"><code>spark.KryoRegistrator</code></a>).
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
</tr>
<tr>
  <td>spark.local.dir</td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored
    on disk. This should be on a fast, local disk in your system. It can also be a comma-separated
    list of multiple directories.
  </td>
</tr>
<tr>
  <td>spark.cores.max</td>
  <td>(infinite)</td>
  <td>
    When running on a <a href="spark-standalone.html">standalone deploy cluster</a> or a
    <a href="running-on-mesos.html#mesos-run-modes">Mesos cluster in "coarse-grained"
    sharing mode</a>, how many CPU cores to request at most. The default will use all available cores.
  </td>
</tr>
</table>


Apart from these, the following properties are also available, and may be useful in some situations:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
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
  <td>spark.ui.port</td>
  <td>(random)</td>
  <td>
    Port for your application's dashboard, which shows memory usage of each RDD.
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
  <td>spark.JavaSerializer</td>
  <td>
    Serializer class to use for closures. Generally Java is fine unless your distributed functions
    (e.g. map functions) reference large objects in the driver program.
  </td>
</tr>
<tr>
  <td>spark.kryoserializer.buffer.mb</td>
  <td>32</td>
  <td>
    Maximum object size to allow within Kryo (the library needs to create a buffer at least as
    large as the largest single object you'll serialize). Increase this if you get a "buffer limit
    exceeded" exception inside Kryo. Note that there will be one buffer <i>per core</i> on each worker.
  </td>
</tr>
<tr>
  <td>spark.broadcast.factory</td>
  <td>spark.broadcast.HttpBroadcastFactory</td>
  <td>
    Which broadcast implementation to use.
  </td>
</tr>
<tr>
  <td>spark.locality.wait</td>
  <td>3000</td>
  <td>
    Number of milliseconds to wait to launch a data-local task before giving up and launching it
    in a non-data-local location. You should increase this if your tasks are long and you are seeing
    poor data locality, but the default generally works well.
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
  <td>20</td>
  <td>
    Communication timeout between Spark nodes, in seconds.
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
  <td>(disable)</td>
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

</table>

# Configuring Logging

Spark uses [log4j](http://logging.apache.org/log4j/) for logging. You can configure it by adding a `log4j.properties`
file in the `conf` directory. One way to start is to copy the existing `log4j.properties.template` located there.
