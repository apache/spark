---
layout: global
title: Spark Configuration
---

Spark provides three main locations to configure the system:

* The [`conf/spark-env.sh` script](#environment-variables-in-spark-envsh), in which you can set environment variables
  that affect how the JVM is launched, such as, most notably, the amount of memory per JVM.
* [Java system properties](#system-properties), which control internal configuration parameters and can be set either
  programmatically (by calling `System.setProperty` *before* creating a `SparkContext`) or through the
  `SPARK_JAVA_OPTS` environment variable in `spark-env.sh`.
* [Logging configuration](#configuring-logging), which is done through `log4j.properties`.


# Environment Variables in spark-env.sh

Spark determines how to initialize the JVM on worker nodes, or even on the local node when you run `spark-shell`,
by running the `conf/spark-env.sh` script in the directory where it is installed. This script does not exist by default
in the Git repository, but but you can create it by copying `conf/spark-env.sh.template`. Make sure that you make
the copy executable.

Inside `spark-env.sh`, you can set the following environment variables:

* `SCALA_HOME` to point to your Scala installation.
* `MESOS_NATIVE_LIBRARY` if you are [running on a Mesos cluster]({{HOME_PATH}}running-on-mesos.html).
* `SPARK_MEM` to set the amount of memory used per node (this should be in the same format as the JVM's -Xmx option, e.g. `300m` or `1g`)
* `SPARK_JAVA_OPTS` to add JVM options. This includes any system properties that you'd like to pass with `-D`.
* `SPARK_CLASSPATH` to add elements to Spark's classpath.
* `SPARK_LIBRARY_PATH` to add search directories for native libraries.

The most important things to set first will be `SCALA_HOME`, without which `spark-shell` cannot run, and `MESOS_NATIVE_LIBRARY`
if running on Mesos. The next setting will probably be the memory (`SPARK_MEM`). Make sure you set it high enough to be able to run your job but lower than the total memory on the machines (leave at least 1 GB for the operating system).


# System Properties

To set a system property for configuring Spark, you need to either pass it with a -D flag to the JVM (for example `java -Dspark.cores.max=5 MyProgram`) or call `System.setProperty` in your code *before* creating your Spark context, as follows:

{% highlight scala %}
System.setProperty("spark.cores.max", "5")
val sc = new SparkContext(...)
{% endhighlight %}

Most of the configurable system properties control internal settings that have reasonable default values. However,
there are at least four properties that you will commonly want to control:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>spark.serializer</td>
  <td>spark.JavaSerializer</td>
  <td>
    Class to use for serializing objects that will be sent over the network or need to be cached
    in serialized form. The default of Java serialization works with any Serializable Java object but is
    quite slow, so we recommend <a href="{{HOME_PATH}}tuning.html">using <code>spark.KryoSerializer</code>
    and configuring Kryo serialization</a> when speed is necessary. Can be any subclass of 
    <a href="{{HOME_PATH}}api/core/index.html#spark.Serializer"><code>spark.Serializer</code></a>).
  </td>
</tr>
<tr>
  <td>spark.kryo.registrator</td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, set this class to register your custom classes with Kryo.
    You need to set it to a class that extends 
    <a href="{{HOME_PATH}}api/core/index.html#spark.KryoRegistrator"><code>spark.KryoRegistrator</code></a>).
    See the <a href="{{HOME_PATH}}tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
</tr>
<tr>
  <td>spark.local.dir</td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored
    on disk. This should be on a fast, local disk in your system.
  </td>
</tr>
<tr>
  <td>spark.cores.max</td>
  <td>(infinite)</td>
  <td>
    When running on a <a href="{{HOME_PATH}}spark-standalone.html">standalone deploy cluster</a> or a
    <a href="{{HOME_PATH}}running-on-mesos.html#mesos-run-modes">Mesos cluster in "coarse-grained"
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
    <a href="{{HOME_PATH}}running-on-mesos.html#mesos-run-modes">"coarse-grained" sharing mode</a>,
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
  <td>spark.blockManager.compress</td>
  <td>false</td>
  <td>
    Set to "true" to have Spark compress map output files, RDDs that get cached on disk,
    and RDDs that get cached in serialized form. Generally a good idea when dealing with
    large datasets, but might add some CPU overhead.
  </td>
</tr>
<tr>
  <td>spark.broadcast.compress</td>
  <td>false</td>
  <td>
    Set to "true" to have Spark compress broadcast variables before sending them.
    Generally a good idea when broadcasting large values.
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
  <td>spark.blockManager.parallelFetches</td>
  <td>4</td>
  <td>
    Number of map output files to fetch concurrently from each reduce task.
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
  <td>spark.broadcast. HttpBroadcastFactory</td>
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
  <td>spark.master.host</td>
  <td>(local hostname)</td>
  <td>
    Hostname for the master to listen on (it will bind to this hostname's IP address).
  </td>
</tr>
<tr>
  <td>spark.master.port</td>
  <td>(random)</td>
  <td>
    Port for the master to listen on.
  </td>
</tr>
</table>

# Configuring Logging

Spark uses [log4j](http://logging.apache.org/log4j/) for logging. You can configure it by adding a `log4j.properties`
file in the `conf` directory. One way to start is to copy the existing `log4j.properties.template` located there.
