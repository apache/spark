---
layout: global
title: Spark Programming Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}


# Overview

At a high level, every Spark application consists of a *driver program* that runs the user's `main` function and executes various *parallel operations* on a cluster. The main abstraction Spark provides is a *resilient distributed dataset* (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to *persist* an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

A second abstraction in Spark is *shared variables* that can be used in parallel operations. By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. Spark supports two types of shared variables: *broadcast variables*, which can be used to cache a value in memory on all nodes, and *accumulators*, which are variables that are only "added" to, such as counters and sums.

This guide shows each of these features and walks through some samples. It assumes some familiarity with Scala, especially with the syntax for [closures](http://www.scala-lang.org/node/133). Note that you can also run Spark interactively using the `bin/spark-shell` script. We highly recommend doing that to follow along!

# Linking with Spark

Spark {{site.SPARK_VERSION}} uses Scala {{site.SCALA_BINARY_VERSION}}. If you write applications in Scala, you will need to use a compatible Scala version (e.g. {{site.SCALA_BINARY_VERSION}}.X) -- newer major versions may not work.

To write a Spark application, you need to add a dependency on Spark. If you use SBT or Maven, Spark is available through Maven Central at:

    groupId = org.apache.spark
    artifactId = spark-core_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION}}

In addition, if you wish to access an HDFS cluster, you need to add a dependency on `hadoop-client` for your version of HDFS:

    groupId = org.apache.hadoop
    artifactId = hadoop-client
    version = <your-hdfs-version>

For other build systems, you can run `sbt/sbt assembly` to pack Spark and its dependencies into one JAR (`assembly/target/scala-{{site.SCALA_BINARY_VERSION}}/spark-assembly-{{site.SPARK_VERSION}}-hadoop*.jar`), then add this to your CLASSPATH. Set the HDFS version as described [here](index.html#a-note-about-hadoop-versions).

Finally, you need to import some Spark classes and implicit conversions into your program. Add the following lines:

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
{% endhighlight %}

# Initializing Spark

The first thing a Spark program must do is to create a `SparkContext` object, which tells Spark how to access a cluster.
This is done through the following constructor:

{% highlight scala %}
new SparkContext(master, appName, [sparkHome], [jars])
{% endhighlight %}

or through `new SparkContext(conf)`, which takes a [SparkConf](api/core/index.html#org.apache.spark.SparkConf)
object for more advanced configuration.

The `master` parameter is a string specifying a [Spark or Mesos cluster URL](#master-urls) to connect to, or a special "local" string to run in local mode, as described below. `appName` is a name for your application, which will be shown in the cluster web UI. Finally, the last two parameters are needed to deploy your code to a cluster if running in distributed mode, as described later.

In the Spark shell, a special interpreter-aware SparkContext is already created for you, in the variable called `sc`. Making your own SparkContext will not work. You can set which master the context connects to using the `MASTER` environment variable, and you can add JARs to the classpath with the `ADD_JARS` variable. For example, to run `bin/spark-shell` on exactly four cores, use

{% highlight bash %}
$ MASTER=local[4] ./bin/spark-shell
{% endhighlight %}

Or, to also add `code.jar` to its classpath, use:

{% highlight bash %}
$ MASTER=local[4] ADD_JARS=code.jar ./bin/spark-shell
{% endhighlight %}

### Master URLs

The master URL passed to Spark can be in one of the following formats:

<table class="table">
<tr><th>Master URL</th><th>Meaning</th></tr>
<tr><td> local </td><td> Run Spark locally with one worker thread (i.e. no parallelism at all). </td></tr>
<tr><td> local[K] </td><td> Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
<tr><td> local[*] </td><td> Run Spark locally with as many worker threads as logical cores on your machine.</td></tr>
</td></tr>
<tr><td> spark://HOST:PORT </td><td> Connect to the given <a href="spark-standalone.html">Spark standalone
        cluster</a> master. The port must be whichever one your master is configured to use, which is 7077 by default.
</td></tr>
<tr><td> mesos://HOST:PORT </td><td> Connect to the given <a href="running-on-mesos.html">Mesos</a> cluster.
        The host parameter is the hostname of the Mesos master. The port must be whichever one the master is configured to use,
        which is 5050 by default.
</td></tr>
</table>

If no master URL is specified, the spark shell defaults to "local[*]".

For running on YARN, Spark launches an instance of the standalone deploy cluster within YARN; see [running on YARN](running-on-yarn.html) for details.

### Deploying Code on a Cluster

If you want to run your application on a cluster, you will need to specify the two optional parameters to `SparkContext` to let it find your code:

* `sparkHome`: The path at which Spark is installed on your worker machines (it should be the same on all of them).
* `jars`: A list of JAR files on the local machine containing your application's code and any dependencies, which Spark will deploy to all the worker nodes. You'll need to package your application into a set of JARs using your build system. For example, if you're using SBT, the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin is a good way to make a single JAR with your code and dependencies.

If you run `bin/spark-shell` on a cluster, you can add JARs to it by specifying the `ADD_JARS` environment variable before you launch it.  This variable should contain a comma-separated list of JARs. For example, `ADD_JARS=a.jar,b.jar ./bin/spark-shell` will launch a shell with `a.jar` and `b.jar` on its classpath. In addition, any new classes you define in the shell will automatically be distributed.

# Resilient Distributed Datasets (RDDs)

Spark revolves around the concept of a _resilient distributed dataset_ (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are currently two types of RDDs: *parallelized collections*, which take an existing Scala collection and run functions on it in parallel, and *Hadoop datasets*, which run functions on each record of a file in Hadoop distributed file system or any other storage system supported by Hadoop. Both types of RDDs can be operated on through the same methods.

## Parallelized Collections

Parallelized collections are created by calling `SparkContext`'s `parallelize` method on an existing Scala collection (a `Seq` object). The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is some interpreter output showing how to create a parallel collection from an array:

{% highlight scala %}
scala> val data = Array(1, 2, 3, 4, 5)
data: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val distData = sc.parallelize(data)
distData: spark.RDD[Int] = spark.ParallelCollection@10d13e3e
{% endhighlight %}

Once created, the distributed dataset (`distData` here) can be operated on in parallel. For example, we might call `distData.reduce(_ + _)` to add up the elements of the array. We describe operations on distributed datasets later on.

One important parameter for parallel collections is the number of *slices* to cut the dataset into. Spark will run one task for each slice of the cluster. Typically you want 2-4 slices for each CPU in your cluster. Normally, Spark tries to set the number of slices automatically based on your cluster. However, you can also set it manually by passing it as a second parameter to `parallelize` (e.g. `sc.parallelize(data, 10)`).

## Hadoop Datasets

Spark can create distributed datasets from any file stored in the Hadoop distributed file system (HDFS) or other storage systems supported by Hadoop (including your local file system, [Amazon S3](http://wiki.apache.org/hadoop/AmazonS3), Hypertable, HBase, etc). Spark supports text files, [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), and any other Hadoop InputFormat.

Text file RDDs can be created using `SparkContext`'s `textFile` method. This method takes an URI for the file (either a local path on the machine, or a `hdfs://`, `s3n://`, `kfs://`, etc URI). Here is an example invocation:

{% highlight scala %}
scala> val distFile = sc.textFile("data.txt")
distFile: spark.RDD[String] = spark.HadoopRDD@1d4cee08
{% endhighlight %}

Once created, `distFile` can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the `map` and `reduce` operations as follows: `distFile.map(_.size).reduce(_ + _)`.

The `textFile` method also takes an optional second argument for controlling the number of slices of the file. By default, Spark creates one slice for each block of the file (blocks being 64MB by default in HDFS), but you can also ask for a higher number of slices by passing a larger value. Note that you cannot have fewer slices than blocks.

For [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), use SparkContext's `sequenceFile[K, V]` method where `K` and `V` are the types of key and values in the file. These should be subclasses of Hadoop's [Writable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html) interface, like [IntWritable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/IntWritable.html) and [Text](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Text.html). In addition, Spark allows you to specify native types for a few common Writables; for example, `sequenceFile[Int, String]` will automatically read IntWritables and Texts.

Finally, for other Hadoop InputFormats, you can use the `SparkContext.hadoopRDD` method, which takes an arbitrary `JobConf` and input format class, key class and value class. Set these the same way you would for a Hadoop job with your input source.

## RDD Operations

RDDs support two types of operations: *transformations*, which create a new dataset from an existing one, and *actions*, which return a value to the driver program after running a computation on the dataset. For example, `map` is a transformation that passes each dataset element through a function and returns a new distributed dataset representing the results. On the other hand, `reduce` is an action that aggregates all the elements of the dataset using some function and returns the final result to the driver program (although there is also a parallel `reduceByKey` that returns a distributed dataset).

All transformations in Spark are <i>lazy</i>, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently -- for example, we can realize that a dataset created through `map` will be used in a `reduce` and return only the result of the `reduce` to the driver, rather than the larger mapped dataset.

By default, each transformed RDD is recomputed each time you run an action on it. However, you may also *persist* an RDD in memory using the `persist` (or `cache`) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting datasets on disk, or replicated across the cluster. The next section in this document describes these options.

The following tables list the transformations and actions currently supported (see also the [RDD API doc](api/core/index.html#org.apache.spark.rdd.RDD) for details):

### Transformations

<table class="table">
<tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>map</b>(<i>func</i>) </td>
  <td> Return a new distributed dataset formed by passing each element of the source through a function <i>func</i>. </td>
</tr>
<tr>
  <td> <b>filter</b>(<i>func</i>) </td>
  <td> Return a new dataset formed by selecting those elements of the source on which <i>func</i> returns true. </td>
</tr>
<tr>
  <td> <b>flatMap</b>(<i>func</i>) </td>
  <td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td>
</tr>
<tr>
  <td> <b>mapPartitions</b>(<i>func</i>) </td>
  <td> Similar to map, but runs separately on each partition (block) of the RDD, so <i>func</i> must be of type
    Iterator[T] => Iterator[U] when running on an RDD of type T. </td>
</tr>
<tr>
  <td> <b>mapPartitionsWithIndex</b>(<i>func</i>) </td>
  <td> Similar to mapPartitions, but also provides <i>func</i> with an integer value representing the index of
  the partition, so <i>func</i> must be of type (Int, Iterator[T]) => Iterator[U] when running on an RDD of type T.
  </td>
</tr>
<tr>
  <td> <b>sample</b>(<i>withReplacement</i>, <i>fraction</i>, <i>seed</i>) </td>
  <td> Sample a fraction <i>fraction</i> of the data, with or without replacement, using a given random number generator seed. </td>
</tr>
<tr>
  <td> <b>union</b>(<i>otherDataset</i>) </td>
  <td> Return a new dataset that contains the union of the elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>distinct</b>([<i>numTasks</i>])) </td>
  <td> Return a new dataset that contains the distinct elements of the source dataset.</td>
</tr>
<tr>
  <td> <b>groupByKey</b>([<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, Seq[V]) pairs. <br />
<b>Note:</b> By default, if the RDD already has a partitioner,  the task number is decided by the partition number of the partitioner, or else relies on the value of <code>spark.default.parallelism</code> if the property is set , otherwise depends on the partition number of the RDD. You can pass an optional <code>numTasks</code> argument to set a different number of tasks.
</td>
</tr>
<tr>
  <td> <b>reduceByKey</b>(<i>func</i>, [<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>sortByKey</b>([<i>ascending</i>], [<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean <code>ascending</code> argument.</td>
</tr>
<tr>
  <td> <b>join</b>(<i>otherDataset</i>, [<i>numTasks</i>]) </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. </td>
</tr>
<tr>
  <td> <b>cogroup</b>(<i>otherDataset</i>, [<i>numTasks</i>]) </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, Seq[V], Seq[W]) tuples. This operation is also called <code>groupWith</code>. </td>
</tr>
<tr>
  <td> <b>cartesian</b>(<i>otherDataset</i>) </td>
  <td> When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). </td>
</tr>
</table>

A complete list of transformations is available in the [RDD API doc](api/core/index.html#org.apache.spark.rdd.RDD).

### Actions

<table class="table">
<tr><th>Action</th><th>Meaning</th></tr>
<tr>
  <td> <b>reduce</b>(<i>func</i>) </td>
  <td> Aggregate the elements of the dataset using a function <i>func</i> (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. </td>
</tr>
<tr>
  <td> <b>collect</b>() </td>
  <td> Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. </td>
</tr>
<tr>
  <td> <b>count</b>() </td>
  <td> Return the number of elements in the dataset. </td>
</tr>
<tr>
  <td> <b>first</b>() </td>
  <td> Return the first element of the dataset (similar to take(1)). </td>
</tr>
<tr>
  <td> <b>take</b>(<i>n</i>) </td>
  <td> Return an array with the first <i>n</i> elements of the dataset. Note that this is currently not executed in parallel. Instead, the driver program computes all the elements. </td>
</tr>
<tr>
  <td> <b>takeSample</b>(<i>withReplacement</i>, <i>num</i>, <i>seed</i>) </td>
  <td> Return an array with a random sample of <i>num</i> elements of the dataset, with or without replacement, using the given random number generator seed. </td>
</tr>
<tr>
  <td> <b>saveAsTextFile</b>(<i>path</i>) </td>
  <td> Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. </td>
</tr>
<tr>
  <td> <b>saveAsSequenceFile</b>(<i>path</i>) </td>
  <td> Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is only available on RDDs of key-value pairs that either implement Hadoop's Writable interface or are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). </td>
</tr>
<tr>
  <td> <b>countByKey</b>() </td>
  <td> Only available on RDDs of type (K, V). Returns a `Map` of (K, Int) pairs with the count of each key. </td>
</tr>
<tr>
  <td> <b>foreach</b>(<i>func</i>) </td>
  <td> Run a function <i>func</i> on each element of the dataset. This is usually done for side effects such as updating an accumulator variable (see below) or interacting with external storage systems. </td>
</tr>
</table>

A complete list of actions is available in the [RDD API doc](api/core/index.html#org.apache.spark.rdd.RDD).

## RDD Persistence

One of the most important capabilities in Spark is *persisting* (or *caching*) a dataset in memory
across operations. When you persist an RDD, each node stores any slices of it that it computes in
memory and reuses them in other actions on that dataset (or datasets derived from it). This allows
future actions to be much faster (often by more than 10x). Caching is a key tool for building
iterative algorithms with Spark and for interactive use from the interpreter.

You can mark an RDD to be persisted using the `persist()` or `cache()` methods on it. The first time
it is computed in an action, it will be kept in memory on the nodes. The cache is fault-tolerant --
if any partition of an RDD is lost, it will automatically be recomputed using the transformations
that originally created it.

In addition, each RDD can be stored using a different *storage level*, allowing you, for example, to
persist the dataset on disk, or persist it in memory but as serialized Java objects (to save space),
or replicate it across nodes, or store the data in off-heap memory in [Tachyon](http://tachyon-project.org/).
These levels are chosen by passing a
[`org.apache.spark.storage.StorageLevel`](api/core/index.html#org.apache.spark.storage.StorageLevel)
object to `persist()`. The `cache()` method is a shorthand for using the default storage level,
which is `StorageLevel.MEMORY_ONLY` (store deserialized objects in memory). The complete set of
available storage levels is:

<table class="table">
<tr><th style="width:23%">Storage Level</th><th>Meaning</th></tr>
<tr>
  <td> MEMORY_ONLY </td>
  <td> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will
    not be cached and will be recomputed on the fly each time they're needed. This is the default level. </td>
</tr>
<tr>
  <td> MEMORY_AND_DISK </td>
  <td> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the
    partitions that don't fit on disk, and read them from there when they're needed. </td>
</tr>
<tr>
  <td> MEMORY_ONLY_SER </td>
  <td> Store RDD as <i>serialized</i> Java objects (one byte array per partition).
    This is generally more space-efficient than deserialized objects, especially when using a
    <a href="tuning.html">fast serializer</a>, but more CPU-intensive to read.
  </td>
</tr>
<tr>
  <td> MEMORY_AND_DISK_SER </td>
  <td> Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of
    recomputing them on the fly each time they're needed. </td>
</tr>
<tr>
  <td> OFF_HEAP  </td>
  <td> Store RDD in a <i>serialized</i> format in Tachyon.
    This is generally more space-efficient than deserialized objects, especially when using a
    <a href="tuning.html">fast serializer</a>, but more CPU-intensive to read.
    This also significantly reduces the overheads of GC.
  </td>
</tr>
<tr>
  <td> DISK_ONLY </td>
  <td> Store the RDD partitions only on disk. </td>
</tr>
<tr>
  <td> MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.  </td>
  <td> Same as the levels above, but replicate each partition on two cluster nodes. </td>
</tr>
</table>

### Which Storage Level to Choose?

Spark's storage levels are meant to provide different trade-offs between memory usage and CPU
efficiency. It allows uses to choose memory, disk, or Tachyon for storing data. We recommend going
through the following process to select one:

* If your RDDs fit comfortably with the default storage level (`MEMORY_ONLY`), leave them that way.
  This is the most CPU-efficient option, allowing operations on the RDDs to run as fast as possible.

* If not, try using `MEMORY_ONLY_SER` and [selecting a fast serialization library](tuning.html) to
make the objects much more space-efficient, but still reasonably fast to access. You can also use
`OFF_HEAP` mode to store the data off the heap in [Tachyon](http://tachyon-project.org/). This will
significantly reduce JVM GC overhead.

* Don't spill to disk unless the functions that computed your datasets are expensive, or they filter
a large amount of the data. Otherwise, recomputing a partition is about as fast as reading it from
disk.

* Use the replicated storage levels if you want fast fault recovery (e.g. if using Spark to serve
requests from a web application). *All* the storage levels provide full fault tolerance by
recomputing lost data, but the replicated ones let you continue running tasks on the RDD without
waiting to recompute a lost partition.

If you want to define your own storage level (say, with replication factor of 3 instead of 2), then
use the function factor method `apply()` of the
[`StorageLevel`](api/core/index.html#org.apache.spark.storage.StorageLevel$) singleton object.

Spark has a block manager inside the Executors that let you chose memory, disk, or off-heap. The
latter is for storing RDDs off-heap outside the Executor JVM on top of the memory management system
[Tachyon](http://tachyon-project.org/). This mode has the following advantages:

* Cached data will not be lost if individual executors crash.
* Executors can have a smaller memory footprint, allowing you to run more executors on the same
machine as the bulk of the memory will be inside Tachyon.
* Reduced GC overhead since data is stored in Tachyon.

# Shared Variables

Normally, when a function passed to a Spark operation (such as `map` or `reduce`) is executed on a
remote cluster node, it works on separate copies of all the variables used in the function. These
variables are copied to each machine, and no updates to the variables on the remote machine are
propagated back to the driver program. Supporting general, read-write shared variables across tasks
would be inefficient. However, Spark does provide two limited types of *shared variables* for two
common usage patterns: broadcast variables and accumulators.

## Broadcast Variables

Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather
than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a
large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables
using efficient broadcast algorithms to reduce communication cost.

Broadcast variables are created from a variable `v` by calling `SparkContext.broadcast(v)`. The
broadcast variable is a wrapper around `v`, and its value can be accessed by calling the `value`
method. The interpreter session below shows this:

{% highlight scala %}
scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar: spark.Broadcast[Array[Int]] = spark.Broadcast(b5c40191-a864-4c7d-b9bf-d87e1a4e787c)

scala> broadcastVar.value
res0: Array[Int] = Array(1, 2, 3)
{% endhighlight %}

After the broadcast variable is created, it should be used instead of the value `v` in any functions
run on the cluster so that `v` is not shipped to the nodes more than once. In addition, the object
`v` should not be modified after it is broadcast in order to ensure that all nodes get the same
value of the broadcast variable (e.g. if the variable is shipped to a new node later).

## Accumulators

Accumulators are variables that are only "added" to through an associative operation and can
therefore be efficiently supported in parallel. They can be used to implement counters (as in
MapReduce) or sums. Spark natively supports accumulators of numeric value types and standard mutable
collections, and programmers can add support for new types.

An accumulator is created from an initial value `v` by calling `SparkContext.accumulator(v)`. Tasks
running on the cluster can then add to it using the `+=` operator. However, they cannot read its
value. Only the driver program can read the accumulator's value, using its `value` method.

The interpreter session below shows an accumulator being used to add up the elements of an array:

{% highlight scala %}
scala> val accum = sc.accumulator(0)
accum: spark.Accumulator[Int] = 0

scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

scala> accum.value
res2: Int = 10
{% endhighlight %}


# Where to Go from Here

You can see some [example Spark programs](http://spark.apache.org/examples.html) on the Spark website.
In addition, Spark includes several samples in `examples/src/main/scala`. Some of them have both Spark versions and local (non-parallel) versions, allowing you to see what had to be changed to make the program run on a cluster. You can run them using by passing the class name to the `bin/run-example` script included in Spark; for example:

    ./bin/run-example org.apache.spark.examples.SparkPi

Each example program prints usage help when run without any arguments.

For help on optimizing your program, the [configuration](configuration.html) and
[tuning](tuning.html) guides provide information on best practices. They are especially important for
making sure that your data is stored in memory in an efficient format.
