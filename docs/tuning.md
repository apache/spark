---
layout: global
title: Tuning Spark
---

* This will become a table of contents (this text will be scraped).
{:toc}

Because of the in-memory nature of most Spark computations, Spark programs can be bottlenecked
by any resource in the cluster: CPU, network bandwidth, or memory.
Most often, if the data fits in memory, the bottleneck is network bandwidth, but sometimes, you
also need to do some tuning, such as
[storing RDDs in serialized form](scala-programming-guide.html#rdd-persistence), to
decrease memory usage.
This guide will cover two main topics: data serialization, which is crucial for good network
performance and can also reduce memory use, and memory tuning. We also sketch several smaller topics.

# Data Serialization

Serialization plays an important role in the performance of any distributed application.
Formats that are slow to serialize objects into, or consume a large number of
bytes, will greatly slow down the computation.
Often, this will be the first thing you should tune to optimize a Spark application.
Spark aims to strike a balance between convenience (allowing you to work with any Java type
in your operations) and performance. It provides two serialization libraries:

* [Java serialization](http://docs.oracle.com/javase/6/docs/api/java/io/Serializable.html):
  By default, Spark serializes objects using Java's `ObjectOutputStream` framework, and can work
  with any class you create that implements
  [`java.io.Serializable`](http://docs.oracle.com/javase/6/docs/api/java/io/Serializable.html).
  You can also control the performance of your serialization more closely by extending
  [`java.io.Externalizable`](http://docs.oracle.com/javase/6/docs/api/java/io/Externalizable.html).
  Java serialization is flexible but often quite slow, and leads to large
  serialized formats for many classes.
* [Kryo serialization](http://code.google.com/p/kryo/): Spark can also use
  the Kryo library (version 2) to serialize objects more quickly. Kryo is significantly
  faster and more compact than Java serialization (often as much as 10x), but does not support all
  `Serializable` types and requires you to *register* the classes you'll use in the program in advance
  for best performance.

You can switch to using Kryo by initializing your job with a [SparkConf](configuration.html#spark-properties)
and calling `conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")`.
This setting configures the serializer used for not only shuffling data between worker
nodes but also when serializing RDDs to disk.  The only reason Kryo is not the default is because of the custom
registration requirement, but we recommend trying it in any network-intensive application.

Finally, to register your classes with Kryo, create a public class that extends
[`org.apache.spark.serializer.KryoRegistrator`](api/core/index.html#org.apache.spark.serializer.KryoRegistrator) and set the
`spark.kryo.registrator` config property to point to it, as follows:

{% highlight scala %}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[MyClass1])
    kryo.register(classOf[MyClass2])
  }
}

val conf = new SparkConf().setMaster(...).setAppName(...)
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryo.registrator", "mypackage.MyRegistrator")
val sc = new SparkContext(conf)
{% endhighlight %}

The [Kryo documentation](http://code.google.com/p/kryo/) describes more advanced
registration options, such as adding custom serialization code.

If your objects are large, you may also need to increase the `spark.kryoserializer.buffer.mb`
config property. The default is 2, but this value needs to be large enough to hold the *largest*
object you will serialize.

Finally, if you don't register your classes, Kryo will still work, but it will have to store the
full class name with each object, which is wasteful.

# Memory Tuning

There are three considerations in tuning memory usage: the *amount* of memory used by your objects
(you may want your entire dataset to fit in memory), the *cost* of accessing those objects, and the
overhead of *garbage collection* (if you have high turnover in terms of objects).

By default, Java objects are fast to access, but can easily consume a factor of 2-5x more space
than the "raw" data inside their fields. This is due to several reasons:

* Each distinct Java object has an "object header", which is about 16 bytes and contains information
  such as a pointer to its class. For an object with very little data in it (say one `Int` field), this
  can be bigger than the data.
* Java Strings have about 40 bytes of overhead over the raw string data (since they store it in an
  array of `Char`s and keep extra data such as the length), and store each character
  as *two* bytes due to Unicode. Thus a 10-character string can easily consume 60 bytes.
* Common collection classes, such as `HashMap` and `LinkedList`, use linked data structures, where
  there is a "wrapper" object for each entry (e.g. `Map.Entry`). This object not only has a header,
  but also pointers (typically 8 bytes each) to the next object in the list.
* Collections of primitive types often store them as "boxed" objects such as `java.lang.Integer`.

This section will discuss how to determine the memory usage of your objects, and how to improve
it -- either by changing your data structures, or by storing data in a serialized format.
We will then cover tuning Spark's cache size and the Java garbage collector.

## Determining Memory Consumption

The best way to size the amount of memory consumption your dataset will require is to create an RDD, put it into cache, and look at the SparkContext logs on your driver program. The logs will tell you how much memory each partition is consuming, which you can aggregate to get the total size of the RDD. You will see messages like this:

    INFO BlockManagerMasterActor: Added rdd_0_1 in memory on mbk.local:50311 (size: 717.5 KB, free: 332.3 MB)

This means that partition 1 of RDD 0 consumed 717.5 KB.

## Tuning Data Structures

The first way to reduce memory consumption is to avoid the Java features that add overhead, such as
pointer-based data structures and wrapper objects. There are several ways to do this:

1. Design your data structures to prefer arrays of objects, and primitive types, instead of the
   standard Java or Scala collection classes (e.g. `HashMap`). The [fastutil](http://fastutil.di.unimi.it)
   library provides convenient collection classes for primitive types that are compatible with the
   Java standard library.
2. Avoid nested structures with a lot of small objects and pointers when possible.
3. Consider using numeric IDs or enumeration objects instead of strings for keys.
4. If you have less than 32 GB of RAM, set the JVM flag `-XX:+UseCompressedOops` to make pointers be
   four bytes instead of eight. You can add these options in
   [`spark-env.sh`](configuration.html#environment-variables-in-spark-envsh).

## Serialized RDD Storage

When your objects are still too large to efficiently store despite this tuning, a much simpler way
to reduce memory usage is to store them in *serialized* form, using the serialized StorageLevels in
the [RDD persistence API](scala-programming-guide.html#rdd-persistence), such as `MEMORY_ONLY_SER`.
Spark will then store each RDD partition as one large byte array.
The only downside of storing data in serialized form is slower access times, due to having to
deserialize each object on the fly.
We highly recommend [using Kryo](#data-serialization) if you want to cache data in serialized form, as
it leads to much smaller sizes than Java serialization (and certainly than raw Java objects).

## Garbage Collection Tuning

JVM garbage collection can be a problem when you have large "churn" in terms of the RDDs
stored by your program. (It is usually not a problem in programs that just read an RDD once
and then run many operations on it.) When Java needs to evict old objects to make room for new ones, it will
need to trace through all your Java objects and find the unused ones. The main point to remember here is
that *the cost of garbage collection is proportional to the number of Java objects*, so using data
structures with fewer objects (e.g. an array of `Int`s instead of a `LinkedList`) greatly lowers
this cost. An even better method is to persist objects in serialized form, as described above: now
there will be only *one* object (a byte array) per RDD partition. Before trying other
techniques, the first thing to try if GC is a problem is to use [serialized caching](#serialized-rdd-storage).

GC can also be a problem due to interference between your tasks' working memory (the
amount of space needed to run the task) and the RDDs cached on your nodes. We will discuss how to control
the space allocated to the RDD cache to mitigate this.

**Measuring the Impact of GC**

The first step in GC tuning is to collect statistics on how frequently garbage collection occurs and the amount of
time spent GC. This can be done by adding `-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps` to your
`SPARK_JAVA_OPTS` environment variable. Next time your Spark job is run, you will see messages printed in the worker's logs
each time a garbage collection occurs. Note these logs will be on your cluster's worker nodes (in the `stdout` files in
their work directories), *not* on your driver program.

**Cache Size Tuning**

One important configuration parameter for GC is the amount of memory that should be used for caching RDDs.
By default, Spark uses 66% of the configured executor memory (`spark.executor.memory` or `SPARK_MEM`) to
cache RDDs. This means that 33% of memory is available for any objects created during task execution.

In case your tasks slow down and you find that your JVM is garbage-collecting frequently or running out of
memory, lowering this value will help reduce the memory consumption. To change this to say 50%, you can call
`conf.set("spark.storage.memoryFraction", "0.5")` on your SparkConf. Combined with the use of serialized caching,
using a smaller cache should be sufficient to mitigate most of the garbage collection problems.
In case you are interested in further tuning the Java GC, continue reading below.

**Advanced GC Tuning**

To further tune garbage collection, we first need to understand some basic information about memory management in the JVM:

* Java Heap space is divided in to two regions Young and Old. The Young generation is meant to hold short-lived objects
  while the Old generation is intended for objects with longer lifetimes.

* The Young generation is further divided into three regions \[Eden, Survivor1, Survivor2\].

* A simplified description of the garbage collection procedure: When Eden is full, a minor GC is run on Eden and objects
  that are alive from Eden and Survivor1 are copied to Survivor2. The Survivor regions are swapped. If an object is old
  enough or Survivor2 is full, it is moved to Old. Finally when Old is close to full, a full GC is invoked.

The goal of GC tuning in Spark is to ensure that only long-lived RDDs are stored in the Old generation and that
the Young generation is sufficiently sized to store short-lived objects. This will help avoid full GCs to collect
temporary objects created during task execution. Some steps which may be useful are:

* Check if there are too many garbage collections by collecting GC stats. If a full GC is invoked multiple times for
  before a task completes, it means that there isn't enough memory available for executing tasks.

* In the GC stats that are printed, if the OldGen is close to being full, reduce the amount of memory used for caching.
  This can be done using the `spark.storage.memoryFraction` property. It is better to cache fewer objects than to slow
  down task execution!

* If there are too many minor collections but not many major GCs, allocating more memory for Eden would help. You
  can set the size of the Eden to be an over-estimate of how much memory each task will need. If the size of Eden
  is determined to be `E`, then you can set the size of the Young generation using the option `-Xmn=4/3*E`. (The scaling
  up by 4/3 is to account for space used by survivor regions as well.)

* As an example, if your task is reading data from HDFS, the amount of memory used by the task can be estimated using
  the size of the data block read from HDFS. Note that the size of a decompressed block is often 2 or 3 times the
  size of the block. So if we wish to have 3 or 4 tasks worth of working space, and the HDFS block size is 64 MB,
  we can estimate size of Eden to be `4*3*64MB`.

* Monitor how the frequency and time taken by garbage collection changes with the new settings.

Our experience suggests that the effect of GC tuning depends on your application and the amount of memory available.
There are [many more tuning options](http://www.oracle.com/technetwork/java/javase/gc-tuning-6-140523.html) described online,
but at a high level, managing how frequently full GC takes place can help in reducing the overhead.

# Other Considerations

## Level of Parallelism

Clusters will not be fully utilized unless you set the level of parallelism for each operation high
enough. Spark automatically sets the number of "map" tasks to run on each file according to its size
(though you can control it through optional parameters to `SparkContext.textFile`, etc), and for
distributed "reduce" operations, such as `groupByKey` and `reduceByKey`, it uses the largest
parent RDD's number of partitions. You can pass the level of parallelism as a second argument
(see the [`spark.PairRDDFunctions`](api/core/index.html#org.apache.spark.rdd.PairRDDFunctions) documentation),
or set the config property `spark.default.parallelism` to change the default.
In general, we recommend 2-3 tasks per CPU core in your cluster.

## Memory Usage of Reduce Tasks

Sometimes, you will get an OutOfMemoryError not because your RDDs don't fit in memory, but because the
working set of one of your tasks, such as one of the reduce tasks in `groupByKey`, was too large.
Spark's shuffle operations (`sortByKey`, `groupByKey`, `reduceByKey`, `join`, etc) build a hash table
within each task to perform the grouping, which can often be large. The simplest fix here is to
*increase the level of parallelism*, so that each task's input set is smaller. Spark can efficiently
support tasks as short as 200 ms, because it reuses one worker JVMs across all tasks and it has
a low task launching cost, so you can safely increase the level of parallelism to more than the
number of cores in your clusters.

## Broadcasting Large Variables

Using the [broadcast functionality](scala-programming-guide.html#broadcast-variables)
available in `SparkContext` can greatly reduce the size of each serialized task, and the cost
of launching a job over a cluster. If your tasks use any large object from the driver program
inside of them (e.g. a static lookup table), consider turning it into a broadcast variable.
Spark prints the serialized size of each task on the master, so you can look at that to
decide whether your tasks are too large; in general tasks larger than about 20 KB are probably
worth optimizing.

# Summary

This has been a short guide to point out the main concerns you should know about when tuning a
Spark application -- most importantly, data serialization and memory tuning. For most programs,
switching to Kryo serialization and persisting data in serialized form will solve most common
performance issues. Feel free to ask on the
[Spark mailing list](http://groups.google.com/group/spark-users) about other tuning best practices.
