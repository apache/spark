---
layout: global
title: Tuning Spark
---

Because of the in-memory nature of most Spark computations, Spark programs can be bottlenecked
by any resource in the cluster: CPU, network bandwidth, or memory.
Most often, if the data fits in memory, the bottleneck is network bandwidth, but sometimes, you
also need to do some tuning, such as
[storing RDDs in serialized form]({{HOME_PATH}}scala-programming-guide#rdd-persistence), to
make the memory usage smaller.
This guide will cover two main topics: data serialization, which is crucial for good network
performance, and memory tuning. We also sketch several smaller topics.

# Data Serialization

One of the most important concerns in any distributed program is the format of data sent across
the network -- formats that are slow to serialize objects into, or consume a large number of
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
* [Kryo serialization](http://code.google.com/p/kryo/wiki/V1Documentation): Spark can also use
  the Kryo library (currently just version 1) to serialize objects more quickly. Kryo is significantly
  faster and more compact than Java serialization (often as much as 10x), but does not support all
  `Serializable` types and requires you to *register* the classes you'll use in the program in advance
  for best performance.

You can switch to using Kryo by calling `System.setProperty("spark.serializer", "spark.KryoSerializer")`
*before* creating your SparkContext. The only reason it is not the default is because of the custom
registration requirement, but we recommend trying it in any network-intensive application.

Finally, to register your classes with Kryo, create a public class that extends
[`spark.KryoRegistrator`]({{HOME_PATH}}api/core/index.html#spark.KryoRegistrator) and set the
`spark.kryo.registrator` system property to point to it, as follows:

{% highlight scala %}
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[MyClass1])
    kryo.register(classOf[MyClass2])
  }
}

// Make sure to set these properties *before* creating a SparkContext!
System.setProperty("spark.serializer", "spark.KryoSerializer")
System.setProperty("spark.kryo.registrator", "mypackage.MyRegistrator")
val sc = new SparkContext(...)
{% endhighlight %}

The [Kryo documentation](http://code.google.com/p/kryo/wiki/V1Documentation) describes more advanced
registration options, such as adding custom serialization code.

If your objects are large, you may also need to increase the `spark.kryoserializer.buffer.mb`
system property. The default is 32, but this value needs to be large enough to hold the *largest*
object you will serialize.

Finally, if you don't register your classes, Kryo will still work, but it will have to store the
full class name with each object, which is wasteful.


# Memory Tuning

There are three considerations in tuning memory usage: the *amount* of memory used by your objects
(you likely want your entire dataset to fit in memory), the *cost* of accessing those objects, and the
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

There are several ways to reduce this cost and still make Java objects efficient to work with:

1. Design your data structures to prefer arrays of objects, and primitive types, instead of the
   standard Java or Scala collection classes (e.g. `HashMap`). The [fastutil](http://fastutil.di.unimi.it)
   library provides convenient collection classes for primitive types that are compatible with the
   Java standard library.
2. Avoid nested structures with a lot of small objects and pointers when possible.
3. If you have less than 32 GB of RAM, set the JVM flag `-XX:+UseCompressedOops` to make pointers be
   four bytes instead of eight. Also, on Java 7 or later, try `-XX:+UseCompressedStrings` to store
   ASCII strings as just 8 bits per character. You can add these options in
   [`spark-env.sh`]({{HOME_PATH}}configuration.html#environment-variables-in-spark-envsh).

You can get a sense of the memory usage of each object by looking at the logs of your Spark worker
nodes -- they will print the size of each RDD partition cached.

When your objects are still too large to efficiently store despite this tuning, a much simpler way
to reduce memory usage is to store them in *serialized* form, using the serialized StorageLevels in
the [RDD persistence API]({{HOME_PATH}}scala-programming-guide#rdd-persistence).
Spark will then store each RDD partition as one large byte array.
The only downside of storing data in serialized form is slower access times, due to having to
deserialize each object on the fly.
We highly recommend [using Kryo](#data-serialization) if you want to cache data in serialized form, as
it leads to much smaller sizes than Java serialization (and certainly than raw Java objects).

Finally, JVM garbage collection can be a problem when you have large "churn" in terms of the RDDs
stored by your program. (It is generally not a problem in programs that just read an RDD once
and then run many operations on it.) When Java needs to evict old objects to make room for new ones, it will
need to trace through all your Java objects and find the unused ones. The main point to remember here is
that *the cost of garbage collection is proportional to the number of Java objects*, so using data
structures with fewer objects (e.g. an array of `Int`s instead of a `LinkedList`) greatly reduces
this cost. An even better method is to persist objects in serialized form, as described above: now
there will be only *one* object (a byte array) per RDD partition. There is a lot of
[detailed information on GC tuning](http://www.oracle.com/technetwork/java/javase/gc-tuning-6-140523.html)
available online, but at a high level, the first thing to try if GC is a problem is to use serialized caching.


# Other Considerations

## Level of Parallelism

Clusters will not be fully utilized unless you set the level of parallelism for each operation high
enough. Spark automatically sets the number of "map" tasks to run on each file according to its size
(though you can control it through optional parameters to `SparkContext.textFile`, etc), but for
distributed "reduce" operations, such as `groupByKey` and `reduceByKey`, it uses a default value of 8.
You can pass the level of parallelism as a second argument (see the
[`spark.PairRDDFunctions`]({{HOME_PATH}}api/core/index.html#spark.PairRDDFunctions) documentation),
or set the system property `spark.default.parallelism` to change the default.
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

Using the [broadcast functionality]({{HOME_PATH}}scala-programming-guide#broadcast-variables)
available in `SparkContext` can greatly reduce the size of each serialized task, and the cost
of launching a job over a cluster. If your tasks use any large object from the driver program
inside of them (e.g. a static lookup table), consider turning it into a broadcast variable.
Spark prints the serialized size of each task on the master, so you can look at that to
decide whether your tasks are too large; in general tasks larger than about 20 KB are probably
worth optimizing.


# Summary

This has been a quick guide to point out the main concerns you should know about when tuning a
Spark application -- most importantly, data serialization and memory tuning. For most programs,
switching to Kryo serialization and persisting data in serialized form will solve most common
performance issues. Feel free to ask on the
[Spark mailing list](http://groups.google.com/group/spark-users) about other tuning best practices.