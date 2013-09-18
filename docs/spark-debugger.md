---
layout: global
title: The Spark Debugger
---
**Summary:** The Spark debugger provides replay debugging for deterministic (logic) errors in Spark programs. It's currently in development, but you can try it out in the [arthur branch](https://github.com/apache/incubator-spark/tree/arthur).

## Introduction

From a user's point of view, debugging a general distributed program can be tedious and confusing. Many distributed programs are nondeterministic; their outcome depends on the interleaving between computation and message passing across multiple machines. Also, the fact that a program is running on a cluster of hundreds or thousands of machines means that it's hard to understand the program state and pinpoint the location of problems.

In order to tame nondeterminism, a distributed debugger has to log a lot of information, imposing a serious performance penalty on the application being debugged.

But the Spark programming model lets us provide replay debugging for almost zero overhead. Spark programs are a series of RDDs and deterministic transformations, so when debugging a Spark program, we don't have to debug it all at once -- instead, we can debug each transformation individually. Broadly, the debugger lets us do the following two things:

* Recompute and inspect intermediate RDDs after the program has finished.
* Re-run a particular task in a single-threaded debugger to find exactly what went wrong.

For deterministic errors, debugging a Spark program is now as easy as debugging a single-threaded one.

## Approach

As your Spark program runs, the slaves report key events back to the master -- for example, RDD creations, RDD contents, and uncaught exceptions. (A full list of event types is in [EventLogging.scala](https://github.com/apache/incubator-spark/blob/arthur/core/src/main/scala/spark/EventLogging.scala).) The master logs those events, and you can load the event log into the debugger after your program is done running.

_A note on nondeterminism:_ For fault recovery, Spark requires RDD transformations (for example, the function passed to `RDD.map`) to be deterministic. The Spark debugger also relies on this property, and it can also warn you if your transformation is nondeterministic. This works by checksumming the contents of each RDD and comparing the checksums from the original execution to the checksums after recomputing the RDD in the debugger.

## Usage

### Enabling the event log

To turn on event logging for your program, set `$SPARK_JAVA_OPTS` in `conf/spark-env.sh` as follows:

{% highlight bash %}
export SPARK_JAVA_OPTS='-Dspark.arthur.logPath=path/to/event-log'
{% endhighlight %}
   
where `path/to/event-log` is where you want the event log to go relative to `$SPARK_HOME`.

**Warning:** If `path/to/event-log` already exists, event logging will be automatically disabled.

### Loading the event log into the debugger

1. Run a Spark shell with `MASTER=<i>host</i> ./spark-shell`.
2. Use `EventLogReader` to load the event log as follows:
    {% highlight scala %}
spark> val r = new spark.EventLogReader(sc, Some("path/to/event-log"))
r: spark.EventLogReader = spark.EventLogReader@726b37ad
{% endhighlight %}

    **Warning:** If the event log doesn't exist or is unreadable, this will silently fail and `r.events` will be empty.

### Exploring intermediate RDDs

Use `r.rdds` to get a list of intermediate RDDs generated during your program's execution. An RDD with id _x_ is located at <code>r.rdds(<i>x</i>)</code>. For example:

{% highlight scala %}
scala> r.rdds
res8: scala.collection.mutable.ArrayBuffer[spark.RDD[_]] = ArrayBuffer(spark.HadoopRDD@fe85adf, spark.MappedRDD@5fa5eea1, spark.MappedRDD@6d5bd16, spark.ShuffledRDD@3a70f2db, spark.FlatMappedValuesRDD@4d5825d6, spark.MappedValuesRDD@561c2c45, spark.CoGroupedRDD@539e922d, spark.MappedValuesRDD@4f8ef33e, spark.FlatMappedRDD@32039440, spark.ShuffledRDD@8fa0f67, spark.MappedValuesRDD@590937cb, spark.CoGroupedRDD@6c2e1e17, spark.MappedValuesRDD@47b9af7d, spark.FlatMappedRDD@6fb05c54, spark.ShuffledRDD@237dc815, spark.MappedValuesRDD@16daece7, spark.CoGroupedRDD@7ef73d69, spark.MappedValuesRDD@19e0f99e, spark.FlatMappedRDD@1240158, spark.ShuffledRDD@62d438fd, spark.MappedValuesRDD@5ae99cbb, spark.FilteredRDD@1f30e79e, spark.MappedRDD@43b64611)
{% endhighlight %}

Use `r.printRDDs()` to get a formatted list of intermediate RDDs, along with the source location where they were created. For example:

{% highlight scala %}
scala> r.printRDDs
#00: HadoopRDD            spark.bagel.examples.WikipediaPageRankStandalone$.main(WikipediaPageRankStandalone.scala:31)
#01: MappedRDD            spark.bagel.examples.WikipediaPageRankStandalone$.main(WikipediaPageRankStandalone.scala:31)
#02: MappedRDD            spark.bagel.examples.WikipediaPageRankStandalone$.main(WikipediaPageRankStandalone.scala:35)
#03: ShuffledRDD          spark.bagel.examples.WikipediaPageRankStandalone$.main(WikipediaPageRankStandalone.scala:35)
#04: FlatMappedValuesRDD  spark.bagel.examples.WikipediaPageRankStandalone$.main(WikipediaPageRankStandalone.scala:35)
#05: MappedValuesRDD      spark.bagel.examples.WikipediaPageRankStandalone$.pageRank(WikipediaPageRankStandalone.scala:91)
#06: CoGroupedRDD         spark.bagel.examples.WikipediaPageRankStandalone$.pageRank(WikipediaPageRankStandalone.scala:92)
[...]
{% endhighlight %}

Use `r.visualizeRDDs()` to visualize the RDDs as a dependency graph. For example:

{% highlight scala %}
scala> r.visualizeRDDs
/tmp/spark-rdds-3758182885839775712.pdf
{% endhighlight %}

![Example RDD dependency graph](http://www.ankurdave.com/images/rdd-dep-graph.png)

Iterate over the `RDDCreation` entries in `r.events` (e.g. `for (RDDCreation(rdd, location) <- events)`) to access the RDD creation locations as well as the RDDs themselves.

### Debugging a particular task

1. Find the task you want to debug. If the task threw an exception, the `ExceptionEvent` that was created will have a reference to the task. For example:
    {% highlight scala %}
spark> val task = r.events.collect { case e: ExceptionEvent => e }.head.task
{% endhighlight %}
    Otherwise, look through the list of all tasks in `r.tasks`, or browse tasks by RDD using <code>r.tasksForRDD(<i>rdd</i>)</code>, which returns a list of tasks whose input is the given RDD.

2. Run the task by calling <code>r.debugTask(<i>taskStageId</i>, <i>taskPartition</i>)</code>. The task should contain these two values; you can extract them as follows:
    {% highlight scala %}
val (taskStageId, taskPartition) = task match {
    case rt: ResultTask[_, _] => (rt.stageId, rt.partition)
    case smt: ShuffleMapTask => (smt.stageId, smt.partition)
    case _ => throw new UnsupportedOperationException
})
{% endhighlight %}
    The Spark debugger will launch the task in a separate JVM, but you will see the task's stdout and stderr inline with the Spark shell. If you want to pass custom debugging arguments to the task's JVM (for example, to change the debugging port), set the optional `debugOpts` argument to `r.debugTask`. When `debugOpts` is left unset, it defaults to:
    {% highlight scala %}
-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000
{% endhighlight %}

3. In another terminal, attach your favorite conventional debugger to the Spark shell. For example, if you want to use jdb, run `jdb -attach 8000`.

4. Debug the task as you would debug a normal program. For example, to break when an exception is thrown:
    {% highlight scala %}
> catch org.xml.sax.SAXParseException
{% endhighlight %}

5. When the task ends, its JVM will quit and control will return to the main Spark shell. To stop it prematurely, you can kill it from the debugger, or interrupt it from the terminal with Ctrl-C.

### Detecting nondeterminism in your transformations

When a task gets run more than once, Arthur is able to compare the checksums of the task's output. If they are different, Arthur will insert a `ChecksumEvent` into  `r.checksumMismatches` and print a warning like the following:
    {% highlight scala %}
12/04/07 11:42:44 WARN spark.EventLogWriter: Nondeterminism detected in shuffle output on RDD 2, partition 3, output split 0
{% endhighlight %}

