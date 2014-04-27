---
layout: global
title: Job Scheduling
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Spark has several facilities for scheduling resources between computations. First, recall that, as described
in the [cluster mode overview](cluster-overview.html), each Spark application (instance of SparkContext)
runs an independent set of executor processes. The cluster managers that Spark runs on provide
facilities for [scheduling across applications](#scheduling-across-applications). Second,
_within_ each Spark application, multiple "jobs" (Spark actions) may be running concurrently
if they were submitted by different threads. This is common if your application is serving requests
over the network; for example, the [Shark](http://shark.cs.berkeley.edu) server works this way. Spark
includes a [fair scheduler](#scheduling-within-an-application) to schedule resources within each SparkContext.

# Scheduling Across Applications

When running on a cluster, each Spark application gets an independent set of executor JVMs that only
run tasks and store data for that application. If multiple users need to share your cluster, there are
different options to manage allocation, depending on the cluster manager.

The simplest option, available on all cluster managers, is _static partitioning_ of resources. With
this approach, each application is given a maximum amount of resources it can use, and holds onto them
for its whole duration. This is the approach used in Spark's [standalone](spark-standalone.html)
and [YARN](running-on-yarn.html) modes, as well as the
[coarse-grained Mesos mode](running-on-mesos.html#mesos-run-modes).
Resource allocation can be configured as follows, based on the cluster type:

* **Standalone mode:** By default, applications submitted to the standalone mode cluster will run in
  FIFO (first-in-first-out) order, and each application will try to use all available nodes. You can limit
  the number of nodes an application uses by setting the `spark.cores.max` configuration property in it,
  or change the default for applications that don't set this setting through `spark.deploy.defaultCores`. 
  Finally, in addition to controlling cores, each application's `spark.executor.memory` setting controls
  its memory use.
* **Mesos:** To use static partitioning on Mesos, set the `spark.mesos.coarse` configuration property to `true`,
  and optionally set `spark.cores.max` to limit each application's resource share as in the standalone mode.
  You should also set `spark.executor.memory` to control the executor memory.
* **YARN:** The `--num-executors` option to the Spark YARN client controls how many executors it will allocate
  on the cluster, while `--executor-memory` and `--executor-cores` control the resources per executor.

A second option available on Mesos is _dynamic sharing_ of CPU cores. In this mode, each Spark application
still has a fixed and independent memory allocation (set by `spark.executor.memory`), but when the
application is not running tasks on a machine, other applications may run tasks on those cores. This mode
is useful when you expect large numbers of not overly active applications, such as shell sessions from
separate users. However, it comes with a risk of less predictable latency, because it may take a while for
an application to gain back cores on one node when it has work to do. To use this mode, simply use a
`mesos://` URL without setting `spark.mesos.coarse` to true.

Note that none of the modes currently provide memory sharing across applications. If you would like to share
data this way, we recommend running a single server application that can serve multiple requests by querying
the same RDDs. For example, the [Shark](http://shark.cs.berkeley.edu) JDBC server works this way for SQL
queries. In future releases, in-memory storage systems such as [Tachyon](http://tachyon-project.org) will
provide another approach to share RDDs.


# Scheduling Within an Application

Inside a given Spark application (SparkContext instance), multiple parallel jobs can run simultaneously if
they were submitted from separate threads. By "job", in this section, we mean a Spark action (e.g. `save`,
`collect`) and any tasks that need to run to evaluate that action. Spark's scheduler is fully thread-safe
and supports this use case to enable applications that serve multiple requests (e.g. queries for
multiple users).

By default, Spark's scheduler runs jobs in FIFO fashion. Each job is divided into "stages" (e.g. map and
reduce phases), and the first job gets priority on all available resources while its stages have tasks to
launch, then the second job gets priority, etc. If the jobs at the head of the queue don't need to use
the whole cluster, later jobs can start to run right away, but if the jobs at the head of the queue are
large, then later jobs may be delayed significantly.

Starting in Spark 0.8, it is also possible to configure fair sharing between jobs. Under fair sharing,
Spark assigns tasks between jobs in a "round robin" fashion, so that all jobs get a roughly equal share
of cluster resources. This means that short jobs submitted while a long job is running can start receiving
resources right away and still get good response times, without waiting for the long job to finish. This
mode is best for multi-user settings.

To enable the fair scheduler, simply set the `spark.scheduler.mode` property to `FAIR` when configuring
a SparkContext:

{% highlight scala %}
val conf = new SparkConf().setMaster(...).setAppName(...)
conf.set("spark.scheduler.mode", "FAIR")
val sc = new SparkContext(conf)
{% endhighlight %}

## Fair Scheduler Pools

The fair scheduler also supports grouping jobs into _pools_, and setting different scheduling options
(e.g. weight) for each pool. This can be useful to create a "high-priority" pool for more important jobs,
for example, or to group the jobs of each user together and give _users_ equal shares regardless of how
many concurrent jobs they have instead of giving _jobs_ equal shares. This approach is modeled after the
[Hadoop Fair Scheduler](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/FairScheduler.html).

Without any intervention, newly submitted jobs go into a _default pool_, but jobs' pools can be set by
adding the `spark.scheduler.pool` "local property" to the SparkContext in the thread that's submitting them.
This is done as follows:

{% highlight scala %}
// Assuming sc is your SparkContext variable
sc.setLocalProperty("spark.scheduler.pool", "pool1")
{% endhighlight %}

After setting this local property, _all_ jobs submitted within this thread (by calls in this thread
to `RDD.save`, `count`, `collect`, etc) will use this pool name. The setting is per-thread to make
it easy to have a thread run multiple jobs on behalf of the same user. If you'd like to clear the
pool that a thread is associated with, simply call:

{% highlight scala %}
sc.setLocalProperty("spark.scheduler.pool", null)
{% endhighlight %}

## Default Behavior of Pools

By default, each pool gets an equal share of the cluster (also equal in share to each job in the default
pool), but inside each pool, jobs run in FIFO order. For example, if you create one pool per user, this
means that each user will get an equal share of the cluster, and that each user's queries will run in
order instead of later queries taking resources from that user's earlier ones.

## Configuring Pool Properties

Specific pools' properties can also be modified through a configuration file. Each pool supports three
properties:

* `schedulingMode`: This can be FIFO or FAIR, to control whether jobs within the pool queue up behind
  each other (the default) or share the pool's resources fairly.
* `weight`: This controls the pool's share of the cluster relative to other pools. By default, all pools
  have a weight of 1. If you give a specific pool a weight of 2, for example, it will get 2x more
  resources as other active pools. Setting a high weight such as 1000 also makes it possible to implement
  _priority_ between pools---in essence, the weight-1000 pool will always get to launch tasks first
  whenever it has jobs active.
* `minShare`: Apart from an overall weight, each pool can be given a _minimum shares_ (as a number of
  CPU cores) that the administrator would like it to have. The fair scheduler always attempts to meet
  all active pools' minimum shares before redistributing extra resources according to the weights.
  The `minShare` property can therefore be another way to ensure that a pool can always get up to a
  certain number of resources (e.g. 10 cores) quickly without giving it a high priority for the rest
  of the cluster. By default, each pool's `minShare` is 0.

The pool properties can be set by creating an XML file, similar to `conf/fairscheduler.xml.template`,
and setting a `spark.scheduler.allocation.file` property in your
[SparkConf](configuration.html#spark-properties).

{% highlight scala %}
conf.set("spark.scheduler.allocation.file", "/path/to/file")
{% endhighlight %}

The format of the XML file is simply a `<pool>` element for each pool, with different elements
within it for the various settings. For example:

{% highlight xml %}
<?xml version="1.0"?>
<allocations>
  <pool name="production">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
  <pool name="test">
    <schedulingMode>FIFO</schedulingMode>
    <weight>2</weight>
    <minShare>3</minShare>
  </pool>
</allocations>
{% endhighlight %}

A full example is also available in `conf/fairscheduler.xml.template`. Note that any pools not
configured in the XML file will simply get default values for all settings (scheduling mode FIFO,
weight 1, and minShare 0).
