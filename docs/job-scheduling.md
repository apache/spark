---
layout: global
title: Job Scheduling
---

Spark has several facilities for scheduling resources between jobs. First, recall that, as described
in the [cluster mode overview](cluster-overview.html), each Spark application (instance of SparkContext)
runs an independent set of executor processes. The cluster managers that Spark runs on provide
facilities for [scheduling across applications](#scheduling-across-applications). Second,
_within_ each Spark application, multiple jobs may be running concurrently if they were submitted
from different threads. This is common if your application is serving requests over the network; for
example, the [Shark](http://shark.cs.berkeley.edu) server works this way. Spark includes a
[fair scheduler](#scheduling-within-an-application) to schedule between these jobs.

# Scheduling Across Applications

When running on a cluster, each Spark application gets an independent set of executor JVMs that only
run tasks and store data for that application. If multiple users need to share your cluster, there are
different options to manage allocation, depending on the cluster manager.

The simplest option, available on all cluster managers, is _static partitioning_ of resources. With
this approach, each application is given a maximum amount of resources it can use, and holds onto them
for its whole duration. This is the only approach available in Spark's [standalone](spark-standalone.html)
and [YARN](running-on-yarn.html) modes, as well as the
[coarse-grained Mesos mode](running-on-mesos.html#mesos-run-modes).
Resource allocation can be configured as follows, based on the cluster type:

* **Standalone mode:** By default, applications submitted to the standalone mode cluster will run in
  FIFO (first-in-first-out) order, and each application will try to use all available nodes. You can limit
  the number of nodes an application uses by setting the `spark.cores.max` system property in it. This
  will allow multiple users/applications to run concurrently. For example, you might launch a long-running
  server that uses 10 cores, and allow users to launch shells that use 20 cores each.
  Finally, in addition to controlling cores, each application's `spark.executor.memory` setting controls
  its memory use.
* **Mesos:** To use static partitioning on Mesos, set the `spark.mesos.coarse` system property to `true`,
  and optionally set `spark.cores.max` to limit each application's resource share as in the standalone mode.
  You should also set `spark.executor.memory` to control the executor memory.
* **YARN:** The `--num-workers` option to the Spark YARN client controls how many workers it will allocate
  on the cluster, while `--worker-memory` and `--worker-cores` control the resources per worker.

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

To enable the fair scheduler, simply set the `spark.scheduler.mode` to `FAIR` before creating
a SparkContext:

    System.setProperty("spark.scheduler.mode", "FAIR")

The fair scheduler also supports
