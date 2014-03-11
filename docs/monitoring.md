---
layout: global
title: Monitoring and Instrumentation
---

There are several ways to monitor Spark applications.

# Web Interfaces

Every SparkContext launches a web UI, by default on port 4040, that 
displays useful information about the application. This includes:

* A list of scheduler stages and tasks
* A summary of RDD sizes and memory usage
* Information about the running executors
* Environmental information.

You can access this interface by simply opening `http://<driver-node>:4040` in a web browser.
If multiple SparkContexts are running on the same host, they will bind to succesive ports
beginning with 4040 (4041, 4042, etc).

Spark's Standlone Mode cluster manager also has its own 
[web UI](spark-standalone.html#monitoring-and-logging). 

Note that in both of these UIs, the tables are sortable by clicking their headers,
making it easy to identify slow tasks, data skew, etc.

# Metrics

Spark has a configurable metrics system based on the 
[Coda Hale Metrics Library](http://metrics.codahale.com/). 
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV 
files. The metrics system is configured via a configuration file that Spark expects to be present 
at `$SPARK_HOME/conf/metrics.conf`. A custom file location can be specified via the 
`spark.metrics.conf` [configuration property](configuration.html#spark-properties).
Spark's metrics are decoupled into different 
_instances_ corresponding to Spark components. Within each instance, you can configure a 
set of sinks to which metrics are reported. The following instances are currently supported:

* `master`: The Spark standalone master process.
* `applications`: A component within the master which reports on various applications.
* `worker`: A Spark standalone worker process.
* `executor`: A Spark executor.
* `driver`: The Spark driver process (the process in which your SparkContext is created).

Each instance can report to zero or more _sinks_. Sinks are contained in the
`org.apache.spark.metrics.sink` package:

* `ConsoleSink`: Logs metrics information to the console.
* `CSVSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JXM console.
* `MetricsServlet`: Adds a servlet within the existing Spark UI to serve metrics data as JSON data.
* `GraphiteSink`: Sends metrics to a Graphite node.

Spark also supports a Ganglia sink which is not included in the default build due to
licensing restrictions:

* `GangliaSink`: Sends metrics to a Ganglia node or multicast group.

To install the `GangliaSink` you'll need to perform a custom build of Spark. _**Note that
by embedding this library you will include [LGPL](http://www.gnu.org/copyleft/lesser.html)-licensed 
code in your Spark package**_. For sbt users, set the 
`SPARK_GANGLIA_LGPL` environment variable before building. For Maven users, enable 
the `-Pspark-ganglia-lgpl` profile. In addition to modifying the cluster's Spark build
user applications will need to link to the `spark-ganglia-lgpl` artifact.

The syntax of the metrics configuration file is defined in an example configuration file, 
`$SPARK_HOME/conf/metrics.conf.template`.

# Advanced Instrumentation

Several external tools can be used to help profile the performance of Spark jobs:

* Cluster-wide monitoring tools, such as [Ganglia](http://ganglia.sourceforge.net/), can provide 
insight into overall cluster utilization and resource bottlenecks. For instance, a Ganglia 
dashboard can quickly reveal whether a particular workload is disk bound, network bound, or 
CPU bound.
* OS profiling tools such as [dstat](http://dag.wieers.com/home-made/dstat/), 
[iostat](http://linux.die.net/man/1/iostat), and [iotop](http://linux.die.net/man/1/iotop) 
can provide fine-grained profiling on individual nodes.
* JVM utilities such as `jstack` for providing stack traces, `jmap` for creating heap-dumps, 
`jstat` for reporting time-series statistics and `jconsole` for visually exploring various JVM 
properties are useful for those comfortable with JVM internals.
