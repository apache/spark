---
layout: global
title: Monitoring and Instrumentation
---

There are several ways to monitor the progress of Spark jobs.

# Web Interfaces
When a SparkContext is initialized, it launches a web server (by default at port 3030) which 
displays useful information. This includes a list of active and completed scheduler stages, 
a summary of RDD blocks and partitions, and environmental information. If multiple SparkContexts
are running on the same host, they will bind to succesive ports beginning with 3030 (3031, 3032, 
etc).

Spark's Standlone Mode scheduler also has its own 
[web interface](spark-standalone.html#monitoring-and-logging). 

# Spark Metrics
Spark has a configurable metrics system based on the 
[Coda Hale Metrics Library](http://metrics.codahale.com/). 
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV 
files. The metrics system is configured via a configuration file that Spark expects to be present 
at `$SPARK_HOME/conf/metrics.conf`. A custom file location can be specified via the 
`spark.metrics.conf` Java system property. Spark's metrics are decoupled into different 
_instances_ corresponding to Spark components. Within each instance, you can configure a 
set of sinks to which metrics are reported. The following instances are currently supported:

* `master`: The Spark standalone master process.
* `applications`: A component within the master which reports on various applications.
* `worker`: A Spark standalone worker process.
* `executor`: A Spark executor.
* `driver`: The Spark driver process (the process in which your SparkContext is created).

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
