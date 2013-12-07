---
layout: global
title: Cluster Mode Overview
---

This document gives a short overview of how Spark runs on clusters, to make it easier to understand
the components involved.

# Components

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext
object in your main program (called the _driver program_).
Specifically, to run on a cluster, the SparkContext can connect to several types of _cluster managers_
(either Spark's own standalone cluster manager or Mesos/YARN), which allocate resources across
applications. Once connected, Spark acquires *executors* on nodes in the cluster, which are
worker processes that run computations and store data for your application.
Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to
the executors. Finally, SparkContext sends *tasks* for the executors to run.

<p style="text-align: center;">
  <img src="img/cluster-overview.png" title="Spark cluster components" alt="Spark cluster components" />
</p>

There are several useful things to note about this architecture:

1. Each application gets its own executor processes, which stay up for the duration of the whole
   application and run tasks in multiple threads. This has the benefit of isolating applications
   from each other, on both the scheduling side (each driver schedules its own tasks) and executor
   side (tasks from different applications run in different JVMs). However, it also means that
   data cannot be shared across different Spark applications (instances of SparkContext) without
   writing it to an external storage system.
2. Spark is agnostic to the underlying cluster manager. As long as it can acquire executor
   processes, and these communicate with each other, it is relatively easy to run it even on a
   cluster manager that also supports other applications (e.g. Mesos/YARN).
3. Because the driver schedules tasks on the cluster, it should be run close to the worker
   nodes, preferably on the same local area network. If you'd like to send requests to the
   cluster remotely, it's better to open an RPC to the driver and have it submit operations
   from nearby than to run a driver far away from the worker nodes.

# Cluster Manager Types

The system currently supports three cluster managers:

* [Standalone](spark-standalone.html) -- a simple cluster manager included with Spark that makes it
  easy to set up a cluster.
* [Apache Mesos](running-on-mesos.html) -- a general cluster manager that can also run Hadoop MapReduce
  and service applications.
* [Hadoop YARN](running-on-yarn.html) -- the resource manager in Hadoop 2.

In addition, Spark's [EC2 launch scripts](ec2-scripts.html) make it easy to launch a standalone
cluster on Amazon EC2.

# Shipping Code to the Cluster

The recommended way to ship your code to the cluster is to pass it through SparkContext's constructor,
which takes a list of JAR files (Java/Scala) or .egg and .zip libraries (Python) to disseminate to
worker nodes. You can also dynamically add new files to be sent to executors with `SparkContext.addJar`
and `addFile`.

## URIs for addJar / addFile

- **file:** - Absolute paths and `file:/` URIs are served by the driver's HTTP file server, and every executor
  pulls the file from the driver HTTP server
- **hdfs:**, **http:**, **https:**, **ftp:** - these pull down files and JARs from the URI as expected
- **local:** - a URI starting with local:/ is expected to exist as a local file on each worker node.  This
  means that no network IO will be incurred, and works well for large files/JARs that are pushed to each worker,
  or shared via NFS, GlusterFS, etc.

Note that JARs and files are copied to the working directory for each SparkContext on the executor nodes.
Over time this can use up a significant amount of space and will need to be cleaned up.

# Monitoring

Each driver program has a web UI, typically on port 4040, that displays information about running
tasks, executors, and storage usage. Simply go to `http://<driver-node>:4040` in a web browser to
access this UI. The [monitoring guide](monitoring.html) also describes other monitoring options.

# Job Scheduling

Spark gives control over resource allocation both _across_ applications (at the level of the cluster
manager) and _within_ applications (if multiple computations are happening on the same SparkContext).
The [job scheduling overview](job-scheduling.html) describes this in more detail.

# Glossary

The following table summarizes terms you'll see used to refer to cluster concepts:

<table class="table">
  <thead>
    <tr><th style="width: 130px;">Term</th><th>Meaning</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Application</td>
      <td>User program built on Spark. Consists of a <em>driver program</em> and <em>executors</em> on the cluster.</td>
    </tr>
    <tr>
      <td>Driver program</td>
      <td>The process running the main() function of the application and creating the SparkContext</td>
    </tr>
    <tr>
      <td>Cluster manager</td>
      <td>An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)</td>
    </tr>
    <tr>
      <td>Worker node</td>
      <td>Any node that can run application code in the cluster</td>
    </tr>
    <tr>
      <td>Executor</td>
      <td>A process launched for an application on a worker node, that runs tasks and keeps data in memory
        or disk storage across them. Each application has its own executors.</td>
    </tr>
    <tr>
      <td>Task</td>
      <td>A unit of work that will be sent to one executor</td>
    </tr>
    <tr>
      <td>Job</td>
      <td>A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action
        (e.g. <code>save</code>, <code>collect</code>); you'll see this term used in the driver's logs.</td>
    </tr>
    <tr>
      <td>Stage</td>
      <td>Each job gets divided into smaller sets of tasks called <em>stages</em> that depend on each other
        (similar to the map and reduce stages in MapReduce); you'll see this term used in the driver's logs.</td>
    </tr>
  </tbody>
</table>
