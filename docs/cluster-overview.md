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
processes that run computations and store data for your application.
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

# Bundling and Launching Applications

### Bundling Your Application's Dependencies
If your code depends on other projects, you will need to package them alongside
your application in order to distribute the code to a Spark cluster. To do this,
to create an assembly jar (or "uber" jar) containing your code and its dependencies. Both
[sbt](https://github.com/sbt/sbt-assembly) and
[Maven](http://maven.apache.org/plugins/maven-shade-plugin/)
have assembly plugins. When creating assembly jars, list Spark and Hadoop
as `provided` dependencies; these need not be bundled since they are provided by
the cluster manager at runtime. Once you have an assembled jar you can call the `bin/spark-submit`
script as shown here while passing your jar.

For Python, you can use the `pyFiles` argument of SparkContext
or its `addPyFile` method to add `.py`, `.zip` or `.egg` files to be distributed.

### Launching Applications with Spark submit

Once a user application is bundled, it can be launched using the `spark-submit` script located in
the bin directory. This script takes care of setting up the classpath with Spark and its
dependencies, and can support different cluster managers and deploy modes that Spark supports:

    ./bin/spark-submit \
      --class <main-class>
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      ... // other options
      <application-jar>
      [application-arguments]

    main-class: The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
    master-url: The URL of the master node (e.g. spark://23.195.26.187:7077)
    deploy-mode: Whether to deploy this application within the cluster or from an external client (e.g. client)
    application-jar: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an `hdfs://` path or a `file://` path that is present on all nodes.
    application-arguments: Space delimited arguments passed to the main method of <main-class>, if any

To enumerate all options available to `spark-submit` run it with the `--help` flag. Here are a few
examples of common options:

{% highlight bash %}
# Run application locally
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi
  --master local[8] \
  /path/to/examples.jar \
  100

# Run on a Spark standalone cluster
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000

# Run on a YARN cluster
HADOOP_CONF_DIR=XX ./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi
  --master yarn-cluster \  # can also be `yarn-client` for client mode
  --executor-memory 20G \
  --num-executors 50 \
  /path/to/examples.jar \
  1000
{% endhighlight %}

### Loading Configurations from a File

The `spark-submit` script can load default [Spark configuration values](configuration.html) from a
properties file and pass them on to your application. By default it will read configuration options
from `conf/spark-defaults.conf`. For more detail, see the section on
[loading default configurations](configuration.html#loading-default-configurations).

Loading default Spark configurations this way can obviate the need for certain flags to
`spark-submit`. For instance, if the `spark.master` property is set, you can safely omit the
`--master` flag from `spark-submit`. In general, configuration values explicitly set on a
`SparkConf` take the highest precedence, then flags passed to `spark-submit`, then values in the
defaults file.

If you are ever unclear where configuration options are coming from, you can print out fine-grained
debugging information by running `spark-submit` with the `--verbose` option.

### Advanced Dependency Management
When using `spark-submit`, the application jar along with any jars included with the `--jars` option
will be automatically transferred to the cluster. Spark uses the following URL scheme to allow
different strategies for disseminating jars:

- **file:** - Absolute paths and `file:/` URIs are served by the driver's HTTP file server, and
  every executor pulls the file from the driver HTTP server.
- **hdfs:**, **http:**, **https:**, **ftp:** - these pull down files and JARs from the URI as expected
- **local:** - a URI starting with local:/ is expected to exist as a local file on each worker node.  This
  means that no network IO will be incurred, and works well for large files/JARs that are pushed to each worker,
  or shared via NFS, GlusterFS, etc.

Note that JARs and files are copied to the working directory for each SparkContext on the executor nodes.
This can use up a significant amount of space over time and will need to be cleaned up. With YARN, cleanup
is handled automatically, and with Spark standalone, automatic cleanup can be configured with the
`spark.worker.cleanup.appDataTtl` property.

For python, the equivalent `--py-files` option can be used to distribute .egg and .zip libraries
to executors.

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
      <td>Application jar</td>
      <td>
        A jar containing the user's Spark application. In some cases users will want to create
        an "uber jar" containing their application along with its dependencies. The user's jar
        should never include Hadoop or Spark libraries, however, these will be added at runtime.
      </td>
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
      <td>Deploy mode</td>
      <td>Distinguishes where the driver process runs. In "cluster" mode, the framework launches
        the driver inside of the cluster. In "client" mode, the submitter launches the driver
        outside of the cluster.</td>
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
