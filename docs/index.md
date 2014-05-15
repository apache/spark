---
layout: global
title: Spark Overview
---

Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in [Scala](scala-programming-guide.html), [Java](java-programming-guide.html), and [Python](python-programming-guide.html) that make parallel jobs easy to write, and an optimized engine that supports general computation graphs.
It also supports a rich set of higher-level tools including [Shark](http://shark.cs.berkeley.edu) (Hive on Spark), [MLlib](mllib-guide.html) for machine learning, [GraphX](graphx-programming-guide.html) for graph processing, and [Spark Streaming](streaming-programming-guide.html).

# Downloading

Get Spark by visiting the [downloads page](http://spark.apache.org/downloads.html) of the Apache Spark site. This documentation is for Spark version {{site.SPARK_VERSION}}. The downloads page 
contains Spark packages for many popular HDFS versions. If you'd like to build Spark from 
scratch, visit the [building with Maven](building-with-maven.html) page.

Spark runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS). All you need to run it is 
to have `java` to installed on your system `PATH`, or the `JAVA_HOME` environment variable 
pointing to a Java installation.

For its Scala API, Spark {{site.SPARK_VERSION}} depends on Scala {{site.SCALA_BINARY_VERSION}}. 
If you write applications in Scala, you will need to use a compatible Scala version 
(e.g. {{site.SCALA_BINARY_VERSION}}.X) -- newer major versions may not work. You can get the 
right version of Scala from [scala-lang.org](http://www.scala-lang.org/download/).

# Running the Examples and Shell

Spark comes with several sample programs.  Scala, Java and Python examples are in the
`examples/src/main` directory. To run one of the Java or Scala sample programs, use
`bin/run-example <class> [params]` in the top-level Spark directory. (Behind the scenes, this
invokes the more general
[Spark submit script](cluster-overview.html#launching-applications-with-spark-submit) for
launching applications). For example,

    ./bin/run-example SparkPi 10

You can also run Spark interactively through modified versions of the Scala shell. This is a
great way to learn the framework.

    ./bin/spark-shell --master local[2]

The `--master` option specifies the
[master URL for a distributed cluster](scala-programming-guide.html#master-urls), or `local` to run
locally with one thread, or `local[N]` to run locally with N threads. You should start by using
`local` for testing. For a full list of options, run Spark shell with the `--help` option.

Spark also provides a Python interface. To run an example Spark application written in Python, use
`bin/pyspark <program> [params]`. For example,

    ./bin/pyspark examples/src/main/python/pi.py local[2] 10

or simply `bin/pyspark` without any arguments to run Spark interactively in a python interpreter.

# Launching on a Cluster

The Spark [cluster mode overview](cluster-overview.html) explains the key concepts in running on a cluster.
Spark can run both by itself, or over several existing cluster managers. It currently provides several
options for deployment:

* [Amazon EC2](ec2-scripts.html): our EC2 scripts let you launch a cluster in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): simplest way to deploy Spark on a private cluster
* [Apache Mesos](running-on-mesos.html)
* [Hadoop YARN](running-on-yarn.html)

# Where to Go from Here

**Programming guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [Spark Programming Guide](scala-programming-guide.html): an overview of Spark concepts, and details on the Scala API
  * [Java Programming Guide](java-programming-guide.html): using Spark from Java
  * [Python Programming Guide](python-programming-guide.html): using Spark from Python
* [Spark Streaming](streaming-programming-guide.html): Spark's API for processing data streams
* [Spark SQL](sql-programming-guide.html): Support for running relational queries on Spark
* [MLlib (Machine Learning)](mllib-guide.html): Spark's built-in machine learning library
* [Bagel (Pregel on Spark)](bagel-programming-guide.html): simple graph processing model
* [GraphX (Graphs on Spark)](graphx-programming-guide.html): Spark's new API for graphs

**API Docs:**

* [Spark Scala API (Scaladoc)](api/scala/index.html#org.apache.spark.package)
* [Spark Java API (Javadoc)](api/java/index.html)
* [Spark Python API (Epydoc)](api/python/index.html)

**Deployment guides:**

* [Cluster Overview](cluster-overview.html): overview of concepts and components when running on a cluster
* [Amazon EC2](ec2-scripts.html): scripts that let you launch a cluster on EC2 in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): launch a standalone cluster quickly without a third-party cluster manager
* [Mesos](running-on-mesos.html): deploy a private cluster using
    [Apache Mesos](http://mesos.apache.org)
* [YARN](running-on-yarn.html): deploy Spark on top of Hadoop NextGen (YARN)

**Other documents:**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [Security](security.html): Spark security support
* [Hardware Provisioning](hardware-provisioning.html): recommendations for cluster hardware
* [Job Scheduling](job-scheduling.html): scheduling resources across and within Spark applications
* [Building Spark with Maven](building-with-maven.html): build Spark using the Maven system
* [Contributing to Spark](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark)

**External resources:**

* [Spark Homepage](http://spark.apache.org)
* [Shark](http://shark.cs.berkeley.edu): Apache Hive over Spark
* [Mailing Lists](http://spark.apache.org/mailing-lists.html): ask questions about Spark here
* [AMP Camps](http://ampcamp.berkeley.edu/): a series of training camps at UC Berkeley that featured talks and
  exercises about Spark, Shark, Spark Streaming, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/3/),
  [slides](http://ampcamp.berkeley.edu/3/) and [exercises](http://ampcamp.berkeley.edu/3/exercises/) are
  available online for free.
* [Code Examples](http://spark.apache.org/examples.html): more are also available in the [examples subfolder](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/) of Spark
* [Paper Describing Spark](http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)
* [Paper Describing Spark Streaming](http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-259.pdf)

# Community

To get help using Spark or keep up with Spark development, sign up for the [user mailing list](http://spark.apache.org/mailing-lists.html).

If you're in the San Francisco Bay Area, there's a regular [Spark meetup](http://www.meetup.com/spark-users/) every few weeks. Come by to meet the developers and other users.

Finally, if you'd like to contribute code to Spark, read [how to contribute](contributing-to-spark.html).
