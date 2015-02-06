---
layout: global
displayTitle: Spark Overview
title: Overview
description: Apache Spark SPARK_VERSION_SHORT documentation homepage
---

Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in Java, Scala and Python,
and an optimized engine that supports general execution graphs.
It also supports a rich set of higher-level tools including [Spark SQL](sql-programming-guide.html) for SQL and structured data processing, [MLlib](mllib-guide.html) for machine learning, [GraphX](graphx-programming-guide.html) for graph processing, and [Spark Streaming](streaming-programming-guide.html).

# Downloading

Get Spark from the [downloads page](http://spark.apache.org/downloads.html) of the project website. This documentation is for Spark version {{site.SPARK_VERSION}}. The downloads page 
contains Spark packages for many popular HDFS versions. If you'd like to build Spark from 
scratch, visit [Building Spark](building-spark.html).

Spark runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS). It's easy to run
locally on one machine --- all you need is to have `java` installed on your system `PATH`,
or the `JAVA_HOME` environment variable pointing to a Java installation.

Spark runs on Java 6+ and Python 2.6+. For the Scala API, Spark {{site.SPARK_VERSION}} uses
Scala {{site.SCALA_BINARY_VERSION}}. You will need to use a compatible Scala version 
({{site.SCALA_BINARY_VERSION}}.x).

# Running the Examples and Shell

Spark comes with several sample programs.  Scala, Java and Python examples are in the
`examples/src/main` directory. To run one of the Java or Scala sample programs, use
`bin/run-example <class> [params]` in the top-level Spark directory. (Behind the scenes, this
invokes the more general
[`spark-submit` script](submitting-applications.html) for
launching applications). For example,

    ./bin/run-example SparkPi 10

You can also run Spark interactively through a modified version of the Scala shell. This is a
great way to learn the framework.

    ./bin/spark-shell --master local[2]

The `--master` option specifies the
[master URL for a distributed cluster](submitting-applications.html#master-urls), or `local` to run
locally with one thread, or `local[N]` to run locally with N threads. You should start by using
`local` for testing. For a full list of options, run Spark shell with the `--help` option.

Spark also provides a Python API. To run Spark interactively in a Python interpreter, use
`bin/pyspark`:

    ./bin/pyspark --master local[2]

Example applications are also provided in Python. For example,

    ./bin/spark-submit examples/src/main/python/pi.py 10

# Launching on a Cluster

The Spark [cluster mode overview](cluster-overview.html) explains the key concepts in running on a cluster.
Spark can run both by itself, or over several existing cluster managers. It currently provides several
options for deployment:

* [Amazon EC2](ec2-scripts.html): our EC2 scripts let you launch a cluster in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): simplest way to deploy Spark on a private cluster
* [Apache Mesos](running-on-mesos.html)
* [Hadoop YARN](running-on-yarn.html)

# Where to Go from Here

**Programming Guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [Spark Programming Guide](programming-guide.html): detailed overview of Spark
  in all supported languages (Scala, Java, Python)
* Modules built on Spark:
  * [Spark Streaming](streaming-programming-guide.html): processing real-time data streams
  * [Spark SQL](sql-programming-guide.html): support for structured data and relational queries
  * [MLlib](mllib-guide.html): built-in machine learning library
  * [GraphX](graphx-programming-guide.html): Spark's new API for graph processing
  * [Bagel (Pregel on Spark)](bagel-programming-guide.html): older, simple graph processing model

**API Docs:**

* [Spark Scala API (Scaladoc)](api/scala/index.html#org.apache.spark.package)
* [Spark Java API (Javadoc)](api/java/index.html)
* [Spark Python API (Epydoc)](api/python/index.html)

**Deployment Guides:**

* [Cluster Overview](cluster-overview.html): overview of concepts and components when running on a cluster
* [Submitting Applications](submitting-applications.html): packaging and deploying applications
* Deployment modes:
  * [Amazon EC2](ec2-scripts.html): scripts that let you launch a cluster on EC2 in about 5 minutes
  * [Standalone Deploy Mode](spark-standalone.html): launch a standalone cluster quickly without a third-party cluster manager
  * [Mesos](running-on-mesos.html): deploy a private cluster using
      [Apache Mesos](http://mesos.apache.org)
  * [YARN](running-on-yarn.html): deploy Spark on top of Hadoop NextGen (YARN)

**Other Documents:**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Monitoring](monitoring.html): track the behavior of your applications
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [Job Scheduling](job-scheduling.html): scheduling resources across and within Spark applications
* [Security](security.html): Spark security support
* [Hardware Provisioning](hardware-provisioning.html): recommendations for cluster hardware
* [3<sup>rd</sup> Party Hadoop Distributions](hadoop-third-party-distributions.html): using common Hadoop distributions
* Integration with other storage systems:
  * [OpenStack Swift](storage-openstack-swift.html)
* [Building Spark](building-spark.html): build Spark using the Maven system
* [Contributing to Spark](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark)
* [Supplemental Projects](https://cwiki.apache.org/confluence/display/SPARK/Supplemental+Spark+Projects): related third party Spark projects

**External Resources:**

* [Spark Homepage](http://spark.apache.org)
* [Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
* [Mailing Lists](http://spark.apache.org/mailing-lists.html): ask questions about Spark here
* [AMP Camps](http://ampcamp.berkeley.edu/): a series of training camps at UC Berkeley that featured talks and
  exercises about Spark, Spark Streaming, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/3/),
  [slides](http://ampcamp.berkeley.edu/3/) and [exercises](http://ampcamp.berkeley.edu/3/exercises/) are
  available online for free.
* [Code Examples](http://spark.apache.org/examples.html): more are also available in the `examples` subfolder of Spark ([Scala]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples),
 [Java]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples),
 [Python]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python))

# Community

To get help using Spark or keep up with Spark development, sign up for the [user mailing list](http://spark.apache.org/mailing-lists.html).

If you're in the San Francisco Bay Area, there's a regular [Spark meetup](http://www.meetup.com/spark-users/) every few weeks. Come by to meet the developers and other users.

Finally, if you'd like to contribute code to Spark, read [how to contribute](contributing-to-spark.html).
