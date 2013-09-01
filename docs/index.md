---
layout: global
title: Spark Overview
---

Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in [Scala](scala-programming-guide.html), [Java](java-programming-guide.html), and [Python](python-programming-guide.html) that make parallel jobs easy to write, and an optimized engine that supports general computation graphs.
Spark can run on the Apache Mesos cluster manager, Hadoop YARN, Amazon EC2, or without an independent resource manager ("standalone mode").

# Downloading

Get Spark by visiting the [downloads page](http://spark.incubator.apache.org/downloads.html) of the Apache Spark site. This documentation is for Spark version {{site.SPARK_VERSION}}.

# Building

Spark uses [Simple Build Tool](http://www.scala-sbt.org), which is bundled with it. To compile the code, go into the top-level Spark directory and run

    sbt/sbt assembly

For its Scala API, Spark {{site.SPARK_VERSION}} depends on Scala {{site.SCALA_VERSION}}. If you write applications in Scala, you will need to use this same version of Scala in your own program -- newer major versions may not work. You can get the right version of Scala from [scala-lang.org](http://www.scala-lang.org/download/).

# Testing the Build

Spark comes with several sample programs in the `examples` directory.
To run one of the samples, use `./run-example <class> <params>` in the top-level Spark directory
(the `run-example` script sets up the appropriate paths and launches that program).
For example, try `./run-example org.apache.spark.examples.SparkPi local`.
Each example prints usage help when run with no parameters.

Note that all of the sample programs take a `<master>` parameter specifying the cluster URL
to connect to. This can be a [URL for a distributed cluster](scala-programming-guide.html#master-urls),
or `local` to run locally with one thread, or `local[N]` to run locally with N threads. You should start by using
`local` for testing.

Finally, Spark can be used interactively through modified versions of the Scala shell (`./spark-shell`) or
Python interpreter (`./pyspark`). These are a great way to learn Spark.

# A Note About Hadoop Versions

Spark uses the Hadoop-client library to talk to HDFS and other Hadoop-supported
storage systems. Because the HDFS protocol has changed in different versions of
Hadoop, you must build Spark against the same version that your cluster uses.
By default, Spark links to Hadoop 1.0.4. You can change this by setting the
`SPARK_HADOOP_VERSION` variable when compiling:

    SPARK_HADOOP_VERSION=1.2.1 sbt/sbt assembly

In addition, if you wish to run Spark on [YARN](running-on-yarn.md), set
`SPARK_YARN` to `true`:

    SPARK_HADOOP_VERSION=2.0.5-alpha SPARK_YARN=true sbt/sbt assembly

# Where to Go from Here

**Programming guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [Spark Programming Guide](scala-programming-guide.html): an overview of Spark concepts, and details on the Scala API
  * [Java Programming Guide](java-programming-guide.html): using Spark from Java
  * [Python Programming Guide](python-programming-guide.html): using Spark from Python
* [Spark Streaming](streaming-programming-guide.html): using the alpha release of Spark Streaming
* [MLlib (Machine Learning)](mllib-guide.html): Spark's built-in machine learning library
* [Bagel (Pregel on Spark)](bagel-programming-guide.html): simple graph processing model

**API Docs:**

* [Spark for Java/Scala (Scaladoc)](api/core/index.html)
* [Spark for Python (Epydoc)](api/pyspark/index.html)
* [Spark Streaming for Java/Scala (Scaladoc)](api/streaming/index.html)
* [MLlib (Machine Learning) for Java/Scala (Scaladoc)](api/mllib/index.html)
* [Bagel (Pregel on Spark) for Scala (Scaladoc)](api/bagel/index.html)


**Deployment guides:**

* [Running Spark on Amazon EC2](ec2-scripts.html): scripts that let you launch a cluster on EC2 in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): launch a standalone cluster quickly without a third-party cluster manager
* [Running Spark on Mesos](running-on-mesos.html): deploy a private cluster using
    [Apache Mesos](http://incubator.apache.org/mesos)
* [Running Spark on YARN](running-on-yarn.html): deploy Spark on top of Hadoop NextGen (YARN)

**Other documents:**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [Hardware Provisioning](hardware-provisioning.html): recommendations for cluster hardware
* [Building Spark with Maven](building-with-maven.html): Build Spark using the Maven build tool
* [Contributing to Spark](contributing-to-spark.html)

**External resources:**

* [Spark Homepage](http://spark.incubator.apache.org)
* [Mailing Lists](http://spark.incubator.apache.org/mailing-lists.html): ask questions about Spark here
* [AMP Camps](http://ampcamp.berkeley.edu/): a series of training camps at UC Berkeley that featured talks and
  exercises about Spark, Shark, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/agenda-2012),
  [slides](http://ampcamp.berkeley.edu/agenda-2012) and [exercises](http://ampcamp.berkeley.edu/exercises-2012) are
  available online for free.
* [Code Examples](http://spark.incubator.apache.org/examples.html): more are also available in the [examples subfolder](https://github.com/mesos/spark/tree/master/examples/src/main/scala/) of Spark
* [Paper Describing Spark](http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)
* [Paper Describing Spark Streaming](http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-259.pdf)

# Community

To get help using Spark or keep up with Spark development, sign up for the [user mailing list](http://spark.incubator.apache.org/mailing-lists.html).

If you're in the San Francisco Bay Area, there's a regular [Spark meetup](http://www.meetup.com/spark-users/) every few weeks. Come by to meet the developers and other users.

Finally, if you'd like to contribute code to Spark, read [how to contribute](contributing-to-spark.html).
