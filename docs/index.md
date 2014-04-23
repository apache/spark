---
layout: global
title: Getting Started with Apache Spark 
---

Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in [Scala](scala-programming-guide.html), [Java](java-programming-guide.html), and [Python](python-programming-guide.html) that make parallel jobs easy to write, and an optimized engine that supports general computation graphs.
It also supports a rich set of higher-level tools including [Shark](http://shark.cs.berkeley.edu) (Hive on Spark), [MLlib](mllib-guide.html) for machine learning, [GraphX](graphx-programming-guide.html) for graph processing, and [Spark Streaming](streaming-programming-guide.html).

# Downloading

Get Spark by visiting the [downloads page](http://spark.apache.org/downloads.html) of the Apache Spark site. This documentation is for Spark version {{site.SPARK_VERSION}}.

Spark runs on both Windows and Unix-like systems (e.g., Linux, Mac OS). All you need to run it is to have Java installed on your system `PATH` or point the `JAVA_HOME` environment variable to a Java installation.

Note: The Spark Programming Guide is written through a Scala lens, so Java and Python developers may wish to download and install Scala so they can work hands-on with the Scala examples in the Spark Programming Guide. 

For its Scala API, Spark {{site.SPARK_VERSION}} depends on Scala {{site.SCALA_BINARY_VERSION}}. If you write applications in
Scala, you will need to use a compatible Scala version (*e.g.*, {{site.SCALA_BINARY_VERSION}}.X) -- newer major versions may no
t work. You can get the appropriate version of Scala from [scala-lang.org](http://www.scala-lang.org/download/).

# Building

Spark uses the Hadoop-client library to talk to HDFS and other Hadoop-supported
storage systems. Because the HDFS protocol has changed in different versions of
Hadoop, you must build Spark against the same version that your cluster uses.

Spark is bundled with the [Simple Build Tool](http://www.scala-sbt.org) (SBT). 

To compile the code so Spark links to Hadoop 1.0.4 (default), from the top-level Spark directory run:

    $ sbt/sbt assembly

You can change the Hadoop version that Spark links to by setting the 
`SPARK_HADOOP_VERSION` environment variable when compiling. For example:

    $ SPARK_HADOOP_VERSION=2.2.0 sbt/sbt assembly

If you wish to run Spark on [YARN](running-on-yarn.html), set 
`SPARK_YARN` to `true`. For example:

    $ SPARK_HADOOP_VERSION=2.0.5-alpha SPARK_YARN=true sbt/sbt assembly

Note: If you're using the Windows Command Prompt run each command separately:

    > set SPARK_HADOOP_VERSION=2.0.5-alpha
    > set SPARK_YARN=true
    > sbt/sbt assembly

# Running Spark Examples 

Spark comes with a number of sample programs.  Scala and Java examples are in the `examples` directory, and Python examples are in the `python/examples` directory.

To run one of the Java or Scala sample programs, in the top-level Spark directory: 

    $ ./bin/run-example <class> <params> 

The `bin/run-example` script sets up the appropriate paths and launches the specified program. 
For example, try this Scala program:

    $ ./bin/run-example org.apache.spark.examples.SparkPi local

Or run this Java program:

    $ ./bin/run-example org.apache.spark.examples.JavaSparkPi local

To run a Python sample program, in the top-level Spark directory:

    $ ./bin/pyspark <sample-program> <params> 
    
For example, try:

    $ ./bin/pyspark ./python/examples/pi.py local

Each example prints usage help when run without parameters:
    
    $ ./bin/run-example org.apache.spark.examples.JavaWordCount
    Usage: JavaWordCount <master> <file>

    $ ./bin/run-example org.apache.spark.examples.JavaWordCount local README.md
        
The README.md file is located in the top-level Spark directory.

Note that all of the sample programs take a `<master>` parameter specifying the cluster URL
to connect to. This can be a [URL for a distributed cluster](scala-programming-guide.html#master-urls),
`local` to run locally with one thread, or `local[N]` to run locally with N threads. We recommend starting by using
`local` for testing.

# Using the Spark Shells

You can run Spark interactively through modified versions of the Scala shell or
the Python interpreter. These are great ways to learn the Spark framework.

To run Spark's Scala shell:

    $ ./bin/spark-shell
    ...
    >

To run Spark's Python interpreter:

    $ ./bin/pyspark
    ...
    >>>

# Launching on a Cluster

The Spark [cluster mode overview](cluster-overview.html) explains the key concepts in running on a cluster.
Spark can run by itself or over several existing cluster managers. There are currently several
options for deployment:

* [Amazon EC2](ec2-scripts.html): our EC2 scripts let you launch a cluster in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): the simplest way to deploy Spark on a private cluster
* [Apache Mesos](running-on-mesos.html)
* [Hadoop YARN](running-on-yarn.html)

# Where to Go from Here

**Programming Guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [Spark Programming Guide](scala-programming-guide.html): an overview of Spark concepts though the lens of the Scala API
  * [Java Programming Guide](java-programming-guide.html): using Spark from Java
  * [Python Programming Guide](python-programming-guide.html): using Spark from Python
* [Spark Streaming](streaming-programming-guide.html): Spark's API for processing data streams
* [Spark SQL](sql-programming-guide.html): Support for running relational queries on Spark
* [MLlib (Machine Learning)](mllib-guide.html): Spark's built-in machine learning library
* [Bagel (Pregel on Spark)](bagel-programming-guide.html): simple graph processing model
* [GraphX (Graphs on Spark)](graphx-programming-guide.html): Spark's new API for graphs

**API Docs:**

* [Spark for Java/Scala (Scaladoc)](api/core/index.html)
* [Spark for Python (Epydoc)](api/pyspark/index.html)
* [Spark Streaming for Java/Scala (Scaladoc)](api/streaming/index.html)
* [MLlib (Machine Learning) for Java/Scala (Scaladoc)](api/mllib/index.html)
* [Bagel (Pregel on Spark) for Scala (Scaladoc)](api/bagel/index.html)
* [GraphX (Graphs on Spark) for Scala (Scaladoc)](api/graphx/index.html)


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
  exercises about Spark, Shark, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/agenda-2012),
  [slides](http://ampcamp.berkeley.edu/agenda-2012) and [exercises](http://ampcamp.berkeley.edu/exercises-2012) are
  available online for free.
* [Code Examples](http://spark.apache.org/examples.html): more are also available in the [examples subfolder](https://github.com/apache/spark/tree/master/examples/src/main/scala/) of Spark
* [Paper Describing Spark](http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)
* [Paper Describing Spark Streaming](http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-259.pdf)

# Community

To get help using Spark or keep up with Spark development, sign up for the [user mailing list](http://spark.apache.org/mailing-lists.html).

If you're in the San Francisco Bay Area, there's a regular [Spark meetup](http://www.meetup.com/spark-users/) every few weeks. Come by to meet the developers and other users.

Finally, if you'd like to contribute code to Spark, read [how to contribute](contributing-to-spark.html).
