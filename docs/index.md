---
layout: global
title: Spark Overview
---

{% comment %}
TODO(andyk): Rewrite to make the Java API a first class part of the story.
{% endcomment %}

Spark is a MapReduce-like cluster computing framework designed for low-latency iterative jobs and interactive use from an 
interpreter. It provides clean, language-integrated APIs in Scala and Java, with a rich array of parallel operators. Spark can 
run on top of the [Apache Mesos](http://incubator.apache.org/mesos/) cluster manager, 
[Hadoop YARN](http://hadoop.apache.org/docs/r2.0.1-alpha/hadoop-yarn/hadoop-yarn-site/YARN.html),
Amazon EC2, or without an independent resource manager ("standalone mode"). 

# Downloading

Get Spark by visiting the [downloads page](http://spark-project.org/downloads.html) of the Spark website. This documentation is for Spark version {{site.SPARK_VERSION}}.

# Building

Spark requires [Scala {{site.SCALA_VERSION}}](http://www.scala-lang.org/). You will need to have Scala's `bin` directory in your `PATH`,
or you will need to set the `SCALA_HOME` environment variable to point
to where you've installed Scala. Scala must also be accessible through one
of these methods on slave nodes on your cluster.

Spark uses [Simple Build Tool](https://github.com/harrah/xsbt/wiki), which is bundled with it. To compile the code, go into the top-level Spark directory and run

    sbt/sbt package

# Testing the Build

Spark comes with a number of sample programs in the `examples` directory.
To run one of the samples, use `./run <class> <params>` in the top-level Spark directory
(the `run` script sets up the appropriate paths and launches that program).
For example, `./run spark.examples.SparkPi` will run a sample program that estimates Pi. Each of the
examples prints usage help if no params are given.

Note that all of the sample programs take a `<master>` parameter specifying the cluster URL
to connect to. This can be a [URL for a distributed cluster](scala-programming-guide.html#master-urls),
or `local` to run locally with one thread, or `local[N]` to run locally with N threads. You should start by using
`local` for testing.

Finally, Spark can be used interactively from a modified version of the Scala interpreter that you can start through
`./spark-shell`. This is a great way to learn Spark.

# A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the HDFS protocol has changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `HADOOP_VERSION` variable at the top
of `project/SparkBuild.scala`, then rebuilding Spark (`sbt/sbt clean compile`).

# Where to Go from Here

**Programming guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [Spark Programming Guide](scala-programming-guide.html): an overview of Spark concepts, and details on the Scala API
* [Java Programming Guide](java-programming-guide.html): using Spark from Java

**Deployment guides:**

* [Running Spark on Amazon EC2](ec2-scripts.html): scripts that let you launch a cluster on EC2 in about 5 minutes
* [Standalone Deploy Mode](spark-standalone.html): launch a standalone cluster quickly without a third-party cluster manager
* [Running Spark on Mesos](running-on-mesos.html): deploy a private cluster using
    [Apache Mesos](http://incubator.apache.org/mesos)
* [Running Spark on YARN](running-on-yarn.html): deploy Spark on top of Hadoop NextGen (YARN)

**Other documents:**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [API Docs (Scaladoc)](api/core/index.html)
* [Bagel](bagel-programming-guide.html): an implementation of Google's Pregel on Spark
* [Contributing to Spark](contributing-to-spark.html)

**External resources:**

* [Spark Homepage](http://www.spark-project.org)
* [Mailing List](http://groups.google.com/group/spark-users): ask questions about Spark here
* [AMP Camp](http://ampcamp.berkeley.edu/): a two-day training camp at UC Berkeley that featured talks and exercises
  about Spark, Shark, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/agenda-2012),
  [slides](http://ampcamp.berkeley.edu/agenda-2012) and [exercises](http://ampcamp.berkeley.edu/exercises-2012) are
  available online for free.
* [Code Examples](http://spark-project.org/examples.html): more are also available in the [examples subfolder](https://github.com/mesos/spark/tree/master/examples/src/main/scala/spark/examples) of Spark
* [Paper Describing the Spark System](http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)

# Community

To get help using Spark or keep up with Spark development, sign up for the [spark-users mailing list](http://groups.google.com/group/spark-users).

If you're in the San Francisco Bay Area, there's a regular [Spark meetup](http://www.meetup.com/spark-users/) every few weeks. Come by to meet the developers and other users.

Finally, if you'd like to contribute code to Spark, read [how to contribute](contributing-to-spark.html).
