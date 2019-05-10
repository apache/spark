---
layout: global
displayTitle: Spark Overview
title: Overview
description: Apache Spark SPARK_VERSION_SHORT documentation homepage
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Apache Spark is a unified analytics engine for large-scale data processing.
It provides high-level APIs in Java, Scala, Python and R,
and an optimized engine that supports general execution graphs.
It also supports a rich set of higher-level tools including [Spark SQL](sql-programming-guide.html) for SQL and structured data processing, [MLlib](ml-guide.html) for machine learning, [GraphX](graphx-programming-guide.html) for graph processing, and [Structured Streaming](structured-streaming-programming-guide.html) for incremental computation and stream processing.

# Security

Security in Spark is OFF by default. This could mean you are vulnerable to attack by default.
Please see [Spark Security](security.html) before downloading and running Spark.

# Downloading

Get Spark from the [downloads page](https://spark.apache.org/downloads.html) of the project website. This documentation is for Spark version {{site.SPARK_VERSION}}. Spark uses Hadoop's client libraries for HDFS and YARN. Downloads are pre-packaged for a handful of popular Hadoop versions.
Users can also download a "Hadoop free" binary and run Spark with any Hadoop version
[by augmenting Spark's classpath](hadoop-provided.html).
Scala and Java users can include Spark in their projects using its Maven coordinates and Python users can install Spark from PyPI.


If you'd like to build Spark from 
source, visit [Building Spark](building-spark.html).


Spark runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS). It's easy to run
locally on one machine --- all you need is to have `java` installed on your system `PATH`,
or the `JAVA_HOME` environment variable pointing to a Java installation.

Spark runs on Java 8+, Scala 2.12, Python 2.7+/3.4+ and R 3.1+.
R prior to version 3.4 support is deprecated as of Spark 3.0.0.
For the Scala API, Spark {{site.SPARK_VERSION}}
uses Scala {{site.SCALA_BINARY_VERSION}}. You will need to use a compatible Scala version
({{site.SCALA_BINARY_VERSION}}.x).

# Running the Examples and Shell

Spark comes with several sample programs.  Scala, Java, Python and R examples are in the
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

Spark also provides an [R API](sparkr.html) since 1.4 (only DataFrames APIs included).
To run Spark interactively in an R interpreter, use `bin/sparkR`:

    ./bin/sparkR --master local[2]

Example applications are also provided in R. For example,

    ./bin/spark-submit examples/src/main/r/dataframe.R

# Launching on a Cluster

The Spark [cluster mode overview](cluster-overview.html) explains the key concepts in running on a cluster.
Spark can run both by itself, or over several existing cluster managers. It currently provides several
options for deployment:

* [Standalone Deploy Mode](spark-standalone.html): simplest way to deploy Spark on a private cluster
* [Apache Mesos](running-on-mesos.html)
* [Hadoop YARN](running-on-yarn.html)
* [Kubernetes](running-on-kubernetes.html)

# Where to Go from Here

**Programming Guides:**

* [Quick Start](quick-start.html): a quick introduction to the Spark API; start here!
* [RDD Programming Guide](rdd-programming-guide.html): overview of Spark basics - RDDs (core but old API), accumulators, and broadcast variables  
* [Spark SQL, Datasets, and DataFrames](sql-programming-guide.html): processing structured data with relational queries (newer API than RDDs)
* [Structured Streaming](structured-streaming-programming-guide.html): processing structured data streams with relation queries (using Datasets and DataFrames, newer API than DStreams)
* [Spark Streaming](streaming-programming-guide.html): processing data streams using DStreams (old API)
* [MLlib](ml-guide.html): applying machine learning algorithms
* [GraphX](graphx-programming-guide.html): processing graphs 

**API Docs:**

* [Spark Scala API (Scaladoc)](api/scala/index.html#org.apache.spark.package)
* [Spark Java API (Javadoc)](api/java/index.html)
* [Spark Python API (Sphinx)](api/python/index.html)
* [Spark R API (Roxygen2)](api/R/index.html)
* [Spark SQL, Built-in Functions (MkDocs)](api/sql/index.html)

**Deployment Guides:**

* [Cluster Overview](cluster-overview.html): overview of concepts and components when running on a cluster
* [Submitting Applications](submitting-applications.html): packaging and deploying applications
* Deployment modes:
  * [Amazon EC2](https://github.com/amplab/spark-ec2): scripts that let you launch a cluster on EC2 in about 5 minutes
  * [Standalone Deploy Mode](spark-standalone.html): launch a standalone cluster quickly without a third-party cluster manager
  * [Mesos](running-on-mesos.html): deploy a private cluster using
      [Apache Mesos](https://mesos.apache.org)
  * [YARN](running-on-yarn.html): deploy Spark on top of Hadoop NextGen (YARN)
  * [Kubernetes](running-on-kubernetes.html): deploy Spark on top of Kubernetes

**Other Documents:**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Monitoring](monitoring.html): track the behavior of your applications
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [Job Scheduling](job-scheduling.html): scheduling resources across and within Spark applications
* [Security](security.html): Spark security support
* [Hardware Provisioning](hardware-provisioning.html): recommendations for cluster hardware
* Integration with other storage systems:
  * [Cloud Infrastructures](cloud-integration.html)
  * [OpenStack Swift](storage-openstack-swift.html)
* [Building Spark](building-spark.html): build Spark using the Maven system
* [Contributing to Spark](https://spark.apache.org/contributing.html)
* [Third Party Projects](https://spark.apache.org/third-party-projects.html): related third party Spark projects

**External Resources:**

* [Spark Homepage](https://spark.apache.org)
* [Spark Community](https://spark.apache.org/community.html) resources, including local meetups
* [StackOverflow tag `apache-spark`](http://stackoverflow.com/questions/tagged/apache-spark)
* [Mailing Lists](https://spark.apache.org/mailing-lists.html): ask questions about Spark here
* [AMP Camps](http://ampcamp.berkeley.edu/): a series of training camps at UC Berkeley that featured talks and
  exercises about Spark, Spark Streaming, Mesos, and more. [Videos](http://ampcamp.berkeley.edu/6/),
  [slides](http://ampcamp.berkeley.edu/6/) and [exercises](http://ampcamp.berkeley.edu/6/exercises/) are
  available online for free.
* [Code Examples](https://spark.apache.org/examples.html): more are also available in the `examples` subfolder of Spark ([Scala]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples),
 [Java]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples),
 [Python]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python),
 [R]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/r))
