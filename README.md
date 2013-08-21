# Spark

Lightning-Fast Cluster Computing - <http://www.spark-project.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the project webpage at <http://spark-project.org/documentation.html>.
This README file only contains basic setup instructions.


## Building

Spark requires Scala 2.9.3 (Scala 2.10 is not yet supported). The project is
built using Simple Build Tool (SBT), which is packaged with it. To build
Spark and its example programs, run:

    sbt/sbt package assembly

Spark also supports building using Maven. If you would like to build using Maven,
see the [instructions for building Spark with Maven](http://spark-project.org/docs/latest/building-with-maven.html)
in the spark documentation..

To run Spark, you will need to have Scala's bin directory in your `PATH`, or
you will need to set the `SCALA_HOME` environment variable to point to where
you've installed Scala. Scala must be accessible through one of these
methods on your cluster's worker nodes as well as its master.

To run one of the examples, use `./run <class> <params>`. For example:

    ./run spark.examples.SparkLR local[2]

will run the Logistic Regression example locally on 2 CPUs.

Each of the example programs prints usage help if no params are given.

All of the Spark samples take a `<host>` parameter that is the cluster URL
to connect to. This can be a mesos:// or spark:// URL, or "local" to run
locally with one thread, or "local[N]" to run locally with N threads.


## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `SPARK_HADOOP_VERSION` environment
when building Spark.

For Apache Hadoop versions 1.x, 0.20.x, Cloudera CDH MRv1, and other Hadoop
versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ SPARK_HADOOP_VERSION=1.2.1 sbt/sbt package assembly

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ SPARK_HADOOP_VERSION=2.0.0-mr1-cdh4.2.0 sbt/sbt package assembly

For Apache Hadoop 2.x, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions
with YARN, also set `SPARK_WITH_YARN=true`:

    # Apache Hadoop 2.0.5-alpha
    $ SPARK_HADOOP_VERSION=2.0.5-alpha SPARK_WITH_YARN=true sbt/sbt package assembly

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ SPARK_HADOOP_VERSION=2.0.0-cdh4.2.0 SPARK_WITH_YARN=true sbt/sbt package assembly

For convenience, these variables may also be set through the `conf/spark-env.sh` file
described below.

When developing a Spark application, specify the Hadoop version by adding the
"hadoop-client" artifact to your project's dependencies. For example, if you're
using Hadoop 1.0.1 and build your application using SBT, add this to
`libraryDependencies`:

    // "force()" is required because "1.0.1" is less than Spark's default of "1.0.4"
    "org.apache.hadoop" % "hadoop-client" % "1.0.1" force()

If your project is built with Maven, add this to your POM file's `<dependencies>` section:

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <!-- the brackets are needed to tell Maven that this is a hard dependency on version "1.0.1" exactly -->
      <version>[1.0.1]</version>
    </dependency>


## Configuration

Please refer to the "Configuration" guide in the online documentation for a
full overview on how to configure Spark. At the minimum, you will need to
create a `conf/spark-env.sh` script (copy `conf/spark-env.sh.template`) and
set the following two variables:

- `SCALA_HOME`: Location where Scala is installed.

- `MESOS_NATIVE_LIBRARY`: Your Mesos library (only needed if you want to run
  on Mesos). For example, this might be `/usr/local/lib/libmesos.so` on Linux.


## Contributing to Spark

Contributions via GitHub pull requests are gladly accepted from their original
author. Along with any pull requests, please state that the contribution is
your original work and that you license the work to the project under the
project's open source license. Whether or not you state this explicitly, by
submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.
