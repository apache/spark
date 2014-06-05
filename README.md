# Apache Spark

Lightning-Fast Cluster Computing - <http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the project webpage at <http://spark.apache.org/documentation.html>.
This README file only contains basic setup instructions.

## Building Spark

Spark is built on Scala 2.10. To build Spark and its example programs, run:

    ./sbt/sbt assembly

(You do not need to do this if you downloaded a pre-built package.)

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark
    
And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL, 
"yarn-cluster" or "yarn-client" to run on YARN, and "local" to run 
locally with one thread, or "local[N]" to run locally with N threads. You 
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./sbt/sbt test

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `SPARK_HADOOP_VERSION` environment
when building Spark.

For Apache Hadoop versions 1.x, Cloudera CDH MRv1, and other Hadoop
versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ SPARK_HADOOP_VERSION=1.2.1 sbt/sbt assembly

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ SPARK_HADOOP_VERSION=2.0.0-mr1-cdh4.2.0 sbt/sbt assembly

For Apache Hadoop 2.2.X, 2.1.X, 2.0.X, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions
with YARN, also set `SPARK_YARN=true`:

    # Apache Hadoop 2.0.5-alpha
    $ SPARK_HADOOP_VERSION=2.0.5-alpha SPARK_YARN=true sbt/sbt assembly

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ SPARK_HADOOP_VERSION=2.0.0-cdh4.2.0 SPARK_YARN=true sbt/sbt assembly

    # Apache Hadoop 2.2.X and newer
    $ SPARK_HADOOP_VERSION=2.2.0 SPARK_YARN=true sbt/sbt assembly

When developing a Spark application, specify the Hadoop version by adding the
"hadoop-client" artifact to your project's dependencies. For example, if you're
using Hadoop 1.2.1 and build your application using SBT, add this entry to
`libraryDependencies`:

    "org.apache.hadoop" % "hadoop-client" % "1.2.1"

If your project is built with Maven, add this to your POM file's `<dependencies>` section:

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>1.2.1</version>
    </dependency>


## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.


## Contributing to Spark

Contributions via GitHub pull requests are gladly accepted from their original
author. Along with any pull requests, please state that the contribution is
your original work and that you license the work to the project under the
project's open source license. Whether or not you state this explicitly, by
submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.

