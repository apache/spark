# Apache Spark On Kubernetes

This repository, located at https://github.com/apache-spark-on-k8s/spark, contains a fork of Apache Spark that enables running Spark jobs natively on a Kubernetes cluster.

## What is this?

This is a collaboratively maintained project working on [SPARK-18278](https://issues.apache.org/jira/browse/SPARK-18278). The goal is to bring native support for Spark to use Kubernetes as a cluster manager, in a fully supported way on par with the Spark Standalone, Mesos, and Apache YARN cluster managers.

## Getting Started

- [Usage guide](docs/running-on-kubernetes.md) shows how to run the code
- [Development docs](resource-managers/kubernetes/README.md) shows how to get set up for development
- Code is primarily located in the [resource-managers/kubernetes](resource-managers/kubernetes) folder

## Why does this fork exist?

Adding native integration for a new cluster manager is a large undertaking.  If poorly executed, it could introduce bugs into Spark when run on other cluster managers, cause release blockers slowing down the overall Spark project, or require hotfixes which divert attention away from development towards managing additional releases.  Any work this deep inside Spark needs to be done carefully to minimize the risk of those negative externalities.

At the same time, an increasing number of people from various companies and organizations desire to work together to natively run Spark on Kubernetes.  The group needs a code repository, communication forum, issue tracking, and continuous integration, all in order to work together effectively on an open source product.

We've been asked by an Apache Spark Committer to work outside of the Apache infrastructure for a short period of time to allow this feature to be hardened and improved without creating risk for Apache Spark.  The aim is to rapidly bring it to the point where it can be brought into the mainline Apache Spark repository for continued development within the Apache umbrella.  If all goes well, this should be a short-lived fork rather than a long-lived one.

## Who are we?

This is a collaborative effort by several folks from different companies who are interested in seeing this feature be successful.  Companies active in this project include (alphabetically):

- Google
- Haiwen
- Hyperpilot
- Intel
- Palantir
- Pepperdata
- Red Hat

--------------------

(original README below)

# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see 
[http://spark.apache.org/developer-tools.html](the Useful Developer Tools page).

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
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](http://spark.apache.org/developer-tools.html#individual-tests).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](http://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
