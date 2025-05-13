# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R (Deprecated), and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

- Official version: <https://spark.apache.org/>
- Development version: <https://apache.github.io/spark/>

[![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_main.yml)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pyspark?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/pyspark/)


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

# Daily Build Pipeline Status

| Branch     | Status                                                                                                                                                                                                          | Desc |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| master     | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_java21.yml)                                     |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml)                                 |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_uds.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_uds.yml)                                           |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml)         |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven.yml)                                       |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml)                         |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos15.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos15.yml)         |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml)                 |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml)                             |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml)                     |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_ps_minimum.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_ps_minimum.yml)               |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.10.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.10.yml)                           |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11_classic_only.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.11_classic_only.yml) |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_connect.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_connect.yml)                     |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11_macos.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.11_macos.yml)               |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.11_arm.yml)                   |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_numpy_2.1.3.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_numpy_2.1.3.yml)             |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12.yml)                           |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml)                           |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.13_nogil.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.13_nogil.yml)               |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml)                   |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml)                       |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml)                             |      |
| branch-4.0 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40.yml)                                 |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_java21.yml)                   |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_non_ansi.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_non_ansi.yml)               |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_maven.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_maven.yml)                     |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_maven_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_maven_java21.yml)       |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_python.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_python.yml)                   |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch40_python_pypy3.10.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch40_python_pypy3.10.yml) |      |
| branch-3.5 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch35.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch35.yml)                                 |      |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_branch35_python.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_branch35_python.yml)                   |      |


## Building Spark

Spark is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
./build/mvn -DskipTests clean package
```

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

```bash
./bin/spark-shell
```

Try the following command, which should return 1,000,000,000:

```scala
scala> spark.range(1000 * 1000 * 1000).count()
```

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```bash
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```python
>>> spark.range(1000 * 1000 * 1000).count()
```

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

```bash
./bin/run-example SparkPi
```

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

```bash
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

```bash
./dev/run-tests
```

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
