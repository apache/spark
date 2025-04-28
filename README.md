Apache Spark

Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Scala, Java, Python, and R (deprecated), and an optimized engine that supports general computation graphs for data analysis. It also supports a rich set of higher-level tools, including Spark SQL for SQL and DataFrames, pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for stream processing.

    Official version: https://spark.apache.org/

    Development version: https://apache.github.io/spark/

GitHub Actions Build PySpark Coverage PyPI Downloads
Online Documentation

The latest Spark documentation including a programming guide is available on the project web page. This README file only contains basic setup instructions.
Building Spark

Spark is built using Apache Maven. To build Spark and its example programs, run:

./build/mvn -DskipTests clean package

If you downloaded a pre-built package, you do not need to do this. For detailed guidance, visit the "Building Spark" page. You can also explore the "Useful Developer Tools" page for tips on using an IDE.
Interactive Scala Shell

Start using Spark through the Scala shell:

./bin/spark-shell

Example command:

scala> spark.range(1000 * 1000 * 1000).count()

Interactive Python Shell

For Python users, use the Python shell:

./bin/pyspark

Example command:

>>> spark.range(1000 * 1000 * 1000).count()

Example Programs

Spark includes several example programs in the examples directory. To run one, use ./bin/run-example <class> [params]. For example:

./bin/run-example SparkPi

You can also set the MASTER environment variable to submit examples to a cluster, such as a spark:// URL, "yarn" for YARN, or "local" for local execution.
Running Tests

After building Spark, tests can be run using:

./dev/run-tests

For more information on running specific tests, refer to the "Running Tests for a Module" documentation.
Hadoop Version Compatibility

Since Spark uses Hadoop's core library to interact with HDFS and other Hadoop-supported storage systems, it's important to build Spark against the version of Hadoop used by your cluster. For more details, see "Specifying the Hadoop Version and Enabling YARN".
Configuration

To configure Spark, refer to the Configuration Guide.
Contributing

To contribute to Apache Spark, please review the Contribution Guide.

You can use this as the foundation for raising a request or submitting documentation. Let me know if you need further changes!


ChatGPT can make mistakes. Check important info.
