# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

PySpark is the Python distribution of Spark.

Project home page: https://spark.apache.org/

Main documentation: https://spark.apache.org/docs/latest/

PySpark documentation: https://spark.apache.org/docs/latest/api/python/index.html

## PySpark on PyPI

There are a few PySpark packages published by the Apache Spark project to PyPI:

- `pyspark`: Classic PySpark, includes Spark assembly JARs
- `pyspark-connect`: Classic PySpark with Spark Connect configured as the default, includes `pyspark`
- `pyspark-client`: Pure Python Spark Connect client, no JARs or JRE needed

For more information, see the [installation guide][install]. If you're building PySpark from source, see [Building Spark][build] and [pyproject.toml][py].

[install]: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
[build]: https://spark.apache.org/docs/latest/building-spark.html
[py]: https://github.com/apache/spark/blob/master/pyproject.toml

## Python vs. "Full" Distribution of Spark

PySpark is not intended to be a complete distribution of Spark. It's meant for local development or for interacting with an existing cluster (be it Spark standalone, YARN, or Kubernetes). Using PySpark to set up a new standalone Spark cluster is not supported. To set up a standalone cluster please [use the full distribution of Spark](https://spark.apache.org/downloads.html).

When using PySpark with an existing Spark standalone cluster you must ensure that the major and minor version (e.g. `4.3.*`) match or you may experience odd errors.
