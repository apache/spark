# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for stream processing.

PySpark is the Python distribution of Spark.

Project home page: https://spark.apache.org/

Online documentation: https://spark.apache.org/docs/latest/

PySpark documentation: https://spark.apache.org/docs/latest/api/python/index.html

## Python vs. "Full" Distribution of Spark

PySpark is not intended to be a complete distribution of Spark. It's suitable for local development or for interacting with an existing cluster (be it Spark standalone, YARN, or Kubernetes), but it is not intended to be used to set up your own standalone Spark cluster. To set up a standalone cluster you need to [use the full version of Spark](https://spark.apache.org/downloads.html).

**NOTE:** If you are using this with a Spark standalone cluster you must ensure that the version (including minor version) matches or you may experience odd errors.

Using PySpark requires the Spark JARs, which are included in this distribution. If you are building this from source please see [the builder instructions](https://spark.apache.org/docs/latest/building-spark.html).

## Python Requirements

At its core PySpark depends on Py4J, but some additional sub-packages have their own extra requirements for some features (including numpy, pandas, and pyarrow).
See also [Dependencies](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies) for production, and [pyproject.toml](https://github.com/apache/spark/blob/master/pyproject.toml) for development.
