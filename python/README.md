# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

<https://spark.apache.org/>

## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html)


## Python Packaging

This README file only contains basic information related to pip installed PySpark.
This packaging is currently experimental and may change in future versions (although we will do our best to keep compatibility).
Using PySpark requires the Spark JARs, and if you are building this from source please see the builder instructions at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

The Python packaging for Spark is not intended to replace all of the other use cases. This Python packaged version of Spark is suitable for interacting with an existing cluster (be it Spark standalone, YARN, or Mesos) - but does not contain the tools required to set up your own standalone Spark cluster. You can download the full version of Spark from the [Apache Spark downloads page](https://spark.apache.org/downloads.html).


**NOTE:** If you are using this with a Spark standalone cluster you must ensure that the version (including minor version) matches or you may experience odd errors.

## Python Requirements

At its core PySpark depends on Py4J, but some additional sub-packages have their own extra requirements for some features (including numpy, pandas, and pyarrow).
See also [Dependencies](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies) for production, and [dev/requirements.txt](https://github.com/apache/spark/blob/master/dev/requirements.txt) for development.
