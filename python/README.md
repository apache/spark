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
guide, on the [project web page](http://spark.apache.org/documentation.html)


## Python Packaging

This README file only contains basic information related to pip installed PySpark.
This packaging is currently experimental and may change in future versions (although we will do our best to keep compatibility).
Using PySpark requires the Spark JARs, and if you are building this from source please see the builder instructions at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

The Python packaging for Spark is not intended to replace all of the other use cases. This Python packaged version of Spark is suitable for interacting with an existing cluster (be it Spark standalone, YARN, or Mesos) - but does not contain the tools required to setup your own standalone Spark cluster. You can download the full version of Spark from the [Apache Spark downloads page](http://spark.apache.org/downloads.html).


**NOTE:** If you are using this with a Spark standalone cluster you must ensure that the version (including minor version) matches or you may experience odd errors.

## Python Requirements

At its core PySpark depends on Py4J (currently version 0.10.4), but additional sub-packages have their own requirements (including numpy and pandas).