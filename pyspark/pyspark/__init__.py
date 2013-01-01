"""
PySpark is a Python API for Spark.

Public classes:

    - L{SparkContext<pyspark.context.SparkContext>}
        Main entry point for Spark functionality.
    - L{RDD<pyspark.rdd.RDD>}
        A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "pyspark/lib/py4j0.7.egg"))


from pyspark.context import SparkContext
from pyspark.rdd import RDD


__all__ = ["SparkContext", "RDD"]
