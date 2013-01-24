"""
PySpark is a Python API for Spark.

Public classes:

    - L{SparkContext<pyspark.context.SparkContext>}
        Main entry point for Spark functionality.
    - L{RDD<pyspark.rdd.RDD>}
        A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
    - L{Broadcast<pyspark.broadcast.Broadcast>}
        A broadcast variable that gets reused across tasks.
    - L{Accumulator<pyspark.accumulators.Accumulator>}
        An "add-only" shared variable that tasks can only add values to.
    - L{SparkFiles<pyspark.files.SparkFiles>}
        Access files shipped with jobs.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python/lib/py4j0.7.egg"))


from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles


__all__ = ["SparkContext", "RDD", "SparkFiles"]
