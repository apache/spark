===
FAQ
===

What's the project's status?
----------------------------

Starting from Apache Spark 3.2, we support pandas API layer on PySpark
You might still face the following differences:

 - Most of pandas-equivalent APIs are implemented but still some may be missing.
   Please create a GitHub issue if your favorite function is not yet supported.
   We also document all APIs that are not yet supported in the `missing directory <https://github.com/apache/spark/tree/master/python/pyspark/pandas/missing>`_.

 - Some behaviors may be different, in particular in the treatment of nulls: Pandas uses
   Not a Number (NaN) special constants to indicate missing values, while Spark has a
   special flag on each value to indicate missing values. We would love to hear from you
   if you come across any discrepancies

 - Because Spark is lazy in nature, some operations like creating new columns only get
   performed when Spark needs to print or write the dataframe.

Should I use PySpark's DataFrame API or pandas on Spark?
-----------------------------------------------

If you are already familiar with pandas and want to leverage Spark for big data, we recommend
using pandas on Spark. If you are learning Spark from ground up, we recommend you start with PySpark's API.

Does pandas on Spark support Structured Streaming?
-----------------------------------------

No, pandas on Spark does not support Structured Streaming officially.

As a workaround, you can use pandas on Spark APIs with `foreachBatch` in Structured Streaming which allows batch APIs:

.. code-block:: python

   >>> def func(batch_df, batch_id):
   ...     pdf = ps.DataFrame(batch_df)
   ...     pdf['a'] = 1
   ...     print(koalas_df)

   >>> spark.readStream.format("rate").load().writeStream.foreachBatch(func).start()
                   timestamp  value  a
   0 2020-02-21 09:49:37.574      4  1
                   timestamp  value  a
   0 2020-02-21 09:49:38.574      5  1
   ...

How can I request support for a method?
---------------------------------------

File a GitHub issue: https://issues.apache.org/jira/projects/SPARK/issues

Databricks customers are also welcome to file a support ticket to request a new feature.

How is pandas on Spark different from Dask?
----------------------------------

Different projects have different focuses. Spark is already deployed in virtually every
organization, and often is the primary interface to the massive amount of data stored in data lakes.
pandas on Spark was inspired by Dask, and aims to make the transition from pandas to Spark easy for data
scientists.

How can I contribute to pandas on Spark?
-------------------------------

See `Contributing Guide <https://spark.apache.org/docs/3.1.2/api/python/development/contributing.html#code-and-docstring-guide>`_.

