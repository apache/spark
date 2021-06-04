===
FAQ
===

What's the project's status?
----------------------------

Koalas 1.0.0 was released, and it is much more stable now.
You might still face the following differences:

 - Most of pandas-equivalent APIs are implemented but still some may be missing.
   Please create a GitHub issue if your favorite function is not yet supported.
   We also document all APIs that are not yet supported in the `missing directory <https://github.com/pyspark.pandas/tree/master/databricks/koalas/missing>`_.

 - Some behaviors may be different, in particular in the treatment of nulls: Pandas uses
   Not a Number (NaN) special constants to indicate missing values, while Spark has a
   special flag on each value to indicate missing values. We would love to hear from you
   if you come across any discrepancies

 - Because Spark is lazy in nature, some operations like creating new columns only get
   performed when Spark needs to print or write the dataframe.

Is it Koalas or koalas?
-----------------------

It's Koalas. Unlike pandas, we use upper case here.

Should I use PySpark's DataFrame API or Koalas?
-----------------------------------------------

If you are already familiar with pandas and want to leverage Spark for big data, we recommend
using Koalas. If you are learning Spark from ground up, we recommend you start with PySpark's API.

Does Koalas support Structured Streaming?
-----------------------------------------

No, Koalas does not support Structured Streaming officially.

As a workaround, you can use Koalas APIs with `foreachBatch` in Structured Streaming which allows batch APIs:

.. code-block:: python

   >>> def func(batch_df, batch_id):
   ...     koalas_df = ks.DataFrame(batch_df)
   ...     koalas_df['a'] = 1
   ...     print(koalas_df)

   >>> spark.readStream.format("rate").load().writeStream.foreachBatch(func).start()
                   timestamp  value  a
   0 2020-02-21 09:49:37.574      4  1
                   timestamp  value  a
   0 2020-02-21 09:49:38.574      5  1
   ...

How can I request support for a method?
---------------------------------------

File a GitHub issue: https://github.com/pyspark.pandas/issues

Databricks customers are also welcome to file a support ticket to request a new feature.

How is Koalas different from Dask?
----------------------------------

Different projects have different focuses. Spark is already deployed in virtually every
organization, and often is the primary interface to the massive amount of data stored in data lakes.
Koalas was inspired by Dask, and aims to make the transition from pandas to Spark easy for data
scientists.

How can I contribute to Koalas?
-------------------------------

See `Contributing Guide <https://koalas.readthedocs.io/en/latest/development/contributing.html>`_.

Why a new project (instead of putting this in Apache Spark itself)?
-------------------------------------------------------------------

Two reasons:

1. We want a venue in which we can rapidly iterate and make new releases. The overhead of making a
release as a separate project is minuscule (in the order of minutes). A release on Spark takes a
lot longer (in the order of days)

2. Koalas takes a different approach that might contradict Spark's API design principles, and those
principles cannot be changed lightly given the large user base of Spark. A new, separate project
provides an opportunity for us to experiment with new design principles.
