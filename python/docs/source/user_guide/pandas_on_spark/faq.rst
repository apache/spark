===
FAQ
===

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

How is Koalas different from Dask?
----------------------------------

Different projects have different focuses. Spark is already deployed in virtually every
organization, and often is the primary interface to the massive amount of data stored in data lakes.
Koalas was inspired by Dask, and aims to make the transition from pandas to Spark easy for data
scientists.

