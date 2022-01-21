..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


==============
Best Practices
==============

Leverage PySpark APIs
---------------------

Pandas API on Spark uses Spark under the hood; therefore, many features and performance optimizations are available
in pandas API on Spark as well. Leverage and combine those cutting-edge features with pandas API on Spark.

Existing Spark context and Spark sessions are used out of the box in pandas API on Spark. If you already have your own
configured Spark context or sessions running, pandas API on Spark uses them.

If there is no Spark context or session running in your environment (e.g., ordinary Python interpreter),
such configurations can be set to ``SparkContext`` and/or ``SparkSession``.
Once a Spark context and/or session is created, pandas API on Spark can use this context and/or session automatically.
For example, if you want to configure the executor memory in Spark, you can do as below:

.. code-block:: python

   from pyspark import SparkConf, SparkContext
   conf = SparkConf()
   conf.set('spark.executor.memory', '2g')
   # Pandas API on Spark automatically uses this Spark context with the configurations set.
   SparkContext(conf=conf)

   import pyspark.pandas as ps
   ...

Another common configuration might be Arrow optimization in PySpark. In case of SQL configuration,
it can be set into Spark session as below:

.. code-block:: python

   from pyspark.sql import SparkSession
   builder = SparkSession.builder.appName("pandas-on-spark")
   builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
   # Pandas API on Spark automatically uses this Spark session with the configurations set.
   builder.getOrCreate()

   import pyspark.pandas as ps
   ...

All Spark features such as history server, web UI and deployment modes can be used as are with pandas API on Spark.
If you are interested in performance tuning, please see also `Tuning Spark <https://spark.apache.org/docs/latest/tuning.html>`_.


Check execution plans
---------------------

Expensive operations can be predicted by leveraging PySpark API `DataFrame.spark.explain()`
before the actual computation since pandas API on Spark is based on lazy execution. For example, see below.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'id': range(10)})
   >>> psdf = psdf[psdf.id > 5]
   >>> psdf.spark.explain()
   == Physical Plan ==
   *(1) Filter (id#1L > 5)
   +- *(1) Scan ExistingRDD[__index_level_0__#0L,id#1L]


Whenever you are not sure about such cases, you can check the actual execution plans and
foresee the expensive cases.

Even though pandas API on Spark tries its best to optimize and reduce such shuffle operations by leveraging Spark
optimizers, it is best to avoid shuffling in the application side whenever possible.


Use checkpoint
--------------

After a bunch of operations on pandas API on Spark objects, the underlying Spark planner can slow down due to the huge and complex plan.
If the Spark plan becomes huge or it takes the planning long time, ``DataFrame.spark.checkpoint()``
or ``DataFrame.spark.local_checkpoint()`` would be helpful.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'id': range(10)})
   >>> psdf = psdf[psdf.id > 5]
   >>> psdf['id'] = psdf['id'] + (10 * psdf['id'] + psdf['id'])
   >>> psdf = psdf.groupby('id').head(2)
   >>> psdf.spark.explain()
   == Physical Plan ==
   *(3) Project [__index_level_0__#0L, id#31L]
   +- *(3) Filter (isnotnull(__row_number__#44) AND (__row_number__#44 <= 2))
      +- Window [row_number() windowspecdefinition(__groupkey_0__#36L, __natural_order__#16L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS __row_number__#44], [__groupkey_0__#36L], [__natural_order__#16L ASC NULLS FIRST]
         +- *(2) Sort [__groupkey_0__#36L ASC NULLS FIRST, __natural_order__#16L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(__groupkey_0__#36L, 200), true, [id=#33]
               +- *(1) Project [__index_level_0__#0L, (id#1L + ((id#1L * 10) + id#1L)) AS __groupkey_0__#36L, (id#1L + ((id#1L * 10) + id#1L)) AS id#31L, __natural_order__#16L]
                  +- *(1) Project [__index_level_0__#0L, id#1L, monotonically_increasing_id() AS __natural_order__#16L]
                     +- *(1) Filter (id#1L > 5)
                        +- *(1) Scan ExistingRDD[__index_level_0__#0L,id#1L]

   >>> psdf = psdf.spark.local_checkpoint()  # or psdf.spark.checkpoint()
   >>> psdf.spark.explain()
   == Physical Plan ==
   *(1) Project [__index_level_0__#0L, id#31L]
   +- *(1) Scan ExistingRDD[__index_level_0__#0L,id#31L,__natural_order__#59L]

As you can see, the previous Spark plan is dropped and starts with a simple plan.
The result of the previous DataFrame is stored in the configured file system when calling ``DataFrame.spark.checkpoint()``,
or in the executor when calling ``DataFrame.spark.local_checkpoint()``.


Avoid shuffling
---------------

Some operations such as ``sort_values`` are more difficult to do in a parallel or distributed
environment than in in-memory on a single machine because it needs to send data to other nodes,
and exchange the data across multiple nodes via networks. See the example below.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'id': range(10)}).sort_values(by="id")
   >>> psdf.spark.explain()
   == Physical Plan ==
   *(2) Sort [id#9L ASC NULLS LAST], true, 0
   +- Exchange rangepartitioning(id#9L ASC NULLS LAST, 200), true, [id=#18]
      +- *(1) Scan ExistingRDD[__index_level_0__#8L,id#9L]

As you can see, it requires ``Exchange`` which requires a shuffle and it is likely expensive.


Avoid computation on single partition
-------------------------------------

Another common case is the computation on a single partition. Currently, some APIs such as
`DataFrame.rank <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.pandas.DataFrame.rank.html>`_
use PySpark’s Window without specifying partition specification. This moves all data into a single
partition in single machine and could cause serious performance degradation.
Such APIs should be avoided for very large datasets.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'id': range(10)})
   >>> psdf.rank().spark.explain()
   == Physical Plan ==
   *(4) Project [__index_level_0__#16L, id#24]
   +- Window [avg(cast(_w0#26 as bigint)) windowspecdefinition(id#17L, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS id#24], [id#17L]
      +- *(3) Project [__index_level_0__#16L, _w0#26, id#17L]
         +- Window [row_number() windowspecdefinition(id#17L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _w0#26], [id#17L ASC NULLS FIRST]
            +- *(2) Sort [id#17L ASC NULLS FIRST], false, 0
               +- Exchange SinglePartition, true, [id=#48]
                  +- *(1) Scan ExistingRDD[__index_level_0__#16L,id#17L]

Instead, use 
`GroupBy.rank <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.pandas.groupby.GroupBy.rank.html>`_
as it is less expensive because data can be distributed and computed for each group.


Avoid reserved column names
---------------------------

Columns with leading ``__`` and trailing ``__`` are reserved in pandas API on Spark. To handle internal behaviors for, such as, index,
pandas API on Spark uses some internal columns. Therefore, it is discouraged to use such column names and they are not guaranteed to work.


Do not use duplicated column names
----------------------------------

It is disallowed to use duplicated column names because Spark SQL does not allow this in general. Pandas API on Spark inherits
this behavior. For instance, see below:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'a': [1, 2], 'b':[3, 4]})
   >>> psdf.columns = ["a", "a"]
   ...
   Reference 'a' is ambiguous, could be: a, a.;

Additionally, it is strongly discouraged to use case sensitive column names. Pandas API on Spark disallows it by default.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'a': [1, 2], 'A':[3, 4]})
   ...
   Reference 'a' is ambiguous, could be: a, a.;

However, you can turn on ``spark.sql.caseSensitive`` in Spark configuration to enable it for use at your own risk.

.. code-block:: python

   >>> from pyspark.sql import SparkSession
   >>> builder = SparkSession.builder.appName("pandas-on-spark")
   >>> builder = builder.config("spark.sql.caseSensitive", "true")
   >>> builder.getOrCreate()

   >>> import pyspark.pandas as ps
   >>> psdf = ps.DataFrame({'a': [1, 2], 'A':[3, 4]})
   >>> psdf
      a  A
   0  1  3
   1  2  4


Specify the index column in conversion from Spark DataFrame to pandas-on-Spark DataFrame
----------------------------------------------------------------------------------------

When pandas-on-Spark Dataframe is converted from Spark DataFrame, it loses the index information, which results in using
the default index in pandas API on Spark DataFrame. The default index is inefficient in general comparing to explicitly specifying
the index column. Specify the index column whenever possible.

See  `working with PySpark <pandas_pyspark.rst#pyspark>`_


Use ``distributed`` or ``distributed-sequence`` default index
-------------------------------------------------------------

One common issue that pandas-on-Spark users face is the slow performance due to the default index. Pandas API on Spark attaches
a default index when the index is unknown, for example, Spark DataFrame is directly converted to pandas-on-Spark DataFrame.

Note that ``sequence`` requires the computation on single partition which is discouraged. If you plan
to handle large data in production, make it distributed by configuring the default index to ``distributed`` or
``distributed-sequence`` .

See `Default Index Type <options.rst#default-index-type>`_ for more details about configuring default index.


Reduce the operations on different DataFrame/Series
---------------------------------------------------

Pandas API on Spark disallows the operations on different DataFrames (or Series) by default to prevent expensive operations.
It internally performs a join operation which can be expensive in general, which is discouraged. Whenever possible,
this operation should be avoided.

See `Operations on different DataFrames <options.rst#operations-on-different-dataframes>`_ for more details.


Use pandas API on Spark directly whenever possible
---------------------------------------------------

Although pandas API on Spark has most of the pandas-equivalent APIs, there are several APIs not implemented yet or explicitly unsupported.

As an example, pandas API on Spark does not implement ``__iter__()`` to prevent users from collecting all data into the client (driver) side from the whole cluster.
Unfortunately, many external APIs such as Python built-in functions such as min, max, sum, etc. require the given argument to be iterable.
In case of pandas, it works properly out of the box as below:

.. code-block:: python

   >>> import pandas as pd
   >>> max(pd.Series([1, 2, 3]))
   3
   >>> min(pd.Series([1, 2, 3]))
   1
   >>> sum(pd.Series([1, 2, 3]))
   6

pandas dataset lives in the single machine, and is naturally iterable locally within the same machine.
However, pandas-on-Spark dataset lives across multiple machines, and they are computed in a distributed manner.
It is difficult to be locally iterable and it is very likely users collect the entire data into the client side without knowing it.
Therefore, it is best to stick to using pandas-on-Spark APIs.
The examples above can be converted as below:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.Series([1, 2, 3]).max()
   3
   >>> ps.Series([1, 2, 3]).min()
   1
   >>> ps.Series([1, 2, 3]).sum()
   6

Another common pattern from pandas users might be to rely on list comprehension or generator expression.
However, it also assumes the dataset is locally iterable under the hood.
Therefore, it works seamlessly in pandas as below:

.. code-block:: python

   >>> import pandas as pd
   >>> data = []
   >>> countries = ['London', 'New York', 'Helsinki']
   >>> pser = pd.Series([20., 21., 12.], index=countries)
   >>> for temperature in pser:
   ...     assert temperature > 0
   ...     if temperature > 1000:
   ...         temperature = None
   ...     data.append(temperature ** 2)
   ...
   >>> pd.Series(data, index=countries)
   London      400.0
   New York    441.0
   Helsinki    144.0
   dtype: float64

However, for pandas API on Spark it does not work as the same reason above.
The example above can be also changed to directly using pandas-on-Spark APIs as below:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> import numpy as np
   >>> countries = ['London', 'New York', 'Helsinki']
   >>> psser = ps.Series([20., 21., 12.], index=countries)
   >>> def square(temperature) -> np.float64:
   ...     assert temperature > 0
   ...     if temperature > 1000:
   ...         temperature = None
   ...     return temperature ** 2
   ...
   >>> psser.apply(square)
   London      400.0
   New York    441.0
   Helsinki    144.0
   dtype: float64
