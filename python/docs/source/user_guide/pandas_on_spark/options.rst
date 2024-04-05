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


.. _user_guide.options:

====================
Options and settings
====================
.. currentmodule:: pyspark.pandas

Pandas API on Spark has an options system that lets you customize some aspects of its behaviour,
display-related options being those the user is most likely to adjust.

Options have a full "dotted-style", case-insensitive name (e.g. ``display.max_rows``).
You can get/set options directly as attributes of the top-level ``options`` attribute:


.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.options.display.max_rows
   1000
   >>> ps.options.display.max_rows = 10
   >>> ps.options.display.max_rows
   10

The API is composed of 3 relevant functions, available directly from the ``pandas_on_spark``
namespace:

* :func:`get_option` / :func:`set_option` - get/set the value of a single option.
* :func:`reset_option` - reset one or more options to their default value.

**Note:** Developers can check out `pyspark.pandas/config.py <https://github.com/apache/spark/blob/master/python/pyspark/pandas/config.py>`_ for more information.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.get_option("display.max_rows")
   1000
   >>> ps.set_option("display.max_rows", 101)
   >>> ps.get_option("display.max_rows")
   101


Getting and setting options
---------------------------

As described above, :func:`get_option` and :func:`set_option`
are available from the ``pandas_on_spark`` namespace.  To change an option, call
``set_option('option name', new_value)``.

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.get_option('compute.max_rows')
   1000
   >>> ps.set_option('compute.max_rows', 2000)
   >>> ps.get_option('compute.max_rows')
   2000

All options also have a default value, and you can use ``reset_option`` to do just that:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.reset_option("display.max_rows")

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>> ps.get_option("display.max_rows")
   1000
   >>> ps.set_option("display.max_rows", 999)
   >>> ps.get_option("display.max_rows")
   999
   >>> ps.reset_option("display.max_rows")
   >>> ps.get_option("display.max_rows")
   1000

``option_context`` context manager has been exposed through
the top-level API, allowing you to execute code with given option values. Option values
are restored automatically when you exit the `with` block:

.. code-block:: python

   >>> with ps.option_context("display.max_rows", 10, "compute.max_rows", 5):
   ...    print(ps.get_option("display.max_rows"))
   ...    print(ps.get_option("compute.max_rows"))
   10
   5
   >>> print(ps.get_option("display.max_rows"))
   >>> print(ps.get_option("compute.max_rows"))
   1000
   1000


Operations on different DataFrames
----------------------------------

Pandas API on Spark disallows the operations on different DataFrames (or Series) by default to prevent expensive
operations. It internally performs a join operation which can be expensive in general.

This can be enabled by setting `compute.ops_on_diff_frames` to `True` to allow such cases.
See the examples below.

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> ps.set_option('compute.ops_on_diff_frames', True)
    >>> psdf1 = ps.range(5)
    >>> psdf2 = ps.DataFrame({'id': [5, 4, 3]})
    >>> (psdf1 - psdf2).sort_index()
        id
    0 -5.0
    1 -3.0
    2 -1.0
    3  NaN
    4  NaN
    >>> ps.reset_option('compute.ops_on_diff_frames')

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> ps.set_option('compute.ops_on_diff_frames', True)
    >>> psdf = ps.range(5)
    >>> psser_a = ps.Series([1, 2, 3, 4])
    >>> # 'psser_a' is not from 'psdf' DataFrame. So it is considered as a Series not from 'psdf'.
    >>> psdf['new_col'] = psser_a
    >>> psdf
       id  new_col
    0   0      1.0
    1   1      2.0
    3   3      4.0
    2   2      3.0
    4   4      NaN
    >>> ps.reset_option('compute.ops_on_diff_frames')


Default Index type
------------------

In the pandas API on Spark, the default index is used in several cases, for instance,
when Spark DataFrame is converted into pandas-on-Spark DataFrame. In this case, internally pandas API on Spark attaches a
default index into pandas-on-Spark DataFrame.

There are several types of the default index that can be configured by `compute.default_index_type` as below:

**sequence**: It implements a sequence that increases one by one, by PySpark's Window function without
specifying a partition. Therefore, it can end up with a whole partition in a single node.
This index type should be avoided when the data is large. See the example below:

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> ps.set_option('compute.default_index_type', 'sequence')
    >>> psdf = ps.range(3)
    >>> ps.reset_option('compute.default_index_type')
    >>> psdf.index
    Index([0, 1, 2], dtype='int64')

This is conceptually equivalent to the PySpark example as below:

.. code-block:: python

    >>> from pyspark.sql import functions as sf, Window
    >>> import pyspark.pandas as ps
    >>> spark_df = ps.range(3).to_spark()
    >>> sequential_index = sf.row_number().over(
    ...    Window.orderBy(sf.monotonically_increasing_id().asc())) - 1
    >>> spark_df.select(sequential_index).rdd.map(lambda r: r[0]).collect()
    [0, 1, 2]

**distributed-sequence** (default): It implements a sequence that increases one by one, by group-by and
group-map approach in a distributed manner. It still generates the sequential index globally.
If the default index must be the sequence in a large dataset, this
index has to be used. See the example below:

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> ps.set_option('compute.default_index_type', 'distributed-sequence')
    >>> psdf = ps.range(3)
    >>> ps.reset_option('compute.default_index_type')
    >>> psdf.index
    Index([0, 1, 2], dtype='int64')

This is conceptually equivalent to the PySpark example as below:

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> spark_df = ps.range(3).to_spark()
    >>> spark_df.rdd.zipWithIndex().map(lambda p: p[1]).collect()
    [0, 1, 2]

**distributed**: It implements a monotonically increasing sequence simply by using
PySpark's `monotonically_increasing_id` function in a fully distributed manner. The
values are indeterministic. If the index does not have to be a sequence that increases
one by one, this index should be used. Performance-wise, this index almost does not
have any penalty compared to other index types. See the example below:

.. code-block:: python

    >>> import pyspark.pandas as ps
    >>> ps.set_option('compute.default_index_type', 'distributed')
    >>> psdf = ps.range(3)
    >>> ps.reset_option('compute.default_index_type')
    >>> psdf.index
    Index([25769803776, 60129542144, 94489280512], dtype='int64')

This is conceptually equivalent to the PySpark example as below:

.. code-block:: python

    >>> from pyspark.sql import functions as sf
    >>> import pyspark.pandas as ps
    >>> spark_df = ps.range(3).to_spark()
    >>> spark_df.select(sf.monotonically_increasing_id()) \
    ...     .rdd.map(lambda r: r[0]).collect()
    [25769803776, 60129542144, 94489280512]

.. warning::
    It is very unlikely for this type of index to be used for computing two
    different dataframes because it is not guaranteed to have the same indexes in two dataframes.
    If you use this default index and turn on `compute.ops_on_diff_frames`, the result
    from the operations between two different DataFrames will likely be an unexpected
    output due to the indeterministic index values.


Available options
-----------------

=============================== ======================= =====================================================
Option                          Default                 Description
=============================== ======================= =====================================================
display.max_rows                1000                    This sets the maximum number of rows pandas-on-Spark
                                                        should output when printing out various output. For
                                                        example, this value determines the number of rows to
                                                        be shown at the repr() in a dataframe. Set `None` to
                                                        unlimit the input length. Default is 1000.
compute.max_rows                1000                    'compute.max_rows' sets the limit of the current
                                                        pandas-on-Spark DataFrame. Set `None` to unlimit the
                                                        input length. When the limit is set, it is executed
                                                        by the shortcut by collecting the data into the
                                                        driver, and then using the pandas API. If the limit
                                                        is unset, the operation is executed by PySpark.
                                                        Default is 1000.
compute.shortcut_limit          1000                    'compute.shortcut_limit' sets the limit for a
                                                        shortcut. It computes specified number of rows and
                                                        use its schema. When the dataframe length is larger
                                                        than this limit, pandas-on-Spark uses PySpark to
                                                        compute.
compute.ops_on_diff_frames      False                   This determines whether or not to operate between two
                                                        different dataframes. For example, 'combine_frames'
                                                        function internally performs a join operation which
                                                        can be expensive in general. So, if
                                                        `compute.ops_on_diff_frames` variable is not True,
                                                        that method throws an exception.
compute.default_index_type      'distributed-sequence'  This sets the default index type: sequence,
                                                        distributed and distributed-sequence.
compute.default_index_cache     'MEMORY_AND_DISK_SER'   This sets the default storage level for temporary
                                                        RDDs cached in distributed-sequence indexing: 'NONE',
                                                        'DISK_ONLY', 'DISK_ONLY_2', 'DISK_ONLY_3',
                                                        'MEMORY_ONLY', 'MEMORY_ONLY_2', 'MEMORY_ONLY_SER',
                                                        'MEMORY_ONLY_SER_2', 'MEMORY_AND_DISK',
                                                        'MEMORY_AND_DISK_2', 'MEMORY_AND_DISK_SER',
                                                        'MEMORY_AND_DISK_SER_2', 'OFF_HEAP',
                                                        'LOCAL_CHECKPOINT'.
compute.ordered_head            False                   'compute.ordered_head' sets whether or not to operate
                                                        head with natural ordering. pandas-on-Spark does not
                                                        guarantee the row ordering so `head` could return
                                                        some rows from distributed partitions. If
                                                        'compute.ordered_head' is set to True, pandas-on-
                                                        Spark performs natural ordering beforehand, but it
                                                        will cause a performance overhead.
compute.eager_check             True                    'compute.eager_check' sets whether or not to launch
                                                        some Spark jobs just for the sake of validation. If
                                                        'compute.eager_check' is set to True, pandas-on-Spark
                                                        performs the validation beforehand, but it will cause
                                                        a performance overhead. Otherwise, pandas-on-Spark
                                                        skip the validation and will be slightly different
                                                        from pandas. Affected APIs: `Series.dot`,
                                                        `Series.asof`, `Series.compare`,
                                                        `FractionalExtensionOps.astype`,
                                                        `IntegralExtensionOps.astype`,
                                                        `FractionalOps.astype`, `DecimalOps.astype`, `skipna
                                                        of statistical functions`.
compute.isin_limit              80                      'compute.isin_limit' sets the limit for filtering by
                                                        'Column.isin(list)'. If the length of the ‘list’ is
                                                        above the limit, broadcast join is used instead for
                                                        better performance.
compute.pandas_fallback         False                   'compute.pandas_fallback' sets whether or not to
                                                        fallback automatically to Pandas' implementation.
plotting.max_rows               1000                    'plotting.max_rows' sets the visual limit on top-n-
                                                        based plots such as `plot.bar` and `plot.pie`. If it
                                                        is set to 1000, the first 1000 data points will be
                                                        used for plotting. Default is 1000.
plotting.sample_ratio           None                    'plotting.sample_ratio' sets the proportion of data
                                                        that will be plotted for sample-based plots such as
                                                        `plot.line` and `plot.area`. This option defaults to
                                                        'plotting.max_rows' option.
plotting.backend                'plotly'                Backend to use for plotting. Default is plotly.
                                                        Supports any package that has a top-level `.plot`
                                                        method. Known options are: [matplotlib, plotly].
=============================== ======================= =====================================================
