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

==================
Arrow Python UDFs
==================

.. currentmodule:: pyspark.sql.functions

Native Arrow UDFs operate directly on ``pyarrow.Array`` objects without converting to Pandas or
row-by-row Python objects. This preserves the columnar layout end-to-end, avoids unnecessary data
copies, and enables vectorized processing using Arrow's native compute functions.

A Native Arrow UDF is defined using :func:`arrow_udf` as a decorator or wrapper, and no additional
configuration is required.

.. note::

    Native Arrow UDFs can also be defined via :func:`udf` with ``pyarrow.Array`` type hints.
    The type hints in the function signature determine which kind of Arrow UDF is created
    (e.g., returning ``pa.Array`` creates an array-to-array UDF, while returning ``float``
    creates an aggregate UDF).

Native Arrow UDF Types
----------------------

Arrays to Array
~~~~~~~~~~~~~~~

The type hint can be expressed as ``pyarrow.Array``, ... -> ``pyarrow.Array``.

The function takes one or more ``pyarrow.Array`` and outputs one ``pyarrow.Array``.
The output should always be of the same length as the input.

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("string")
    def to_upper(s: pa.Array) -> pa.Array:
        return pa.compute.ascii_upper(s)

    df = spark.createDataFrame([("John Doe",)], ("name",))
    df.select(to_upper("name")).show()
    # +--------------+
    # |to_upper(name)|
    # +--------------+
    # |      JOHN DOE|
    # +--------------+

When the ``returnType`` is a struct type, the function returns a ``pa.StructArray``:

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("first string, last string")
    def split_expand(v: pa.Array) -> pa.Array:
        b = pa.compute.ascii_split_whitespace(v)
        s0 = pa.array([t[0] for t in b])
        s1 = pa.array([t[1] for t in b])
        return pa.StructArray.from_arrays([s0, s1], names=["first", "last"])

    df = spark.createDataFrame([("John Doe",)], ("name",))
    df.select(split_expand("name")).show()
    # +------------------+
    # |split_expand(name)|
    # +------------------+
    # |       {John, Doe}|
    # +------------------+

Arrow UDFs support keyword arguments:

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql import functions as sf
    from pyspark.sql.functions import arrow_udf
    from pyspark.sql.types import IntegerType

    @arrow_udf(returnType=IntegerType())
    def calc(a: pa.Array, b: pa.Array) -> pa.Array:
        return pa.compute.add(a, pa.compute.multiply(b, 10))

    spark.range(2).select(calc(b=sf.col("id") * 10, a=sf.col("id"))).show()
    # +-----------------------------+
    # |calc(b => (id * 10), a => id)|
    # +-----------------------------+
    # |                            0|
    # |                          101|
    # +-----------------------------+

Iterator of Arrays to Iterator of Arrays
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type hint can be expressed as ``Iterator[pyarrow.Array]`` -> ``Iterator[pyarrow.Array]``.

The function takes an iterator of ``pyarrow.Array`` and outputs an iterator of ``pyarrow.Array``.
The length of the entire output should be the same as the entire input.
This is useful when the UDF execution requires expensive initialization.

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf
    from typing import Iterator

    @arrow_udf("long")
    def plus_one(iterator: Iterator[pa.Array]) -> Iterator[pa.Array]:
        for v in iterator:
            yield pa.compute.add(v, 1)

    df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
    df.select(plus_one(df.v)).show()
    # +-----------+
    # |plus_one(v)|
    # +-----------+
    # |          2|
    # |          3|
    # |          4|
    # +-----------+

Iterator of Multiple Arrays to Iterator of Arrays
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type hint can be expressed as ``Iterator[Tuple[pyarrow.Array, ...]]`` -> ``Iterator[pyarrow.Array]``.

The function takes an iterator of a tuple of multiple ``pyarrow.Array`` and outputs an
iterator of ``pyarrow.Array``. Use this when the UDF requires multiple input columns.

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql import functions as sf
    from pyspark.sql.functions import arrow_udf
    from typing import Iterator, Tuple

    @arrow_udf("long")
    def multiply(
        iterator: Iterator[Tuple[pa.Array, pa.Array]]
    ) -> Iterator[pa.Array]:
        for v1, v2 in iterator:
            yield pa.compute.multiply(v1, v2.field("v"))

    df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
    df.withColumn(
        'output', multiply(sf.col("v"), sf.struct(sf.col("v")))
    ).show()
    # +---+------+
    # |  v|output|
    # +---+------+
    # |  1|     1|
    # |  2|     4|
    # |  3|     9|
    # +---+------+

Arrays to Scalar
~~~~~~~~~~~~~~~~

The type hint can be expressed as ``pyarrow.Array``, ... -> ``Any``.

The function takes one or more ``pyarrow.Array`` and returns a scalar value. The return type
annotation can be any type other than ``pa.Array``, ``Iterator``, or ``Tuple``, which match the
array-to-array or iterator patterns above. The returned scalar can be
a Python primitive type (e.g., ``int`` or ``float``), a NumPy data type, or a ``pyarrow.Scalar``
instance which supports complex return types.

This type of UDF can be used with :meth:`GroupedData.agg <pyspark.sql.GroupedData.agg>` and
``Window`` operations.

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("double")
    def mean_udf(v: pa.Array) -> float:
        return pa.compute.mean(v).as_py()

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
    df.groupby("id").agg(mean_udf(df['v'])).show()
    # +---+-----------+
    # | id|mean_udf(v)|
    # +---+-----------+
    # |  1|        1.5|
    # |  2|        6.0|
    # +---+-----------+

The return type can also be a complex type such as struct:

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("struct<m1: double, m2: double>")
    def min_max_udf(v: pa.Array) -> pa.Scalar:
        m1 = pa.compute.min(v)
        m2 = pa.compute.max(v)
        t = pa.struct([pa.field("m1", pa.float64()), pa.field("m2", pa.float64())])
        return pa.scalar(value={"m1": m1.as_py(), "m2": m2.as_py()}, type=t)

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
    df.groupby("id").agg(min_max_udf(df['v'])).show()
    # +---+--------------+
    # | id|min_max_udf(v)|
    # +---+--------------+
    # |  1|    {1.0, 2.0}|
    # |  2|   {3.0, 10.0}|
    # +---+--------------+

This UDF can also be used as window functions:

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql import Window
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("double")
    def mean_udf(v: pa.Array) -> float:
        return pa.compute.mean(v).as_py()

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
    w = Window.partitionBy('id').orderBy('v').rowsBetween(-1, 0)
    df.withColumn('mean_v', mean_udf("v").over(w)).show()
    # +---+----+------+
    # | id|   v|mean_v|
    # +---+----+------+
    # |  1| 1.0|   1.0|
    # |  1| 2.0|   1.5|
    # |  2| 3.0|   3.0|
    # |  2| 5.0|   4.0|
    # |  2|10.0|   7.5|
    # +---+----+------+

.. note:: For performance reasons, the input arrays to window functions are not copied.
    Mutating the input arrays is not allowed and will cause incorrect results.

Iterator of Arrays to Scalar
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type hint can be expressed as ``Iterator[pyarrow.Array]`` -> a scalar type.

The function takes an iterator of ``pyarrow.Array`` and returns a scalar value. This is useful for
grouped aggregations where the UDF can process all batches iteratively, which is more
memory-efficient than loading all data at once.

.. note:: Only a single UDF is supported per aggregation.

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udf
    from typing import Iterator

    @arrow_udf("double")
    def arrow_mean(it: Iterator[pa.Array]) -> float:
        sum_val = 0.0
        cnt = 0
        for v in it:
            sum_val += pa.compute.sum(v).as_py()
            cnt += len(v)
        return sum_val / cnt

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
    df.groupby("id").agg(arrow_mean(df['v'])).show()
    # +---+-------------+
    # | id|arrow_mean(v)|
    # +---+-------------+
    # |  1|          1.5|
    # |  2|          6.0|
    # +---+-------------+

Iterator of Multiple Arrays to Scalar
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type hint can be expressed as ``Iterator[Tuple[pyarrow.Array, ...]]`` -> a scalar type.

The function takes an iterator of a tuple of multiple ``pyarrow.Array`` and returns a scalar value.
This is useful for grouped aggregations with multiple input columns.

.. note:: Only a single UDF is supported per aggregation.

.. code-block:: python

    import pyarrow as pa
    import numpy as np
    from pyspark.sql.functions import arrow_udf
    from typing import Iterator, Tuple

    @arrow_udf("double")
    def arrow_weighted_mean(
        it: Iterator[Tuple[pa.Array, pa.Array]]
    ) -> float:
        weighted_sum = 0.0
        weight = 0.0
        for v, w in it:
            weighted_sum += np.dot(v, w)
            weight += pa.compute.sum(w).as_py()
        return weighted_sum / weight

    df = spark.createDataFrame(
        [(1, 1.0, 1.0), (1, 2.0, 2.0), (2, 3.0, 1.0), (2, 5.0, 2.0), (2, 10.0, 3.0)],
        ("id", "v", "w"))
    df.groupby("id").agg(arrow_weighted_mean(df["v"], df["w"])).show()
    # +---+-------------------------+
    # | id|arrow_weighted_mean(v, w)|
    # +---+-------------------------+
    # |  1|       1.6666666666666...|
    # |  2|        7.166666666666...|
    # +---+-------------------------+


Arrow Function APIs
-------------------

Arrow Function APIs apply Python native functions directly on Arrow data at the DataFrame level.
They work similarly to Pandas Function APIs but use ``pyarrow.RecordBatch`` and ``pyarrow.Table``
instead of Pandas DataFrames.

Map
~~~

.. currentmodule:: pyspark.sql

:meth:`DataFrame.mapInArrow` maps an iterator of ``pyarrow.RecordBatch`` to another iterator of
``pyarrow.RecordBatch``. The input and output can have different lengths.

.. code-block:: python

    import pyarrow as pa

    df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def filter_func(iterator):
        for batch in iterator:
            yield batch.filter(pa.compute.field("id") == 1)

    df.mapInArrow(filter_func, df.schema).show()
    # +---+---+
    # | id|age|
    # +---+---+
    # |  1| 21|
    # +---+---+

For detailed usage, please see :meth:`DataFrame.mapInArrow`.

Grouped Map
~~~~~~~~~~~

``DataFrame.groupBy().applyInArrow()`` maps each group using a function that takes a
``pyarrow.Table`` and returns a ``pyarrow.Table``.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))

    def normalize(table):
        v = table.column("v")
        norm = pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
        return table.set_column(1, "v", norm)

    df.groupby("id").applyInArrow(
        normalize, schema="id long, v double"
    ).sort("id", "v").show()
    # +---+-------------------+
    # | id|                  v|
    # +---+-------------------+
    # |  1|-0.7071067811865...|
    # |  1| 0.7071067811865...|
    # |  2|-0.8320502943378...|
    # |  2|-0.2773500981126...|
    # |  2| 1.1094003924504...|
    # +---+-------------------+

The function can also accept grouping keys as the first argument:

.. code-block:: python

    def mean_func(key, table):
        mean = pc.mean(table.column("v"))
        return pa.Table.from_pydict({"id": [key[0].as_py()], "v": [mean.as_py()]})

    df.groupby('id').applyInArrow(
        mean_func, schema="id long, v double"
    ).sort("id").show()
    # +---+---+
    # | id|  v|
    # +---+---+
    # |  1|1.5|
    # |  2|6.0|
    # +---+---+

For detailed usage, please see :meth:`GroupedData.applyInArrow`.

Co-grouped Map
~~~~~~~~~~~~~~

``DataFrame.groupBy().cogroup().applyInArrow()`` allows two DataFrames to be cogrouped by a
common key and then a Python function applied to each cogroup. The function takes two
``pyarrow.Table`` and returns a ``pyarrow.Table``.

.. code-block:: python

    import pyarrow as pa

    df1 = spark.createDataFrame(
        [(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)], ("id", "v1"))
    df2 = spark.createDataFrame([(1, "x"), (2, "y")], ("id", "v2"))

    def summarize(l, r):
        return pa.Table.from_pydict({
            "left": [l.num_rows],
            "right": [r.num_rows]
        })

    df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
        summarize, schema="left long, right long"
    ).show()
    # +----+-----+
    # |left|right|
    # +----+-----+
    # |   2|    1|
    # |   2|    1|
    # +----+-----+

The function can also accept grouping keys as the first argument:

.. code-block:: python

    def summarize(key, l, r):
        return pa.Table.from_pydict({
            "key": [key[0].as_py()],
            "left": [l.num_rows],
            "right": [r.num_rows]
        })

    df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
        summarize, schema="key long, left long, right long"
    ).sort("key").show()
    # +---+----+-----+
    # |key|left|right|
    # +---+----+-----+
    # |  1|   2|    1|
    # |  2|   2|    1|
    # +---+----+-----+

For detailed usage, please see :meth:`PandasCogroupedOps.applyInArrow`.

Notes
-----

SQL boolean expressions do not short-circuit: in ``WHERE cond AND udf(x)``, the UDF may be
called on all rows regardless of ``cond``. If the function can fail on certain input values
(e.g., division by zero), handle those cases inside the function itself.

The Arrow data type of the returned ``pyarrow.Array`` should match the declared ``returnType``.
When there is a mismatch, Spark will attempt to convert the returned data to the expected type
using Arrow's safe casting, which raises an error on overflow or precision loss.

Supported SQL types are the same as for Arrow-based conversion. See
`Supported SQL Types <arrow_pandas.rst#supported-sql-types>`_ for details.

See Also
--------

.. currentmodule:: pyspark.sql.functions

* :func:`udf` -- Create a Python UDF (with optional Arrow optimization)
* :func:`arrow_udtf` -- Create a vectorized Arrow UDTF (see `Arrow Python UDTFs <arrow_python_udtf.rst>`_)
* :func:`pandas_udf` -- Create a Pandas UDF
