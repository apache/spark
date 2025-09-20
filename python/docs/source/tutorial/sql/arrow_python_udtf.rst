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

======================================================
Vectorized Python User-defined Table Functions (UDTFs)
======================================================

Spark 4.1 introduces the Vectorized Python user-defined table function (UDTF), a new type of user-defined table-valued function.
It can be used via the ``@arrow_udtf`` decorator.
Unlike scalar functions that return a single result value from each call, each UDTF is invoked in
the ``FROM`` clause of a query and returns an entire table as output.
Unlike the traditional Python UDTF that evaluates row by row, the Vectorized Python UDTF lets you directly operate on top of Apache Arrow arrays and column batches.
This allows you to leverage vectorized operations and improve the performance of your UDTF.

Vectorized Python UDTF Interface
--------------------------------

.. currentmodule:: pyspark.sql.functions

.. code-block:: python

    class NameYourArrowPythonUDTF:

        def __init__(self) -> None:
            """
            Initializes the user-defined table function (UDTF). This is optional.

            This method serves as the default constructor and is called once when the
            UDTF is instantiated on the executor side.

            Any class fields assigned in this method will be available for subsequent
            calls to the `eval`, `terminate` and `cleanup` methods.

            Notes
            -----
            - You cannot create or reference the Spark session within the UDTF. Any
              attempt to do so will result in a serialization error.
            """
            ...

        def eval(self, *args: Any) -> Iterator[pa.RecordBatch | pa.Table]:
            """
            Evaluates the function using the given input arguments.

            This method is required and must be implemented.

            Argument Mapping:
            - Each provided scalar expression maps to exactly one value in the
              `*args` list with type `pa.Array`.
            - Each provided table argument maps to a `pa.RecordBatch` object containing
              the columns in the order they appear in the provided input table,
              and with the names computed by the query analyzer.

            This method is called on every batch of input rows, and can produce zero or more
            output pyarrow record batches or pyarrow tables. Each element in the output tuple
            corresponds to one column specified in the return type of the UDTF.

            Parameters
            ----------
            *args : Any
                Arbitrary positional arguments representing the input to the UDTF.

            Yields
            ------
            iterator
                An iterator of `pa.RecordBatch` or `pa.Table` objects representing a batch of rows
                in the UDTF result table. Yield as many times as needed to produce multiple batches.

            Notes
            -----
            - UDTFs can instead accept keyword arguments during the function call if needed.
            - The `eval` method can raise a `SkipRestOfInputTableException` to indicate that the
              UDTF wants to skip consuming all remaining rows from the current partition of the
              input table. This will cause the UDTF to proceed directly to the `terminate` method.
            - The `eval` method can raise any other exception to indicate that the UDTF should be
              aborted entirely. This will cause the UDTF to skip the `terminate` method and proceed
              directly to the `cleanup` method, and then the exception will be propagated to the
              query processor causing the invoking query to fail.

            Examples
            --------
            This `eval` method takes a table argument and returns an arrow record batch for each input batch.

            >>> def eval(self, batch: pa.RecordBatch):
            ...     yield batch

            This `eval` method takes a table argument and returns a pyarrow table for each input batch.

            >>> def eval(self, batch: pa.RecordBatch):
            ...     yield pa.table({"x": batch.column(0), "y": batch.column(1)})

            This `eval` method takes both table and scalar arguments and returns a pyarrow table for each input batch.

            >>> def eval(self, batch: pa.RecordBatch, x: pa.Array):
            ...     yield pa.table({"x": x, "y": batch.column(0)})
            """
            ...

        def terminate(self) -> Iterator[pa.RecordBatch | pa.Table]:
            """
            Called when the UDTF has successfully processed all input rows.

            This method is optional to implement and is useful for performing any
            finalization operations after the UDTF has finished processing
            all rows. It can also be used to yield additional rows if needed.
            Table functions that consume all rows in the entire input partition
            and then compute and return the entire output table can do so from
            this method as well (please be mindful of memory usage when doing
            this).

            If any exceptions occur during input row processing, this method
            won't be called.

            Yields
            ------
            iterator
                An iterator of `pa.RecordBatch` or `pa.Table` objects representing a batch of rows
                in the UDTF result table. Yield as many times as needed to produce multiple batches.

            Examples
            --------
            >>> def terminate(self) -> Iterator[pa.RecordBatch | pa.Table]:
            >>>     yield pa.table({"x": pa.array([1, 2, 3])})
            """
            ...

        def cleanup(self) -> None:
            """
            Invoked after the UDTF completes processing input rows.

            This method is optional to implement and is useful for final cleanup
            regardless of whether the UDTF processed all input rows successfully
            or was aborted due to exceptions.

            Examples
            --------
            >>> def cleanup(self) -> None:
            >>>     self.conn.close()
            """
            ...

Defining the Output Schema
--------------------------

The return type of the UDTF defines the schema of the table it outputs.
You can specify it in the ``@arrow_udtf`` decorator.

It must be either a ``StructType``:

.. code-block:: python

    @arrow_udtf(returnType=StructType().add("c1", StringType()).add("c2", IntegerType()))
    class YourArrowPythonUDTF:
        ...

or a DDL string representing a struct type:

.. code-block:: python

    @arrow_udtf(returnType="c1 string, c2 int")
    class YourArrowPythonUDTF:
        ...

Emitting Output Rows
--------------------

The `eval` and `terminate` methods then emit zero or more output batches conforming to this schema by
yielding ``pa.RecordBatch`` or ``pa.Table`` objects.

.. code-block:: python

    @arrow_udtf(returnType="c1 int, c2 int")
    class YourArrowPythonUDTF:
        def eval(self, batch: pa.RecordBatch):
            yield pa.table({"c1": batch.column(0), "c2": batch.column(1)})

You can also yield multiple pyarrow tables in the `eval` method.

.. code-block:: python

    @arrow_udtf(returnType="c1 int")
    class YourArrowPythonUDTF:
        def eval(self, batch: pa.RecordBatch):
            yield pa.table({"c1": batch.column(0)})
            yield pa.table({"c1": batch.column(1)})

You can also yield multiple pyarrow record batches in the `eval` method.

.. code-block:: python

    @arrow_udtf(returnType="c1 int")
    class YourArrowPythonUDTF:
        def eval(self, batch: pa.RecordBatch):
            new_batch = pa.record_batch(
                {"c1": batch.column(0).slice(0, len(batch) // 2)})
            yield new_batch


Usage Examples
--------------

Here's how to use these UDTFs in DataFrame:

.. code-block:: python

    import pyarrow as pa
    from pyspark.sql.functions import arrow_udtf

    @arrow_udtf(returnType="c1 string")
    class MyArrowPythonUDTF:
        def eval(self, batch: pa.RecordBatch):
            yield pa.table({"c1": batch.column("value")})

    df = spark.range(10).selectExpr("id", "cast(id as string) as value")
    MyArrowPythonUDTF(df.asTable()).show()
    # Result:
    # +---+
    # | c1|
    # +---+
    # |  0|
    # |  1|
    # |  2|
    # |  3|
    # |  4|
    # |  5|
    # |  6|
    # |  7|
    # |  8|
    # |  9|
    # +---+

    # Register the UDTF
    spark.udtf.register("my_arrow_udtf", MyArrowPythonUDTF)

    # Use in SQL queries
    df = spark.sql("""
        SELECT * FROM my_arrow_udtf(TABLE(SELECT id, cast(id as string) as value FROM range(10)))
    """)


TABLE Argument
--------------

Arrow UDTFs can take a TABLE argument. When your UDTF receives a TABLE argument,
its ``eval`` method is called with a ``pyarrow.RecordBatch`` containing the input
table’s columns, and any additional scalar/struct expressions are passed as
``pyarrow.Array`` values.

Key points:
- The TABLE argument is a single ``pa.RecordBatch``; access columns by name or index.
- Scalar arguments (including structs) are ``pa.Array`` values, not ``RecordBatch``.
- Named and positional arguments are both supported in SQL.

Example (DataFrame API):

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator, Optional
    from pyspark.sql.functions import arrow_udtf, SkipRestOfInputTableException

    @arrow_udtf(returnType="value int")
    class EchoTable:
        def eval(self, batch: pa.RecordBatch) -> Iterator[pa.Table]:
            # Return the input column named "value" as-is
            yield pa.table({"value": batch.column("value")})

    df = spark.range(5).selectExpr("id as value")
    EchoTable(df.asTable()).show()

    # Result:
    # +-----+
    # |value|
    # +-----+
    # |    0|
    # |    1|
    # |    2|
    # |    3|
    # |    4|
    # +-----+

Example (SQL): TABLE plus a scalar threshold

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator
    from pyspark.sql.functions import arrow_udtf

    # Keep rows with value > threshold; works with SQL using TABLE + scalar argument
    @arrow_udtf(returnType="partition_key int, value int")
    class ThresholdFilter:
        def eval(self, batch: pa.RecordBatch, threshold: pa.Array) -> Iterator[pa.Table]:
            tbl = pa.table(batch)
            thr = int(threshold.cast(pa.int64())[0].as_py())
            mask = pc.greater(tbl["value"], thr)
            yield tbl.filter(mask)

    spark.udtf.register("threshold_filter", ThresholdFilter)
    spark.createDataFrame([(1, 10), (1, 30), (2, 5)], "partition_key int, value int").createOrReplaceTempView("v")

    spark.sql(
        """
        SELECT *
        FROM threshold_filter(
          TABLE(v),
          10
        )
        ORDER BY partition_key, value
        """
    ).show()

    # Result:
    # +-------------+-----+
    # |partition_key|value|
    # +-------------+-----+
    # |            1|   30|
    # +-------------+-----+


PARTITION BY and ORDER BY
-------------------------

Arrow UDTFs support ``TABLE(...) PARTITION BY ... ORDER BY ...``. Think of it as
“process rows group by group, and in a specific order within each group”.

Semantics:

- PARTITION BY groups rows by the given keys; your UDTF runs for each group independently.
- ORDER BY controls the row order within each group as seen by ``eval``.
- ``eval`` may be called multiple times per group; accumulate state and typically emit the group's result in ``terminate``.

Example: Aggregation per key with terminate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PARTITION BY is especially useful for per-group aggregation. ``eval`` may be called
multiple times for the same group as rows arrive in batches, so keep running totals in
the UDTF instance and emit the final row in ``terminate``.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator
    from pyspark.sql.functions import arrow_udtf

    @arrow_udtf(returnType="user_id int, total_amount int, rows int")
    class SumPerUser:
        def __init__(self):
            self._user = None
            self._sum = 0
            self._count = 0

        def eval(self, batch: pa.RecordBatch) -> Iterator[pa.Table]:
            tbl = pa.table(batch)
            # All rows in this batch belong to the same user within a partition
            self._user = pc.unique(tbl["user_id"]).to_pylist()[0]
            self._sum += pc.sum(tbl["amount"]).as_py()
            self._count += tbl.num_rows
            return iter(())  # emit once in terminate

        def terminate(self) -> Iterator[pa.Table]:
            if self._user is not None:
                yield pa.table({
                    "user_id": pa.array([self._user], pa.int32()),
                    "total_amount": pa.array([self._sum], pa.int32()),
                    "rows": pa.array([self._count], pa.int32()),
                })

    spark.udtf.register("sum_per_user", SumPerUser)
    spark.createDataFrame(
        [(1, 10), (2, 5), (1, 20), (2, 15), (3, 7)],
        "user_id int, amount int",
    ).createOrReplaceTempView("purchases")

    spark.sql(
        """
        SELECT *
        FROM sum_per_user(
          TABLE(purchases)
          PARTITION BY user_id
        )
        ORDER BY user_id
        """
    ).show()

    # Result:
    # +-------+------------+----+
    # |user_id|total_amount|rows|
    # +-------+------------+----+
    # |      1|          30|   2|
    # |      2|          20|   2|
    # |      3|           7|   1|
    # +-------+------------+----+

Why terminate? ``eval`` may run multiple times per group if the input is split into
several batches. Emitting the aggregated row in ``terminate`` guarantees exactly one
output row per group after all its rows have been processed.

Example: Top reviews per product using ORDER BY
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator, Optional
    from pyspark.sql.functions import arrow_udtf, SkipRestOfInputTableException

    @arrow_udtf(returnType="product_id int, review_id int, rating int, review string")
    class TopReviewsPerProduct:
        TOP_K = 3

        def __init__(self):
            self._product = None
            self._seen = 0
            self._batches: list[pa.Table] = []
            self._top: Optional[pa.Table] = None

        def eval(self, batch: pa.RecordBatch) -> Iterator[pa.Table]:
            tbl = pa.table(batch)
            if tbl.num_rows == 0:
                return iter(())

            products = pc.unique(tbl["product_id"]).to_pylist()
            assert len(products) == 1, f"Expected one product per batch, saw {products}"
            product = products[0]

            if self._product is None:
                self._product = product
            else:
                assert self._product == product, f"Mixed products {self._product} and {product}"

            self._batches.append(tbl)
            self._seen += tbl.num_rows

            if self._seen >= self.TOP_K and self._top is None:
                combined = pa.concat_tables(self._batches)
                self._top = combined.slice(0, self.TOP_K)
                raise SkipRestOfInputTableException(
                    f"Collected top {self.TOP_K} reviews for product {self._product}"
                )

            return iter(())

        def terminate(self) -> Iterator[pa.Table]:
            if self._product is None:
                return iter(())

            if self._top is None:
                combined = pa.concat_tables(self._batches) if self._batches else pa.table({})
                limit = min(self.TOP_K, self._seen)
                self._top = combined.slice(0, limit)

            yield self._top

    spark.udtf.register("top_reviews_per_product", TopReviewsPerProduct)
    spark.createDataFrame(
        [
            (101, 1, 5, "Amazing battery life"),
            (101, 2, 5, "Still great after a month"),
            (101, 3, 4, "Solid build"),
            (101, 4, 3, "Average sound"),
            (202, 5, 5, "My go-to lens"),
            (202, 6, 4, "Sharp and bright"),
            (202, 7, 4, "Great value"),
        ],
        "product_id int, review_id int, rating int, review string",
    ).createOrReplaceTempView("reviews")

    spark.sql(
        """
        SELECT *
        FROM top_reviews_per_product(
          TABLE(reviews)
          PARTITION BY (product_id)
          ORDER BY (rating DESC, review_id)
        )
        ORDER BY product_id, rating DESC, review_id
        """
    ).show()

    # Result:
    # +----------+---------+------+--------------------------+
    # |product_id|review_id|rating|review                    |
    # +----------+---------+------+--------------------------+
    # |       101|        1|     5|Amazing battery life      |
    # |       101|        2|     5|Still great after a month |
    # |       101|        3|     4|Solid build               |
    # |       202|        5|     5|My go-to lens             |
    # |       202|        6|     4|Sharp and bright          |
    # |       202|        7|     4|Great value               |
    # +----------+---------+------+--------------------------+


Best Practices
--------------
- Stream work from :py:meth:`eval` when possible. Yielding one ``pa.Table`` per Arrow batch keeps
  memory bounded and shortens feedback loops; reserve :py:meth:`terminate` for true per-partition
  operations.
- Keep per-partition state tiny and reset it promptly. If you only need the first *N* rows, raise
  :py:class:`~pyspark.sql.functions.SkipRestOfInputTableException` after collecting them so Spark
  skips the rest of the partition.
- Guard external calls with short timeouts and operate on the current batch instead of deferring to
  ``terminate``; this avoids giant buffers and keeps retries narrow.


When to use Arrow UDTFs vs Other UDTFs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- Prefer ``arrow_udtf`` when the logic is naturally vectorised, you can stay within Python, and the
  input/output schema is Arrow-friendly. You gain batch-friendly
  performance and native interoperability with PySpark DataFrames.
- Stick with the classic (row-based) Python UDTF when you only need simple per-row expansion, or when
  your logic depends on Python objects that Arrow cannot represent cleanly.
- Use SQL UDTFs if the functionality is performance critical and the logic can be represented in SQL.


More Examples
-------------

Example: Simple anomaly detection per device
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Compute simple per-device stats and return them from ``terminate``; this pattern is
useful for anomaly detection workflows that first summarize distributions by key.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator
    from pyspark.sql.functions import arrow_udtf

    @arrow_udtf(returnType="device_id int, count int, mean double, stddev double, max_value int")
    class DeviceStats:
        def __init__(self):
            self._device = None
            self._count = 0
            self._sum = 0.0
            self._sumsq = 0.0
            self._max = None

        def eval(self, batch: pa.RecordBatch) -> Iterator[pa.Table]:
            tbl = pa.table(batch)
            self._device = pc.unique(tbl["device_id"]).to_pylist()[0]
            vals = tbl["reading"].cast(pa.float64())
            self._count += len(vals)
            self._sum += pc.sum(vals).as_py() or 0.0
            self._sumsq += pc.sum(pc.multiply(vals, vals)).as_py() or 0.0
            cur_max = pc.max(vals).as_py()
            self._max = cur_max if self._max is None else max(self._max, cur_max)
            return iter(())

        def terminate(self) -> Iterator[pa.Table]:
            if self._device is not None and self._count > 0:
                mean = self._sum / self._count
                var = max(self._sumsq / self._count - mean * mean, 0.0)
                std = var ** 0.5
                # Round to 2 decimal places for display
                mean_rounded = round(mean, 2)
                std_rounded = round(std, 2)
                yield pa.table({
                    "device_id": pa.array([self._device], pa.int32()),
                    "count": pa.array([self._count], pa.int32()),
                    "mean": pa.array([mean_rounded], pa.float64()),
                    "stddev": pa.array([std_rounded], pa.float64()),
                    "max_value": pa.array([int(self._max)], pa.int32()),
                })

    spark.udtf.register("device_stats", DeviceStats)
    spark.createDataFrame(
        [(1, 10), (1, 12), (1, 100), (2, 5), (2, 7)],
        "device_id int, reading int",
    ).createOrReplaceTempView("readings")

    spark.sql(
        """
        SELECT *
        FROM device_stats(
          TABLE(readings)
          PARTITION BY device_id
        )
        ORDER BY device_id
        """
    ).show()

    # Result:
    # +---------+-----+-----+------+---------+
    # |device_id|count| mean|stddev|max_value|
    # +---------+-----+-----+------+---------+
    # |        1|    3|40.67| 41.96|      100|
    # |        2|    2|  6.0|   1.0|        7|
    # +---------+-----+-----+------+---------+


Example: Arrow UDTFs as RDD map-style transforms
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Arrow UDTFs can replace many ``RDD.map``/``flatMap``-style transforms with better
performance and first-class SQL integration. Instead of mapping row-by-row in Python,
you work on Arrow batches and return a table.

Example: tokenize text into words (flatMap-like)

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    from typing import Iterator
    from pyspark.sql.functions import arrow_udtf

    @arrow_udtf(returnType="doc_id int, word string")
    class Tokenize:
        def eval(self, batch: pa.RecordBatch) -> Iterator[pa.Table]:
            tbl = pa.table(batch)
            # Split on whitespace; build flat arrays for (doc_id, word)
            doc_ids: list[int] = []
            words: list[str] = []
            for doc_id, text in zip(tbl["doc_id"].to_pylist(), tbl["text"].to_pylist()):
                for w in (text or "").split():
                    doc_ids.append(doc_id)
                    words.append(w)
            if doc_ids:
                yield pa.table({"doc_id": pa.array(doc_ids, pa.int32()), "word": pa.array(words)})

    spark.udtf.register("tokenize", Tokenize)
    spark.createDataFrame([(1, "spark is fast"), (2, "arrow udtf")], "doc_id int, text string").createOrReplaceTempView("docs")
    spark.sql("SELECT * FROM tokenize(TABLE(docs)) ORDER BY doc_id, word").show()

    # Result:
    # +------+-----+
    # |doc_id| word|
    # +------+-----+
    # |     1| fast|
    # |     1|   is|
    # |     1|spark|
    # |     2|arrow|
    # |     2| udtf|
    # +------+-----+
