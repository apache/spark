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
Unlike the traditional Python UDTF that evaluates row by row, the Vectorized Python UDTF let you directly operate on top of Apache Arrow arrays and columnm batches.
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

            This method is called on every batch of input row, and can produce zero or more
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
            This `eval` method that takes table argument and returns an arrow record batch for each input batch.

            >>> def eval(self, batch: pa.RecordBatch):
            ...     yield batch

            This `eval` method that takes table argument and returns a pyarrow table for each input batch.

            >>> def eval(self, batch: pa.RecordBatch):
            ...     yield pa.table({"x": batch.column(0), "y": batch.column(1)})

            This `eval` method that takes both table and scalar arguments and returns a pyarrow table for each input batch.

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

    # Register the UDTF
    spark.udtf.register("my_arrow_udtf", MyArrowPythonUDTF)

    # Use in SQL queries
    df = spark.sql("""
        SELECT * FROM my_arrow_udtf(TABLE(SELECT id, cast(id as string) as value FROM range(10)))
    """)
