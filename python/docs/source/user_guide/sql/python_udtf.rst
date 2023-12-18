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

===========================================
Python User-defined Table Functions (UDTFs)
===========================================

Spark 3.5 introduces the Python user-defined table function (UDTF), a new type of user-defined function. 
Unlike scalar functions that return a single result value from each call, each UDTF is invoked in 
the ``FROM`` clause of a query and returns an entire table as output.
Each UDTF call can accept zero or more arguments.
These arguments can either be scalar expressions or table arguments that represent entire input tables.

Implementing a Python UDTF
--------------------------

.. currentmodule:: pyspark.sql.functions

To implement a Python UDTF, you first need to define a class implementing the methods:

.. code-block:: python

    class PythonUDTF:

        def __init__(self) -> None:
            """
            Initializes the user-defined table function (UDTF). This is optional.

            This method serves as the default constructor and is called once when the
            UDTF is instantiated on the executor side.
            
            Any class fields assigned in this method will be available for subsequent
            calls to the `eval` and `terminate` methods. This class instance will remain
            alive until all rows in the current partition have been consumed by the `eval`
            method.

            Notes
            -----
            - You cannot create or reference the Spark session within the UDTF. Any
              attempt to do so will result in a serialization error.
            - If the below `analyze` method is implemented, it is also possible to define this
              method as: `__init__(self, analyze_result: AnalyzeResult)`. In this case, the result
              of the `analyze` method is passed into all future instantiations of this UDTF class.
              In this way, the UDTF may inspect the schema and metadata of the output table as
              needed during execution of other methods in this class. Note that it is possible to
              create a subclass of the `AnalyzeResult` class if desired for purposes of passing
              custom information generated just once during UDTF analysis to other method calls;
              this can be especially useful if this initialization is expensive.
            """
            ...

        def analyze(self, *args: Any) -> AnalyzeResult:
            """
            Static method to compute the output schema of a particular call to this function in
            response to the arguments provided.

            This method is optional and only needed if the registration of the UDTF did not provide
            a static output schema to be use for all calls to the function. In this context,
            `output schema` refers to the ordered list of the names and types of the columns in the
            function's result table.

            This method accepts zero or more parameters mapping 1:1 with the arguments provided to
            the particular UDTF call under consideration. Each parameter is an instance of the
            `AnalyzeArgument` class, which contains fields including the provided argument's data
            type and value (in the case of literal scalar arguments only). For table arguments, the
            `isTable` field is set to true and the `dataType` field is a StructType representing
            the table's column types:

                dataType: DataType
                value: Optional[Any]
                isTable: bool

            This method returns an instance of the `AnalyzeResult` class which includes the result
            table's schema as a StructType. If the UDTF accepts an input table argument, then the
            `AnalyzeResult` can also include a requested way to partition the rows of the input
            table across several UDTF calls. If `withSinglePartition` is set to True, the query
            planner will arrange a repartitioning operation from the previous execution stage such
            that all rows of the input table are consumed by the `eval` method from exactly one
            instance of the UDTF class. On the other hand, if the `partitionBy` list is non-empty,
            the query planner will arrange a repartitioning such that all rows with each unique
            combination of values of the partitioning expressions are consumed by a separate unique
            instance of the UDTF class. If `orderBy` is non-empty, this specifies the requested
            ordering of rows within each partition.

                schema: StructType
                withSinglePartition: bool = False
                partitionBy: Sequence[PartitioningColumn] = field(default_factory=tuple)
                orderBy: Sequence[OrderingColumn] = field(default_factory=tuple)

            Notes
            -----
            - It is possible for the `analyze` method to accept the exact arguments expected,
              mapping 1:1 with the arguments provided to the UDTF call.
            - The `analyze` method can instead choose to accept positional arguments if desired
              (using `*args`) or keyword arguments (using `**kwargs`).

            Examples
            --------
            This is an `analyze` implementation that returns one output column for each word in the
            input string argument.

            >>> @staticmethod
            ... def analyze(text: str) -> AnalyzeResult:
            ...     schema = StructType()
            ...     for index, word in enumerate(text.split(" ")):
            ...         schema = schema.add(f"word_{index}")
            ...     return AnalyzeResult(schema=schema)

            Same as above, but using *args to accept the arguments.

            >>> @staticmethod
            ... def analyze(*args) -> AnalyzeResult:
            ...     assert len(args) == 1, "This function accepts one argument only"
            ...     assert args[0].dataType == StringType(), "Only string arguments are supported"
            ...     text = args[0]
            ...     schema = StructType()
            ...     for index, word in enumerate(text.split(" ")):
            ...         schema = schema.add(f"word_{index}")
            ...     return AnalyzeResult(schema=schema)

            Same as above, but using **kwargs to accept the arguments.

            >>> @staticmethod
            ... def analyze(**kwargs) -> AnalyzeResult:
            ...     assert len(kwargs) == 1, "This function accepts one argument only"
            ...     assert "text" in kwargs, "An argument named 'text' is required"
            ...     assert kwargs["text"].dataType == StringType(), "Only strings are supported"
            ...     text = args["text"]
            ...     schema = StructType()
            ...     for index, word in enumerate(text.split(" ")):
            ...         schema = schema.add(f"word_{index}")
            ...     return AnalyzeResult(schema=schema)

            An `analyze` implementation that returns a constant output schema, but add custom
            information in the result metadata to be consumed by future __init__ method calls:

            >>> @staticmethod
            ... def analyze(text: str) -> AnalyzeResult:
            ...     @dataclass
            ...     class AnalyzeResultWithOtherMetadata(AnalyzeResult):
            ...         num_words: int
            ...         num_articles: int
            ...     words = text.split(" ")
            ...     return AnalyzeResultWithOtherMetadata(
            ...         schema=StructType()
            ...             .add("word", StringType())
            ...             .add('total", IntegerType()),
            ...         num_words=len(words),
            ...         num_articles=len((
            ...             word for word in words
            ...             if word == 'a' or word == 'an' or word == 'the')))
            """
            ...

        def eval(self, *args: Any) -> Iterator[Any]:
            """
            Evaluates the function using the given input arguments.

            This method is required and must be implemented.

            Argument Mapping:
            - Each provided scalar expression maps to exactly one value in the
              `*args` list.
            - Each provided table argument maps to a pyspark.sql.Row object containing
              the columns in the order they appear in the provided input table,
              and with the names computed by the query analyzer.

            This method is called on every input row, and can produce zero or more
            output rows. Each element in the output tuple corresponds to one column
            specified in the return type of the UDTF.

            Parameters
            ----------
            *args : Any
                Arbitrary positional arguments representing the input to the UDTF.

            Yields
            ------
            tuple
                A tuple representing a single row in the UDTF result table.
                Yield as many times as needed to produce multiple rows.

            Notes
            -----
            - The result of the function must be a tuple representing a single row
              in the UDTF result table.
            - It is also possible for UDTFs to accept the exact arguments expected, along with
              their types.
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
            eval that returns one row and one column for each input.

            >>> def eval(self, x: int):
            ...     yield (x, )

            eval that returns two rows and two columns for each input.

            >>> def eval(self, x: int, y: int):
            ...     yield (x + y, x - y)
            ...     yield (y + x, y - x)

            Same as above, but using *args to accept the arguments:

            >>> def eval(self, *args):
            ...     assert len(args) == 2, "This function accepts two integer arguments only"
            ...     x = args[0]
            ...     y = args[1]
            ...     yield (x + y, x - y)
            ...     yield (y + x, y - x)

            Same as above, but using **kwargs to accept the arguments:

            >>> def eval(self, **kwargs):
            ...     assert len(kwargs) == 2, "This function accepts two integer arguments only"
            ...     x = kwargs["x"]
            ...     y = kwargs["y"]
            ...     yield (x + y, x - y)
            ...     yield (y + x, y - x)
            """
            ...

        def terminate(self) -> Iterator[Any]:
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
            tuple
                A tuple representing a single row in the UDTF result table.
                Yield this if you want to return additional rows during termination.

            Examples
            --------
            >>> def terminate(self) -> Iterator[Any]:
            >>>     yield "done", None
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


The return type of the UDTF defines the schema of the table it outputs. 
It must be either a ``StructType``, for example ``StructType().add("c1", StringType())``
or a DDL string representing a struct type, for example ``c1: string``.

**Example of UDTF Class Implementation**

Here is a simple example of a UDTF class implementation:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 36-40
    :dedent: 4


**Instantiating a UDTF with the ``udtf`` Decorator**

To make use of the UDTF, you'll first need to instantiate it using the ``@udtf`` decorator:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 42-55
    :dedent: 4


**Instantiating a UDTF with the ``udtf`` Function**

An alternative way to create a UDTF is to use the :func:`udtf` function:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 60-77
    :dedent: 4

For more detailed usage, please see :func:`udtf`.


Registering and Using Python UDTFs in SQL
-----------------------------------------

Python UDTFs can also be registered and used in SQL queries.

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 82-116
    :dedent: 4


Arrow Optimization
------------------
Apache Arrow is an in-memory columnar data format used in Spark to efficiently transfer
data between Java and Python processes. Apache Arrow is disabled by default for Python UDTFs.

Arrow can improve performance when each input row generates a large result table from the UDTF.

To enable Arrow optimization, set the ``spark.sql.execution.pythonUDTF.arrow.enabled``
configuration to ``true``. You can also enable it by specifying the ``useArrow`` parameter
when declaring the UDTF.

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 121-126
    :dedent: 4


For more details, please see `Apache Arrow in PySpark <../arrow_pandas.rst>`_.


TABLE input argument
~~~~~~~~~~~~~~~~~~~~
Python UDTFs can also take a TABLE as input argument, and it can be used in conjunction 
with scalar input arguments.
By default, you are allowed to have only one TABLE argument as input, primarily for 
performance reasons. If you need to have more than one TABLE input argument, 
you can enable this by setting the ``spark.sql.tvf.allowMultipleTableArguments.enabled``
configuration to ``true``.

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 191-210
    :dedent: 4


More Examples
-------------

A Python UDTF that expands date ranges into individual dates:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 131-152
    :dedent: 4


A Python UDTF with ``__init__`` and ``terminate``:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 157-186
    :dedent: 4
