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

Spark 3.5 introduces Python user-defined table functions (UDTFs), a new type of user-defined function. 
Unlike scalar functions that return a single result value for every input, UDTFs is invoked in the ``FROM``
clause of a query and returns an entire relation as output.
Each UDTF call can accept zero or more arguments.
These arguments can either be scalar expressions or table arguments that represent entire input relations.

Implementing a Python UDTF
--------------------------

.. currentmodule:: pyspark.sql.functions

To implement a Python UDTF, you can define a class implementing the methods:

.. code-block:: python

    class PythonUDTF:

        def __init__(self) -> None:
            """
            Initialize the user-defined table function (UDTF).

            This method serves as the default constructor and is called once when the
            UDTF is instantiated on the executor side.
            
            Any class fields assigned in this method will be available for subsequent
            calls to the `eval` and `terminate` methods.

            Notes
            -----
            - This method does not accept any extra arguments.
            - You cannot create or reference the Spark session within the UDTF. Any
              attempt to do so will result in a serialization error.
            """
            ...

        def eval(self, *args: Any) -> Iterator[Any]:
            """
            Evaluate the function using the given input arguments.

            This method is required and must be implemented.

            Argument Mapping:
            - Each provided scalar expression maps to exactly one value in the
              `*args` list.
            - Each provided table argument maps to a pyspark.sql.Row object containing
              the columns in the order they appear in the provided input relation.

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
                A tuple representing a single row in the UDTF result relation.
                Yield as many times as needed to produce multiple rows.

            Notes
            -----
            - The result of the function must be a tuple representing a single row
              in the UDTF result relation.
            - UDTFs currently do not accept keyword arguments during the function call.

            Examples
            --------
            >>> def eval(self, x: int, y: int) -> Iterator[Any]:
            >>>     yield x + y, x - y
            >>>     yield y + x, y - x
            """
            ...

        def terminate(self) -> Iterator[Any]:
            """
            Called when the UDTF has processed all input rows.

            This method is optional to implement and is useful for performing any
            cleanup or finalization operations after the UDTF has finished processing
            all rows. It can also be used to yield additional rows if needed.

            Yields
            ------
            tuple
                A tuple representing a single row in the UDTF result relation.
                Yield this if you want to return additional rows during termination.

            Examples
            --------
            >>> def terminate(self) -> Iterator[Any]:
            >>>     yield "done", None
            """
            ...


The return type of the UDTF defines the schema of the table it outputs. 
It must be either a ``StructType`` or a DDL string representing a struct type.

**Example UDTF Implementation:**

Here is a simple example of a UDTF implementation:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 36-40
    :dedent: 4


**Instantiating the UDTF:**

To make use of the UDTF, you'll first need to instantiate it using the :func:`udtf` function:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 42-55
    :dedent: 4


**Using `udtf` as a Decorator:**

An alternative way for implementing a UDTF is to use :func:`udtf` as a decorator:

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
data between JVM and Python processes. Apache Arrow is disabled by default for Python UDTFs.

To enable Arrow optimization, set ``spark.sql.execution.pythonUDTF.arrow.enabled`` to ``true``.
Alternatively, you can specify the ``useArrow`` parameter when declaring the UDTF:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 121-126
    :dedent: 4


For more details, please see `Apache Arrow in PySpark <../arrow_pandas.rst>`_.


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
    :lines: 157-176
    :dedent: 4


Advanced Featuress
------------------

TABLE input argument
~~~~~~~~~~~~~~~~~~~~
Python UDTFs can also take a TABLE as input argument, and it can be used in conjunction 
with scalar input arguments.
By default, you are allowed to have only one TABLE argument as input, primarily for 
performance reasons. If you need to have more than one TABLE input argument, 
you can enable this by setting ``spark.sql.tvf.allowMultipleTableArguments.enabled`` to ``true``.

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 181-200
    :dedent: 4
