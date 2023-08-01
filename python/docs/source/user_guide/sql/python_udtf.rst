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

Spark 3.5 introduces a new type of user-defined fucntion: Python user-defined table functions (UDTFs),
which take zero or more arguments and return a set of rows.

Implementing a Python UDTF
--------------------------

.. currentmodule:: pyspark.sql.functions

To implement a Python UDTF, you can implement this class:

.. code-block:: python

    class PythonUDTF:

        def __init__(self) -> None:
            """
            Initialize the user-defined table function (UDTF).

            This method is optional to implement and is called once when the UDTF is
            instantiated. Use it to perform any initialization required for the UDTF.
            """
            ...

        def eval(self, *args: Any) -> Iterator[Any]:
            """"
            Evaluate the function using the given input arguments.

            This method is required to implement.

            Args:
                *args: Arbitrary positional arguments representing the input
                       to the UDTF.

            Yields:
                tuple: A tuple representing a single row in the UDTF result relation.
                       Yield thisas many times as needed to produce multiple rows.

            Note:
                - The result must be a tuple.
                - UDTFs do not accept keyword arguments on the calling side.
                - Use "yield" to produce one row at a time for the UDTF result relation,
                  or use "return" to produce multiple rows for the UDTF result relation at once.

            Example:
                def eval(self, x: int, y: int):
                    yield x + y, x - y
            """
            ...

        def terminate(self) -> Iterator[Any]:
            """
            Called when the UDTF has processed all rows in a partition.

            This method is optional to implement and is useful for performing any
            cleanup or finalization operations after the UDTF has processed all rows.
            You can also yield additional rows if needed.

            Yields:
                tuple: A tuple representing a single row in the UDTF result relation.
                       Yield this if you want to return additional rows during termination.

            Example:
                def terminate(self):
                    yield "done", None
            """
            ...


The return type of the UDTF must be either a ``StructType`` or a DDL string, both of which 
define the schema of the UDTF output.

Here's a simple example of a UDTF implementation:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 39-53
    :dedent: 4


For more detailed usage, please see :func:`udtf`.


Registering and Using Python UDTFs in SQL
-----------------------------------------

Python UDTFs can also be registered and used in SQL queries.

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 58-84
    :dedent: 4


Apache Arrow
------------
Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer
data between JVM and Python processes. Apache Arrow is by default enabled for Python UDTFs.
You can set ``spark.sql.execution.pythonUDTF.arrow.enabled`` to ``false`` to disable Arrow optimization.

For more details, please see `Apache Arrow in PySpark <../arrow_pandas.rst>`_.


More Examples
-------------

A Python UDTF with `__init__` and `terminate`:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 89-109
    :dedent: 4


A Python UDTF to generate a list of dates:

.. literalinclude:: ../../../../../examples/src/main/python/sql/udtf.py
    :language: python
    :lines: 114-148
    :dedent: 4
