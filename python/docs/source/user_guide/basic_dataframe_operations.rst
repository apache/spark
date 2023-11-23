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

==========================
Basic DataFrame Operations
==========================

.. currentmodule:: pyspark.sql

Select
------

Projects a set of expressions and returns a new :class:`DataFrame`.
For more detailed usage methods, please refer to :meth:`DataFrame.select`.

Select all columns
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    >>> df.select('*').show()
    +---+-----+
    |age| name|
    +---+-----+
    |  2|Alice|
    |  5|  Bob|
    +---+-----+


Select by column names(string) or expressions (:class:`Column`)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    >>> df.select('name', (df.age + 10).alias('age')).show()
    +-----+---+
    | name|age|
    +-----+---+
    |Alice| 12|
    |  Bob| 15|
    +-----+---+


Filter
------

Filters rows using the given condition.
For more detailed usage methods, please refer to :meth:`DataFrame.filter`.

Filter by :class:`Column` instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(2, "Alice", "Math"), (5, "Bob", "Physics"),
    ...     (7, "Charlie", "Chemistry")], schema=["age", "name", "subject"])
    >>> df.filter(df.age > 3).show()
    +---+-------+---------+
    |age|   name|  subject|
    +---+-------+---------+
    |  5|    Bob|  Physics|
    |  7|Charlie|Chemistry|
    +---+-------+---------+


Filter by SQL expression in a string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(2, "Alice", "Math"), (5, "Bob", "Physics"), (7, "Charlie", "Chemistry")],
    ...     schema=["age", "name", "subject"])
    >>> df.filter("age > 3").show()
    +---+-------+---------+
    |age|   name|  subject|
    +---+-------+---------+
    |  5|    Bob|  Physics|
    |  7|Charlie|Chemistry|
    +---+-------+---------+


Collect
-------

Returns all the records in the DataFrame as a list of :class:`Row`.
For more detailed usage methods, please refer to :meth:`DataFrame.collect`.

.. code-block:: python

    >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    >>> df.collect()
    [Row(age=14, name='Tom'), Row(age=23, name='Alice'), Row(age=16, name='Bob')]


Show
----

Prints the first ``n`` rows of the DataFrame to the console.
For more detailed usage methods, please refer to :meth:`DataFrame.show`.

Show the :class:`DataFrame` in a tabular form
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Up to 20 rows, strings more than 20 characters will be truncated, and all cells will be aligned right.

.. code-block:: python

    >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob"),
    ...     (19, "This is a super long name")], ["age", "name"])
    >>> df.show()
    +---+--------------------+
    |age|                name|
    +---+--------------------+
    | 14|                 Tom|
    | 23|               Alice|
    | 16|                 Bob|
    | 19|This is a super l...|
    +---+--------------------+


Show only top 2 rows
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob"),
    ...     (19, "This is a super long name")], ["age", "name"])
    >>> df.show(2)
    +---+-----+
    |age| name|
    +---+-----+
    | 14|  Tom|
    | 23|Alice|
    +---+-----+


Show full column content without truncation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob"),
    ...     (19, "This is a super long name")], ["age", "name"])
    >>> df.show(truncate=False)
    +---+-------------------------+
    |age|name                     |
    +---+-------------------------+
    |14 |Tom                      |
    |23 |Alice                    |
    |16 |Bob                      |
    |19 |This is a super long name|
    +---+-------------------------+

