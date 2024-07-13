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

======================
Python Data Source API
======================

.. currentmodule:: pyspark.sql

Overview
--------
The Python Data Source API is a new feature introduced in Spark 4.0, enabling developers to read from custom data sources and write to custom data sinks in Python.
This guide provides a comprehensive overview of the API and instructions on how to create, use, and manage Python data sources.

Simple Example
--------------
Here's a simple Python data source that generates exactly two rows of synthetic data.
This example demonstrates how to set up a custom data source without using external libraries, focusing on the essentials needed to get it up and running quickly.

**Step 1: Define the data source**

.. code-block:: python

    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    class SimpleDataSource(DataSource):
        """
        A simple data source for PySpark that generates exactly two rows of synthetic data.
        """

        @classmethod
        def name(cls):
            return "simple"

        def schema(self):
            return StructType([
                StructField("name", StringType()),
                StructField("age", IntegerType())
            ])

        def reader(self, schema: StructType):
            return SimpleDataSourceReader()

    class SimpleDataSourceReader(DataSourceReader):

        def read(self, partition):
            yield ("Alice", 20)
            yield ("Bob", 30)

**Step 2: Register the data source**

.. code-block:: python

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    spark.dataSource.register(SimpleDataSource)

**Step 3: Read from the data source**

.. code-block:: python

    spark.read.format("simple").load().show()

    # +-----+---+
    # | name|age|
    # +-----+---+
    # |Alice| 20|
    # |  Bob| 30|
    # +-----+---+


Creating a Python Data Source
-----------------------------
To create a custom Python data source, you'll need to subclass the :class:`DataSource` base classes and implement the necessary methods for reading and writing data.

This example demonstrates creating a simple data source to generate synthetic data using the `faker` library. Ensure the `faker` library is installed and accessible in your Python environment.

**Define the Data Source**

Start by creating a new subclass of :class:`DataSource` with the source name, schema.

In order to be used as source or sink in batch or streaming query, corresponding method of DataSource needs to be implemented.

Method that needs to be implemented for a capability:

+------------+----------------------+------------------+
|            |       source         |      sink        |
+============+======================+==================+
| batch      | reader()             | writer()         |
+------------+----------------------+------------------+
|            | streamReader()       |                  |
| streaming  | or                   | streamWriter()   |
|            | simpleStreamReader() |                  |
+------------+----------------------+------------------+

.. code-block:: python

    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import StructType

    class FakeDataSource(DataSource):
        """
        A fake data source for PySpark to generate synthetic data using the `faker` library.
        Options:
        - numRows: specify number of rows to generate. Default value is 3.
        """

        @classmethod
        def name(cls):
            return "fake"

        def schema(self):
            return "name string, date string, zipcode string, state string"

        def reader(self, schema: StructType):
            return FakeDataSourceReader(schema, self.options)

        def writer(self, schema: StructType, overwrite: bool):
            return FakeDataSourceWriter(self.options)

        def streamReader(self, schema: StructType):
            return FakeStreamReader(schema, self.options)

        # Please skip the implementation of this method if streamReader has been implemented.
        def simpleStreamReader(self, schema: StructType):
            return SimpleStreamReader()

        def streamWriter(self, schema: StructType, overwrite: bool):
            return FakeStreamWriter(self.options)

Implementing Batch Reader and Writer for Python Data Source
-----------------------------------------------------------
**Implement the Reader**

Define the reader logic to generate synthetic data. Use the `faker` library to populate each field in the schema.

.. code-block:: python

    class FakeDataSourceReader(DataSourceReader):

        def __init__(self, schema, options):
            self.schema: StructType = schema
            self.options = options

        def read(self, partition):
            from faker import Faker
            fake = Faker()
            # Note: every value in this `self.options` dictionary is a string.
            num_rows = int(self.options.get("numRows", 3))
            for _ in range(num_rows):
                row = []
                for field in self.schema.fields:
                    value = getattr(fake, field.name)()
                    row.append(value)
                yield tuple(row)

**Implement the Writer**

Create a fake data source writer that processes each partition of data, counts the rows, and either
prints the total count of rows after a successful write or the number of failed tasks if the writing process fails.

.. code-block:: python

    from dataclasses import dataclass
    from typing import Iterator, List

    from pyspark.sql.types import Row
    from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage

    @dataclass
    class SimpleCommitMessage(WriterCommitMessage):
        partition_id: int
        count: int

    class FakeDataSourceWriter(DataSourceWriter):

        def write(self, rows: Iterator[Row]) -> SimpleCommitMessage:
            from pyspark import TaskContext

            context = TaskContext.get()
            partition_id = context.partitionId()
            cnt = sum(1 for _ in rows)
            return SimpleCommitMessage(partition_id=partition_id, count=cnt)

        def commit(self, messages: List[SimpleCommitMessage]) -> None:
            total_count = sum(message.count for message in messages)
            print(f"Total number of rows: {total_count}")

        def abort(self, messages: List[SimpleCommitMessage]) -> None:
            failed_count = sum(message is None for message in messages)
            print(f"Number of failed tasks: {failed_count}")


Implementing Streaming Reader and Writer for Python Data Source
---------------------------------------------------------------
**Implement the Stream Reader**

This is a dummy streaming data reader that generate 2 rows in every microbatch. The streamReader instance has a integer offset that increase by 2 in every microbatch.

.. code-block:: python

    class RangePartition(InputPartition):
        def __init__(self, start, end):
            self.start = start
            self.end = end

    class FakeStreamReader(DataSourceStreamReader):
        def __init__(self, schema, options):
            self.current = 0

        def initialOffset(self) -> dict:
            """
            Return the initial start offset of the reader.
            """
            return {"offset": 0}

        def latestOffset(self) -> dict:
            """
            Return the current latest offset that the next microbatch will read to.
            """
            self.current += 2
            return {"offset": self.current}

        def partitions(self, start: dict, end: dict):
            """
            Plans the partitioning of the current microbatch defined by start and end offset,
            it needs to return a sequence of :class:`InputPartition` object.
            """
            return [RangePartition(start["offset"], end["offset"])]

        def commit(self, end: dict):
            """
            This is invoked when the query has finished processing data before end offset, this can be used to clean up resource.
            """
            pass

        def read(self, partition) -> Iterator[Tuple]:
            """
            Takes a partition as an input and read an iterator of tuples from the data source.
            """
            start, end = partition.start, partition.end
            for i in range(start, end):
                yield (i, str(i))

**Implement the Simple Stream Reader**

If the data source has low throughput and doesn't require partitioning, you can implement SimpleDataSourceStreamReader instead of DataSourceStreamReader.

One of simpleStreamReader() and streamReader() must be implemented for readable streaming data source. And simpleStreamReader() will only be invoked when streamReader() is not implemented.

This is the same dummy streaming reader that generate 2 rows every batch implemented with SimpleDataSourceStreamReader interface.

.. code-block:: python

    class SimpleStreamReader(SimpleDataSourceStreamReader):
        def initialOffset(self):
            """
            Return the initial start offset of the reader.
            """
            return {"offset": 0}

        def read(self, start: dict) -> (Iterator[Tuple], dict):
            """
            Takes start offset as an input, return an iterator of tuples and the start offset of next read.
            """
            start_idx = start["offset"]
            it = iter([(i,) for i in range(start_idx, start_idx + 2)])
            return (it, {"offset": start_idx + 2})

        def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
            """
            Takes start and end offset as input and read an iterator of data deterministically.
            This is called whe query replay batches during restart or after failure.
            """
            start_idx = start["offset"]
            end_idx = end["offset"]
            return iter([(i,) for i in range(start_idx, end_idx)])

        def commit(self, end):
            """
            This is invoked when the query has finished processing data before end offset, this can be used to clean up resource.
            """
            pass

**Implement the Stream Writer**

This is a streaming data writer that write the metadata information of each microbatch to a local path.

.. code-block:: python

    class SimpleCommitMessage(WriterCommitMessage):
       partition_id: int
       count: int

    class FakeStreamWriter(DataSourceStreamWriter):
       def __init__(self, options):
           self.options = options
           self.path = self.options.get("path")
           assert self.path is not None

       def write(self, iterator):
           """
           Write the data and return the commit message of that partition
           """
           from pyspark import TaskContext
           context = TaskContext.get()
           partition_id = context.partitionId()
           cnt = 0
           for row in iterator:
               cnt += 1
           return SimpleCommitMessage(partition_id=partition_id, count=cnt)

       def commit(self, messages, batchId) -> None:
           """
           Receives a sequence of :class:`WriterCommitMessage` when all write tasks succeed and decides what to do with it.
           In this FakeStreamWriter, we write the metadata of the microbatch(number of rows and partitions) into a json file inside commit().
           """
           status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
           with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
               file.write(json.dumps(status) + "\n")

       def abort(self, messages, batchId) -> None:
           """
           Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some tasks fail and decides what to do with it.
           In this FakeStreamWriter, we write a failure message into a txt file inside abort().
           """
           with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
               file.write(f"failed in batch {batchId}")

Serialization Requirement
-------------------------
User defined DataSource, DataSourceReader, DataSourceWriter, DataSourceStreamReader and DataSourceStreamWriter and their methods must be able to be serialized by pickle.

For library that are used inside a method, it must be imported inside the method. For example, TaskContext must be imported inside the read() method in the code below.

.. code-block:: python

    def read(self, partition):
        from pyspark import TaskContext
        context = TaskContext.get()

Using a Python Data Source
--------------------------
**Use a Python Data Source in Batch Query**

After defining your data source, it must be registered before usage.

.. code-block:: python

    spark.dataSource.register(FakeDataSource)

**Read From a Python Data Source**

Read from the fake datasource with the default schema and options:

.. code-block:: python

    spark.read.format("fake").load().show()

    # +-----------+----------+-------+-------+
    # |       name|      date|zipcode|  state|
    # +-----------+----------+-------+-------+
    # |Carlos Cobb|2018-07-15|  73003|Indiana|
    # | Eric Scott|1991-08-22|  10085|  Idaho|
    # | Amy Martin|1988-10-28|  68076| Oregon|
    # +-----------+----------+-------+-------+

Read from the fake datasource with a custom schema:

.. code-block:: python

    spark.read.format("fake").schema("name string, company string").load().show()

    # +---------------------+--------------+
    # |name                 |company       |
    # +---------------------+--------------+
    # |Tanner Brennan       |Adams Group   |
    # |Leslie Maxwell       |Santiago Group|
    # |Mrs. Jacqueline Brown|Maynard Inc   |
    # +---------------------+--------------+

Read from the fake datasource with a different number of rows:

.. code-block:: python

    spark.read.format("fake").option("numRows", 5).load().show()

    # +--------------+----------+-------+------------+
    # |          name|      date|zipcode|       state|
    # +--------------+----------+-------+------------+
    # |  Pam Mitchell|1988-10-20|  23788|   Tennessee|
    # |Melissa Turner|1996-06-14|  30851|      Nevada|
    # |  Brian Ramsey|2021-08-21|  55277|  Washington|
    # |  Caitlin Reed|1983-06-22|  89813|Pennsylvania|
    # | Douglas James|2007-01-18|  46226|     Alabama|
    # +--------------+----------+-------+------------+

**Write To a Python Data Source**

To write data to a custom location, make sure that you specify the `mode()` clause. Supported modes are `append` and `overwrite`.

.. code-block:: python

    df = spark.range(0, 10, 1, 5)
    df.write.format("fake").mode("append").save()

    # You can check the Spark log (standard error) to see the output of the write operation.
    # Total number of rows: 10

**Use a Python Data Source in Streaming Query**

Once we register the python data source, we can also use it in streaming queries as source of readStream() or sink of writeStream() by passing short name or full name to format().

Start a query that read from fake python data source and write to console

.. code-block:: python

    query = spark.readStream.format("fake").load().writeStream.format("console").start()

    # +---+
    # | id|
    # +---+
    # |  0|
    # |  1|
    # +---+
    # +---+
    # | id|
    # +---+
    # |  2|
    # |  3|
    # +---+

We can also use the same data source in streaming reader and writer

.. code-block:: python

    query = spark.readStream.format("fake").load().writeStream.format("fake").start("/output_path")
