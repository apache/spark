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


Creating a Python Data Source
-----------------------------
To create a custom Python data source, you'll need to subclass the :class:`DataSource` base classes and implement the necessary methods for reading and writing data.

This example demonstrates creating a simple data source to generate synthetic data using the `faker` library. Ensure the `faker` library is installed and accessible in your Python environment.

** Define the Data Source**

Start by creating a new subclass of :class:`DataSource`. Define the source name, schema, and reader logic as follows:

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

        def streamReader(self, schema: StructType):
            return FakeStreamReader(schema, self.options)

        def streamWriter(self, schema: StructType, overwrite: bool):
            return FakeStreamWriter(self.options)


** Implement the Reader**

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

** Implement the Stream Reader**

.. code-block:: python

    class RangePartition(InputPartition):
        def __init__(self, start, end):
            self.start = start
            self.end = end

    class FakeStreamReader(DataSourceStreamReader):
        current = 0

        def initialOffset(self):
            return {"offset": 0}

        def latestOffset(self):
            self.current += 2
            return {"offset": self.current}

        def partitions(self, start, end):
            return [RangePartition(start["offset"], end["offset"])]

        def commit(self, end):
            pass

        def read(self, partition):
            start, end = partition.start, partition.end
            for i in range(start, end):
                yield (i, str(i))

This is a dummy streaming data reader that generate 2 rows in every microbatch. The streamReader instance has a integer offset that increase by 2 in every microbatch.

:meth:`pyspark.sql.datasource.DataSourceStreamReader.initialOffset` should return the initial start offset of the reader.

:meth:`pyspark.sql.datasource.DataSourceStreamReader.latestOffset` return the current latest offset that the next microbatch will read to.

:meth:`pyspark.sql.datasource.DataSourceStreamReader.partitions` plans the partitioning of the current microbatch defined by start and end offset, it needs to return a sequence of :class:`InputPartition` object.

:meth:`pyspark.sql.datasource.DataSourceStreamReader.read` takes a partition as an input and read an iterator of tuples from the data source.

:meth:`pyspark.sql.datasource.DataSourceStreamReader.commit` is invoked when the query has finished processing data before end offset, this can be used to clean up resource.

** Implement the Stream Writer**

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
           from pyspark import TaskContext
           context = TaskContext.get()
           partition_id = context.partitionId()
           cnt = 0
           for row in iterator:
               cnt += 1
           return SimpleCommitMessage(partition_id=partition_id, count=cnt)


       def commit(self, messages, batchId) -> None:
           status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))


           with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
               file.write(json.dumps(status) + "\n")


       def abort(self, messages, batchId) -> None:
           with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
               file.write(f"failed in batch {batchId}")

This is a streaming data writer that write the metadata information of each microbatch to a local path.

:meth:`pyspark.sql.datasource.DataSourceStreamWriter.write` should return the commit message of that partition after writing the data.

:meth:`pyspark.sql.datasource.DataSourceStreamWriter.commit` receives a sequence of :class:`WriterCommitMessage` when all write tasks succeed and decides what to do with it.
In this FakeStreamWriter, we write the metadata of the microbatch(number of rows and partitions) into a json file inside commit().

:meth:`pyspark.sql.datasource.DataSourceStreamWriter.abort` receives a sequence of :class:`WriterCommitMessage` from successful tasks when some tasks fail and decides what to do with it.
In this FakeStreamWriter, we write a failure message into a txt file inside abort().

Using a Python Data Source
--------------------------

After defining your data source, it must be registered before usage.

.. code-block:: python

    spark.dataSource.register(FakeDataSource)

Use the fake datasource with the default schema and options:

.. code-block:: python

    spark.read.format("fake").load().show()

    # +-----------+----------+-------+-------+
    # |       name|      date|zipcode|  state|
    # +-----------+----------+-------+-------+
    # |Carlos Cobb|2018-07-15|  73003|Indiana|
    # | Eric Scott|1991-08-22|  10085|  Idaho|
    # | Amy Martin|1988-10-28|  68076| Oregon|
    # +-----------+----------+-------+-------+

Use the fake datasource with a custom schema:

.. code-block:: python

    spark.read.format("fake").schema("name string, company string").load().show()

    # +---------------------+--------------+
    # |name                 |company       |
    # +---------------------+--------------+
    # |Tanner Brennan       |Adams Group   |
    # |Leslie Maxwell       |Santiago Group|
    # |Mrs. Jacqueline Brown|Maynard Inc   |
    # +---------------------+--------------+

Use the fake datasource with a different number of rows:

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

Start a streaming query with the fake data stream. Once we register the python data source, we can use it as source of readStream() or sink of writeStream() by passing short name or full name to format().

.. code-block:: python

    query = spark.readStream.format("fake").load().writeStream().format("fake").start()
