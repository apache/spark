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

Simple Example: Data Source with Batch Reader
---------------------------------------------
Here's a simple Python data source that generates exactly two rows of synthetic data.
This example demonstrates how to set up a custom data source without using external libraries, focusing on the essentials needed to get it up and running quickly.

**Step 1: Define the data source**

.. code-block:: python

    from typing import Iterator, Tuple

    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    class SimpleDataSource(DataSource):
        """
        A simple data source for PySpark that generates exactly two rows of synthetic data.
        """

        @classmethod
        def name(cls) -> str:
            return "simple"

        def schema(self) -> StructType:
            return StructType([
                StructField("name", StringType()),
                StructField("age", IntegerType())
            ])

        def reader(self, schema: StructType) -> DataSourceReader:
            return SimpleDataSourceReader()

    class SimpleDataSourceReader(DataSourceReader):

        def read(self, partition: InputPartition) -> Iterator[Tuple]:
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


Comprehensive Example: Data Source with Batch and Streaming Readers and Writers
-------------------------------------------------------------------------------
To create a custom Python data source, you'll need to subclass the :class:`DataSource` base class and implement the necessary methods for reading and writing data.

This example demonstrates creating a simple data source to generate synthetic data using the `faker` library. Ensure the `faker` library is installed and accessible in your Python environment.

Define the Data Source
~~~~~~~~~~~~~~~~~~~~~~

Start by creating a new subclass of :class:`DataSource` with the source name and schema.

In order to be used as source or sink in batch or streaming query, corresponding methods of DataSource needs to be implemented.

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

    from typing import Union

    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        DataSourceStreamReader,
        DataSourceStreamWriter,
        DataSourceWriter
    )
    from pyspark.sql.types import StructType

    class FakeDataSource(DataSource):
        """
        A fake data source for PySpark to generate synthetic data using the `faker` library.
        Options:
        - numRows: specify number of rows to generate. Default value is 3.
        """

        @classmethod
        def name(cls) -> str:
            return "fake"

        def schema(self) -> Union[StructType, str]:
            return "name string, date string, zipcode string, state string"

        def reader(self, schema: StructType) -> DataSourceReader:
            return FakeDataSourceReader(schema, self.options)

        def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
            return FakeDataSourceWriter(self.options)

        def streamReader(self, schema: StructType) -> DataSourceStreamReader:
            return FakeStreamReader(schema, self.options)

        def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
            return FakeStreamWriter(self.options)

Implement a Batch Reader
~~~~~~~~~~~~~~~~~~~~~~~~

Define the reader logic to generate synthetic data. Use the `faker` library to populate each field in the schema.

.. code-block:: python

    from typing import Dict

    class FakeDataSourceReader(DataSourceReader):

        def __init__(self, schema: StructType, options: Dict[str, str]):
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

Push Down a Limit to a Batch Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a query only needs the first few rows, a reader can implement ``pushLimit`` to fetch less
data, for example by adding a page size parameter to a REST request or a ``LIMIT`` clause to a
SQL query. ``pushLimit`` is called during planning, before ``partitions`` and ``read``, and
returns whether the reader will make use of the limit. It runs after ``pushFilters`` when the
query has filters to push down, so state it depends on belongs in ``__init__``.

Pushing down a limit is only a hint: Spark always applies the limit again after the scan, so a
reader is free to return more rows than requested. Set
``spark.sql.python.limitPushdown.enabled`` to ``true`` to enable limit pushdown.

.. code-block:: python

    from typing import Dict

    from pyspark.sql.datasource import DataSourceReader, InputPartition
    from pyspark.sql.types import StructType

    class FakeDataSourceReader(DataSourceReader):

        def __init__(self, schema: StructType, options: Dict[str, str]):
            self.schema: StructType = schema
            self.options = options
            self.limit = None

        def pushLimit(self, limit: int) -> bool:
            self.limit = limit
            return True

        def partitions(self):
            # A limit makes a single request cheaper than a fan-out, since every
            # partition opens its own connection to the data source.
            if self.limit is not None:
                return [InputPartition(None)]
            return [InputPartition(i) for i in range(16)]

        def read(self, partition):
            num_rows = int(self.options.get("numRows", 3))
            if self.limit is not None:
                num_rows = min(num_rows, self.limit)
            ...

Implement a Batch Writer
~~~~~~~~~~~~~~~~~~~~~~~~

Create a fake data source writer that processes each partition of data, counts the rows, and either prints the total count of rows after a successful write or the number of failed tasks if the writing process fails.

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


Implement a Streaming Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a dummy streaming data reader that generates 2 rows in every microbatch. The streamReader instance has an integer offset that increases by 2 in every microbatch.

.. code-block:: python

    class RangePartition(InputPartition):
        def __init__(self, start: int, end: int):
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

        def partitions(self, start: dict, end: dict) -> list[InputPartition]:
            """
            Plans the partitioning of the current microbatch defined by start
            and end offset, it needs to return a sequence of
            :class:`InputPartition` object.
            """
            return [RangePartition(start["offset"], end["offset"])]

        def commit(self, end: dict) -> None:
            """
            This is invoked when the query has finished processing data before end
            offset, this can be used to clean up resource.
            """
            pass

        def read(self, partition) -> Iterator[Tuple]:
            """
            Takes a partition as an input and read an iterator of tuples from the
            data source.
            """
            start, end = partition.start, partition.end
            for i in range(start, end):
                yield (i, str(i))

Alternative: Implement a Simple Streaming Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the data source has low throughput and doesn't require partitioning, you can implement SimpleDataSourceStreamReader instead of DataSourceStreamReader.

One of simpleStreamReader() and streamReader() must be implemented for a readable streaming data source. And simpleStreamReader() will only be invoked when streamReader() is not implemented.

.. code-block:: python

    from pyspark.sql.datasource import SimpleDataSourceStreamReader


    class FakeDataSource(DataSource):
        ...

        def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
            return FakeSimpleStreamReader()

        # omit implementation of streamReader

        ...

This is the same dummy streaming reader that generates 2 rows every batch implemented with SimpleDataSourceStreamReader interface.

.. code-block:: python

    from typing import Iterator, Tuple

    from pyspark.sql.datasource import SimpleDataSourceStreamReader

    class FakeSimpleStreamReader(SimpleDataSourceStreamReader):
        def initialOffset(self) -> dict:
            """
            Return the initial start offset of the reader.
            """
            return {"offset": 0}

        def read(self, start: dict) -> Tuple[Iterator[Tuple], dict]:
            """
            Takes start offset as an input, return an iterator of tuples and
            the end offset (start offset for the next read). The end offset must
            advance past the start offset when returning data; otherwise Spark
            raises a validation exception.
            For example, returning 2 records from start_idx 0 means end should
            be {"offset": 2} (i.e. start + 2).
            When there is no data to read, you may return the same offset as end and
            start, but you must provide an empty iterator.
            """
            start_idx = start["offset"]
            it = iter([(i,) for i in range(start_idx, start_idx + 2)])
            return (it, {"offset": start_idx + 2})

        def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
            """
            Takes start and end offset as input and read an iterator of data
            deterministically.
            This is called whe query replay batches during restart or after failure.
            """
            start_idx = start["offset"]
            end_idx = end["offset"]
            return iter([(i,) for i in range(start_idx, end_idx)])

        def commit(self, end: dict) -> None:
            """
            This is invoked when the query has finished processing data before end
            offset, this can be used to clean up resource.
            """
            pass

Admission Control for Streaming Readers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To limit the amount of data processed per micro-batch, implement
``getDefaultReadLimit()`` and have ``latestOffset(start, limit)`` honor the
engine-provided ``ReadLimit``:

.. code-block:: python

    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadLimit, ReadMaxRows

    class MyStreamReader(DataSourceStreamReader):

        def getDefaultReadLimit(self) -> ReadLimit:
            """
            Limit each micro-batch to at most 20 rows.

            This value is just an example; in practice, configure the limit
            based on source options (e.g., self.options.get("maxRowsPerBatch")).
            """
            return ReadMaxRows(20)

        def latestOffset(self, start: dict, limit: ReadLimit) -> dict:
            """
            Return the latest offset, respecting the provided limit.
            """
            current = start["offset"]

            if isinstance(limit, ReadMaxRows):
                end = min(current + limit.max_rows, self.max_available)
            elif isinstance(limit, ReadAllAvailable):
                end = self.max_available
            else:
                raise ValueError(f"Unexpected ReadLimit type: {type(limit)}")

            return {"offset": end}

When Spark uses the default ``ReadMaxRows(20)`` limit, each micro-batch
reads at most 20 rows, depending on the available rows. If Spark passes
``ReadAllAvailable``, the reader should return all remaining rows instead.

This is useful for:

- **Controlling data ingestion rate**: Prevent overwhelming downstream systems
- **Memory management**: Limit batch sizes to avoid out-of-memory errors
- **Backpressure handling**: Process data at a sustainable rate

For a complete working example, see:
``examples/src/main/python/sql/streaming/structured_blockchain_admission_control.py``

Implement a Streaming Writer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a streaming data writer that write the metadata information of each microbatch to a local path.

.. code-block:: python

    from typing import Iterator, List, Optional

    from pyspark.sql import Row
    from pyspark.sql.datasource import DataSourceStreamWriter, WriterCommitMessage


    class SimpleCommitMessage(WriterCommitMessage):
       partition_id: int
       count: int

    class FakeStreamWriter(DataSourceStreamWriter):
       def __init__(self, options):
           self.options = options
           self.path = self.options.get("path")
           assert self.path is not None

       def write(self, iterator: Iterator[Row]) -> WriterCommitMessage:
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

       def commit(self, messages: List[Optional[SimpleCommitMessage]], batchId: int) -> None:
           """
           Receives a sequence of :class:`SimpleCommitMessage` when all write tasks
           succeed and decides what to do with it.
           In this FakeStreamWriter, we write the metadata of the microbatch
           (number of rows and partitions) into a json file inside commit().
           """
           status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
           with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
               file.write(json.dumps(status) + "\n")

       def abort(self, messages: List[Optional[SimpleCommitMessage]], batchId: int) -> None:
           """
           Receives a sequence of :class:`SimpleCommitMessage` from successful tasks
           when some tasks fail and decides what to do with it.
           In this FakeStreamWriter, we write a failure message into a txt file inside abort().
           """
           with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
               file.write(f"failed in batch {batchId}")

Lookup Joins
------------

.. currentmodule:: pyspark.sql.datasource

A lookup join enriches a stream with data that an external store serves by key, such as a
database, a key-value store or a web service. For each batch of stream rows, Spark sends the join
keys to the data source and joins the rows that it returns. Spark never reads the whole source.

Use a lookup join for the static side of a stream-static join when that side is too large or
changes too often to read in full for each micro-batch, and the store answers queries by key
quickly. Spark plans a lookup join only for the static side of a stream-static join, in
micro-batch queries and, for now only when it is turned on, in real-time mode (see below). It
supports inner, left outer, left semi and left anti joins, with the stream on the left and the
lookup side on the right. A join of another shape over a lookup side fails rather than falling
back to a batch read.

To serve lookups, a data source implements :meth:`DataSource.lookupReader`, which returns a
:class:`DataSourceLookupReader`. Spark finds lookup support by looking at the data source class;
it does not create an instance to check. There are two modes:

- **Lookup-only**: the data source implements ``lookupReader()`` but not ``reader()``. Spark
  reads it by lookups in every supported stream-static join, with no hint, and rejects a batch
  read of it.
- **Hint mode**: the data source implements both. Spark reads it by batch reads, and by lookups
  only in a join whose lookup side carries the ``EXTERNAL_LOOKUP`` hint:

  .. code-block:: python

      offers = spark.read.format("offers").load().hint("EXTERNAL_LOOKUP")
      joined = events.join(offers, offers.user_id == events.user_id, "left")

  In SQL, name the lookup relation in the hint: ``SELECT /*+ EXTERNAL_LOOKUP(o) */ ...``.

Walk-through: A SQLite Lookup Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The example ``examples/src/main/python/sql/streaming/structured_python_lookup_join.py`` joins a
stream of events ``(user_id, segment, day)`` with offers stored in SQLite:

.. code-block:: python

    events.join(
        offers,
        (offers.user_id == events.user_id)
        & offers.segment.eqNullSafe(events.segment)
        & (offers.valid_from <= events.day)
        & (events.day < offers.valid_until),
        "left",
    )

where ``offers`` carries the hint and the filter ``country = 'KR'``.

.. warning::
    The example is a demo, not a pattern for a connector. SQLite is a file, not a service: the
    driver ships a read-only copy of the file to every executor, and each executor reads its own
    copy, so changes to the source file after the query starts are not seen. A real lookup
    connector calls a network service from ``lookup()``, such as Postgres, Redis or an HTTP API.

Spark plans the lookup on the driver. It calls the planning methods of the reader in this order,
and the reader may keep what it learns on ``self``:

1. :meth:`DataSourceLookupReader.pushFilters` receives the filters on lookup-side columns, as
   :meth:`DataSourceReader.pushFilters` does, and returns the filters that it does not serve.
   Spark calls it only when ``spark.sql.python.filterPushdown.enabled`` is true, and fails the
   query if the reader overrides it while the configuration is false. Unlike a batch read, Spark
   checks every filter again on the returned rows, also the pushed ones, so a pushed filter only
   lets the reader omit rows. The example serves ``country = 'KR'`` in its SQL:

   .. code-block:: python

       def pushFilters(self, filters):
           for f in filters:
               is_country = isinstance(f, EqualTo) and f.attribute == ("country",)
               if is_country and isinstance(f.value, str):
                   self.country = f.value
               else:
                   yield f

2. :meth:`DataSourceLookupReader.selectLookupPredicates` receives the candidate predicates
   that Spark found in the join condition and the lookup-side filters. Each candidate compares a
   lookup column with a :class:`LookupParameter`, a value that Spark binds for each
   lookup, such as ``EqualTo(("user_id",), LookupParameter(...))``. The reader returns the
   positions of the candidates that it serves, in the order of the fields of every parameter
   row. Spark evaluates the candidates that are not selected on the returned rows. The example
   selects ``user_id = ?``, ``segment <=> ?`` and, with the option ``select_range``, the range
   ``valid_from <= ?``. It leaves ``valid_until > ?`` and the constant ``country = ?`` to Spark:

   .. code-block:: python

       def selectLookupPredicates(self, candidates):
           def find(cls, column):
               return next(
                   (
                       i
                       for i, c in enumerate(candidates)
                       if type(c) is cls and c.attribute == (column,)
                   ),
                   None,
               )

           chosen = [find(EqualTo, "user_id"), find(EqualNullSafe, "segment")]
           if self.select_range:
               chosen.append(find(LessThanOrEqual, "valid_from"))
           chosen = [i for i in chosen if i is not None]
           if not chosen or candidates[chosen[0]].attribute != ("user_id",):
               return []  # no access path fits: the query fails
           self.on = [
               (candidates[i].attribute[0], SQL_OPS[type(candidates[i])]) for i in chosen
           ]
           return chosen

   Selecting a range whose value reads a stream column makes every request hold one parameter
   row, and turns off Spark's lookup cache for the join. Leave such a range unselected when the
   equalities alone serve the lookup well.

3. :meth:`DataSourceLookupReader.maxParameterRowsPerRequest` returns the largest number of
   parameter rows that one request may hold. The default is 1000, Spark's own cap.

   .. warning::
       If your backend limits the number of values in one call, such as the number of ``?``
       markers in a SQL statement or the keys of one multi-get, override
       ``maxParameterRowsPerRequest()`` and return a number that fits, counting every selected
       predicate. A request with more values than the backend accepts fails the task.

   The example fits SQLite's limit of 999 markers, the default before SQLite 3.32. Each
   parameter row takes one marker per selected predicate and one for its ordinal, and the
   pushed filter takes one more:

   .. code-block:: python

       def maxParameterRowsPerRequest(self):
           fixed = 1 if self.country is not None else 0
           return (SQLITE_MAX_MARKERS - fixed) // (len(self.on) + 1)

Spark then pickles the reader. On the executors, it calls :meth:`DataSourceLookupReader.lookup`
once per request, on a copy of the reader per input partition, and
:meth:`DataSourceLookupReader.close` after the last request. ``lookup`` receives a list of
tuples, one per distinct parameter row and one value per selected predicate. A ``TIMESTAMP``
value arrives as a naive ``datetime`` in the worker's local time, so two instants in the hour
that a daylight-saving change repeats can make two tuples equal: do not rely on distinct tuples
when a selected parameter is a ``TIMESTAMP``. Only the fields of ``<=>`` predicates can be
``None``, and a ``None`` there matches a stored NULL. The example sends one SQL statement per
request, not one per key: the parameter rows become a ``VALUES`` table with their ordinals,
joined to the stored table. It yields one ``(ordinal, row)`` pair per result row, where the
ordinal is the position of the matched parameter row in ``parameters``:

.. code-block:: python

    def lookup(self, parameters):
        for row in self._query(parameters):  # SELECT p.ord, o.user_id, ... FROM p JOIN offers o
            yield row[0], row[1:]

    def close(self):
        if self.conn is not None:
            self.conn.close()

An ordinal may appear several times, once per matching stored row, and an ordinal that does not
appear is a miss. Spark checks the selected predicates and the lookup-side filters again on each
returned row, so extra rows are harmless, but Spark cannot recover a missing row.

**Shipping files to the executors**: the example opens its connection lazily, in the first
``lookup`` call, on the executor. The planning methods run on the driver, so a connection that
they open would have to be pickled. The example ships the SQLite file with
``spark.sparkContext.addFile(path)`` (or ``spark-submit --files``), and with
``spark.addArtifacts(path, file=True)`` in Spark Connect. It keeps only the file name in the
reader, and finds the executor's copy with ``SparkFiles.get(name)``:

.. code-block:: python

    def connect(file_name):
        path = SparkFiles.get(file_name)
        return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)

Run the example with ``bin/spark-submit`` and it prints:

.. code-block:: text

    +-------+-------+---+----------+
    |user_id|segment|day|     offer|
    +-------+-------+---+----------+
    |      1|   gold|  2|    gold-1|   valid_from 5 > day 2: only one match
    |      1|   gold|  6|    gold-1|   several matches for one parameter row
    |      1|   gold|  6|    gold-5|
    |      2|   NULL|  6|no-segment|   NULL segment matched through <=>
    |      3|   gold|  6|      NULL|   miss: the US offer fails country = 'KR'
    |      4|   gold|  6|      NULL|   miss: valid only from day 9
    |      5|   gold|  6|      NULL|   miss: unknown user
    +-------+-------+---+----------+

Arrow Results
~~~~~~~~~~~~~
As ``read()`` can, ``lookup()`` can yield PyArrow ``RecordBatch`` objects instead of pairs. There
is no separate reader class: the first value that ``lookup`` yields decides the form for the
whole call. Parameters are always a list of tuples. In each batch:

- Column 0 is the ordinal of each row. It can be of any integer type, and cannot hold nulls.
- The other columns are those of the read schema, in order, with their names and exactly the
  Arrow types that a batch of ``read()`` must have. For example, a ``STRING`` column must be
  ``string``, not ``large_string``, and a ``TIMESTAMP`` column ``timestamp[us, tz=UTC]``.

Spark checks every batch, not only the first. The example yields batches with the option
``arrow``:

.. code-block:: python

    import pyarrow as pa

    names = ["ordinal"] + COLUMNS
    types = [pa.int32(), pa.int64(), pa.string(), pa.int64(), pa.int64()]
    types += [pa.string(), pa.string()]
    while rows := cursor.fetchmany(1000):
        columns = zip(*rows)
        arrays = [pa.array(values, t) for values, t in zip(columns, types)]
        yield pa.RecordBatch.from_arrays(arrays, names=names)

The Lookup Contract
~~~~~~~~~~~~~~~~~~~
Check a lookup reader against this list:

- The reader stays picklable. Create connections and other resources lazily, in the first
  ``lookup`` call, not in ``__init__`` or in a planning method.
- ``selectLookupPredicates`` returns distinct positions in the list of candidates, and leaves
  that list unchanged. Do not select a predicate that the backend cannot evaluate exactly as
  Spark does.
- ``maxParameterRowsPerRequest`` returns a positive ``int`` that fits the limits of the backend.
- For each parameter row, ``lookup`` returns every stored row that passes the pushed filters and
  satisfies all selected predicates, with no cap on the number of rows. A stored row that matches
  several parameter rows is returned once per ordinal.
- Every returned row carries the stored values of the columns that the selected predicates read.
- The result of a lookup depends only on its parameter row: Spark may reuse it for an equal
  parameter row, and may send a request again when it retries a task.
- ``lookup`` raises an exception when it cannot answer. Spark never takes a failure as a miss.
- Spark may stop reading the result early, for example when a semi join has decided every stream
  row. It then closes the generator, so ``try``/``finally`` and ``with`` blocks run.
- ``close()`` returns within ``spark.python.task.killTimeout``. Do not rely on it for
  correctness: when a task is killed, or after ``lookup`` raises an exception, Spark ends the
  Python worker without calling it.
- Do not rely on how often the planning methods run. As for a batch read of a Python data
  source, Spark plans the lookup once when a streaming query starts and reuses the plan while
  ``spark.sql.python.filterPushdown.enabled`` and ``spark.sql.python.limitPushdown.enabled`` are
  false, the default. Otherwise, it plans again in every micro-batch.

Caching and Real-Time Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~
Spark caches the complete result of each key, also a miss, within a task, per input partition,
so each micro-batch starts with an empty cache. The configurations
``spark.sql.join.externalLookup.cache.enabled``, ``.cache.maxBytes`` and ``.cache.ttl`` set it
up, and the hint overrides them for one join with options of the form ``key=value``:

.. code-block:: python

    offers.hint("EXTERNAL_LOOKUP", "cache_enabled=false")

A join whose reader selects a range on a stream value caches nothing.

In real-time mode, Spark sends each stream row's request as soon as it reads the row, by
default, so ``lookup`` usually receives one parameter row, and the cache serves keys that
repeat. ``spark.sql.join.externalLookup.realTimeMode.maxBatchRows`` sets a larger request size.
A real-time task runs for the whole batch, and one Python worker serves all its requests.
For now, real-time mode runs lookup joins with Python data sources only when the internal
configuration ``spark.sql.python.externalLookup.realTimeMode.enabled`` is true; otherwise, the
query fails with ``EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.PYTHON_REAL_TIME_MODE``.

Errors
~~~~~~
- A mistake in the planning results stops a streaming query when it starts, before its first
  micro-batch, also without data. Spark raises the subclasses of
  ``DATA_SOURCE_INVALID_EXTERNAL_LOOKUP`` for a broken contract, such as a selected position
  that is not one of a candidate (``INVALID_SELECTED_PREDICATE_POSITION``) or that repeats
  (``DUPLICATE_SELECTED_PREDICATE``), a list of candidates that ``selectLookupPredicates``
  changed (``MODIFIED_CANDIDATES``), ``None`` (``NULL_RESULT``) or a value of the wrong type
  (``INVALID_RESULT_TYPE``) from a planning method, or a non-positive
  ``maxParameterRowsPerRequest`` (``NON_POSITIVE_MAX_PARAMETER_ROWS``). It raises
  ``EXTERNAL_LOOKUP_JOIN_UNSUPPORTED`` for a join that cannot be a lookup join, for example when
  the reader selects no predicate (``NO_LOOKUP_PREDICATE``) or the join type is not supported.
- An exception that the reader raises while Spark plans the lookup fails the first micro-batch.
- An exception that ``lookup`` raises fails the task, and Spark retries it as any task.
- Results that break the contract fail the task: an ordinal outside the parameter rows
  (``DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_INPUT_ORDINAL``), a NULL in a field that the
  schema declares ``NOT NULL`` (``...NULL_IN_NOT_NULL_FIELD``), a value that is not a pair or a
  wrong ordinal (``DATA_SOURCE_INVALID_RETURN_TYPE``), or a batch that does not match the schema
  (``DATA_SOURCE_RETURN_SCHEMA_MISMATCH``).
- An exception that ``close()`` raises fails the task, and so does a ``close()`` that does not
  return within ``spark.python.task.killTimeout`` (``PYTHON_DATA_SOURCE_ERROR``). When the task
  has failed already, its failure stays the task's error, and the failure of ``close()`` is
  added to it as suppressed.

.. currentmodule:: pyspark.sql

Serialization Requirement
-------------------------
User defined DataSource, DataSourceReader, DataSourceWriter, DataSourceStreamReader, DataSourceStreamWriter and DataSourceLookupReader and their methods must be able to be serialized by pickle.

For libraries that are used inside a method, they must be imported inside the method. For example, TaskContext must be imported inside the read() method in the code below.

.. code-block:: python

    def read(self, partition):
        from pyspark import TaskContext
        context = TaskContext.get()

Using a Python Data Source
--------------------------
**Register a Python Data Source**

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

Python Data Source Reader with direct Arrow Batch support for improved performance
----------------------------------------------------------------------------------
The Python Datasource Reader supports direct yielding of Arrow Batches, which can significantly improve data processing performance. By using the efficient Arrow format,
this feature avoids the overhead of traditional row-by-row data processing, resulting in performance improvements of up to one order of magnitude, especially with large datasets.

**Enabling Arrow Batch Support**:
To enable this feature, configure your custom DataSource to yield Arrow batches by returning `pyarrow.RecordBatch` objects within the `read` method of your `DataSourceReader`
(or `DataSourceStreamReader`) implementation. This method simplifies data handling and reduces the number of I/O operations, particularly beneficial for large-scale data processing tasks.

**Arrow Batch Example**:
The following example demonstrates how to implement a basic Data Source using Arrow Batch support.

.. code-block:: python

    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
    from pyspark.sql import SparkSession
    import pyarrow as pa

    # Define the ArrowBatchDataSource
    class ArrowBatchDataSource(DataSource):
        """
        A Data Source for testing Arrow Batch Serialization
        """

        @classmethod
        def name(cls):
            return "arrowbatch"

        def schema(self):
            return "key int, value string"

        def reader(self, schema: str):
            return ArrowBatchDataSourceReader(schema, self.options)

    # Define the ArrowBatchDataSourceReader
    class ArrowBatchDataSourceReader(DataSourceReader):
        def __init__(self, schema, options):
            self.schema: str = schema
            self.options = options

        def read(self, partition):
            # Create Arrow Record Batch
            keys = pa.array([1, 2, 3, 4, 5], type=pa.int32())
            values = pa.array(["one", "two", "three", "four", "five"], type=pa.string())
            schema = pa.schema([("key", pa.int32()), ("value", pa.string())])
            record_batch = pa.RecordBatch.from_arrays([keys, values], schema=schema)
            yield record_batch

        def partitions(self):
            # Define the number of partitions
            num_part = 1
            return [InputPartition(i) for i in range(num_part)]

    # Initialize the Spark Session
    spark = SparkSession.builder.appName("ArrowBatchExample").getOrCreate()

    # Register the ArrowBatchDataSource
    spark.dataSource.register(ArrowBatchDataSource)

    # Load data using the custom data source
    df = spark.read.format("arrowbatch").load()

    df.show()

Usage Notes
-----------

- During Data Source resolution, built-in and Scala/Java Data Sources take precedence over Python Data Sources with the same name; to explicitly use a Python Data Source, make sure its name does not conflict with the other non-Python Data Sources.
- It is allowed to register multiple Python Data Sources with the same name. Later registrations will overwrite earlier ones.
- To automatically register a data source, export it as ``DefaultSource`` in a top level module with name prefix ``pyspark_``. See `pyspark_huggingface <https://github.com/huggingface/pyspark_huggingface>`_ for an example.
