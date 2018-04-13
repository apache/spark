#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import random

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from functools import reduce
else:
    from itertools import imap as map

import warnings

from pyspark import copy_func, since
from pyspark.rdd import RDD, _load_from_socket, ignore_unicode_prefix
from pyspark.serializers import BatchedSerializer, PickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import *

__all__ = ["DataFrame", "DataFrameNaFunctions", "DataFrameStatFunctions"]


class DataFrame(object):
    """A distributed collection of data grouped into named columns.

    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SQLContext`::

        people = sqlContext.read.parquet("...")

    Once created, it can be manipulated using the various domain-specific-language
    (DSL) functions defined in: :class:`DataFrame`, :class:`Column`.

    To select a column from the data frame, use the apply method::

        ageCol = people.age

    A more concrete example::

        # To create DataFrame using SQLContext
        people = sqlContext.read.parquet("...")
        department = sqlContext.read.parquet("...")

        people.filter(people.age > 30).join(department, people.deptId == department.id) \\
          .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})

    .. versionadded:: 1.3
    """

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc
        self.is_cached = False
        self._schema = None  # initialized lazily
        self._lazy_rdd = None

    @property
    @since(1.3)
    def rdd(self):
        """Returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        """
        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            self._lazy_rdd = RDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()))
        return self._lazy_rdd

    @property
    @since("1.3.1")
    def na(self):
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.
        """
        return DataFrameNaFunctions(self)

    @property
    @since(1.4)
    def stat(self):
        """Returns a :class:`DataFrameStatFunctions` for statistic functions.
        """
        return DataFrameStatFunctions(self)

    @ignore_unicode_prefix
    @since(1.3)
    def toJSON(self, use_unicode=True):
        """Converts a :class:`DataFrame` into a :class:`RDD` of string.

        Each row is turned into a JSON document as one element in the returned RDD.

        >>> df.toJSON().first()
        u'{"age":2,"name":"Alice"}'
        """
        rdd = self._jdf.toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

    @since(1.3)
    def registerTempTable(self, name):
        """Registers this RDD as a temporary table using the given name.

        The lifetime of this temporary table is tied to the :class:`SQLContext`
        that was used to create this :class:`DataFrame`.

        >>> df.registerTempTable("people")
        >>> df2 = spark.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropTempView("people")

        .. note:: Deprecated in 2.0, use createOrReplaceTempView instead.
        """
        self._jdf.createOrReplaceTempView(name)

    @since(2.0)
    def createTempView(self, name):
        """Creates a local temporary view with this DataFrame.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        >>> df.createTempView("people")
        >>> df2 = spark.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> df.createTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: u"Temporary table 'people' already exists;"
        >>> spark.catalog.dropTempView("people")

        """
        self._jdf.createTempView(name)

    @since(2.0)
    def createOrReplaceTempView(self, name):
        """Creates or replaces a local temporary view with this DataFrame.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        >>> df.createOrReplaceTempView("people")
        >>> df2 = df.filter(df.age > 3)
        >>> df2.createOrReplaceTempView("people")
        >>> df3 = spark.sql("select * from people")
        >>> sorted(df3.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropTempView("people")

        """
        self._jdf.createOrReplaceTempView(name)

    @since(2.1)
    def createGlobalTempView(self, name):
        """Creates a global temporary view with this DataFrame.

        The lifetime of this temporary view is tied to this Spark application.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        >>> df.createGlobalTempView("people")
        >>> df2 = spark.sql("select * from global_temp.people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> df.createGlobalTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: u"Temporary table 'people' already exists;"
        >>> spark.catalog.dropGlobalTempView("people")

        """
        self._jdf.createGlobalTempView(name)

    @since(2.2)
    def createOrReplaceGlobalTempView(self, name):
        """Creates or replaces a global temporary view using the given name.

        The lifetime of this temporary view is tied to this Spark application.

        >>> df.createOrReplaceGlobalTempView("people")
        >>> df2 = df.filter(df.age > 3)
        >>> df2.createOrReplaceGlobalTempView("people")
        >>> df3 = spark.sql("select * from global_temp.people")
        >>> sorted(df3.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropGlobalTempView("people")

        """
        self._jdf.createOrReplaceGlobalTempView(name)

    @property
    @since(1.4)
    def write(self):
        """
        Interface for saving the content of the non-streaming :class:`DataFrame` out into external
        storage.

        :return: :class:`DataFrameWriter`
        """
        return DataFrameWriter(self)

    @property
    @since(2.0)
    def writeStream(self):
        """
        Interface for saving the content of the streaming :class:`DataFrame` out into external
        storage.

        .. note:: Evolving.

        :return: :class:`DataStreamWriter`
        """
        return DataStreamWriter(self)

    @property
    @since(1.3)
    def schema(self):
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        >>> df.schema
        StructType(List(StructField(age,IntegerType,true),StructField(name,StringType,true)))
        """
        if self._schema is None:
            try:
                self._schema = _parse_datatype_json_string(self._jdf.schema().json())
            except AttributeError as e:
                raise Exception(
                    "Unable to parse datatype from schema. %s" % e)
        return self._schema

    @since(1.3)
    def printSchema(self):
        """Prints out the schema in the tree format.

        >>> df.printSchema()
        root
         |-- age: integer (nullable = true)
         |-- name: string (nullable = true)
        <BLANKLINE>
        """
        print(self._jdf.schema().treeString())

    @since(1.3)
    def explain(self, extended=False):
        """Prints the (logical and physical) plans to the console for debugging purpose.

        :param extended: boolean, default ``False``. If ``False``, prints only the physical plan.

        >>> df.explain()
        == Physical Plan ==
        Scan ExistingRDD[age#0,name#1]

        >>> df.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        """
        if extended:
            print(self._jdf.queryExecution().toString())
        else:
            print(self._jdf.queryExecution().simpleString())

    @since(1.3)
    def isLocal(self):
        """Returns ``True`` if the :func:`collect` and :func:`take` methods can be run locally
        (without any Spark executors).
        """
        return self._jdf.isLocal()

    @property
    @since(2.0)
    def isStreaming(self):
        """Returns true if this :class:`Dataset` contains one or more sources that continuously
        return data as it arrives. A :class:`Dataset` that reads data from a streaming source
        must be executed as a :class:`StreamingQuery` using the :func:`start` method in
        :class:`DataStreamWriter`.  Methods that return a single answer, (e.g., :func:`count` or
        :func:`collect`) will throw an :class:`AnalysisException` when there is a streaming
        source present.

        .. note:: Evolving
        """
        return self._jdf.isStreaming()

    @since(1.3)
    def show(self, n=20, truncate=True):
        """Prints the first ``n`` rows to the console.

        :param n: Number of rows to show.
        :param truncate: If set to True, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.

        >>> df
        DataFrame[age: int, name: string]
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> df.show(truncate=3)
        +---+----+
        |age|name|
        +---+----+
        |  2| Ali|
        |  5| Bob|
        +---+----+
        """
        if isinstance(truncate, bool) and truncate:
            print(self._jdf.showString(n, 20))
        else:
            print(self._jdf.showString(n, int(truncate)))

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    @since(2.1)
    def checkpoint(self, eager=True):
        """Returns a checkpointed version of this Dataset. Checkpointing can be used to truncate the
        logical plan of this DataFrame, which is especially useful in iterative algorithms where the
        plan may grow exponentially. It will be saved to files inside the checkpoint
        directory set with L{SparkContext.setCheckpointDir()}.

        :param eager: Whether to checkpoint this DataFrame immediately

        .. note:: Experimental
        """
        jdf = self._jdf.checkpoint(eager)
        return DataFrame(jdf, self.sql_ctx)

    @since(2.1)
    def withWatermark(self, eventTime, delayThreshold):
        """Defines an event time watermark for this :class:`DataFrame`. A watermark tracks a point
        in time before which we assume no more late data is going to arrive.

        Spark will use this watermark for several purposes:
          - To know when a given time window aggregation can be finalized and thus can be emitted
            when using output modes that do not allow updates.

          - To minimize the amount of state that we need to keep for on-going aggregations.

        The current watermark is computed by looking at the `MAX(eventTime)` seen across
        all of the partitions in the query minus a user specified `delayThreshold`.  Due to the cost
        of coordinating this value across partitions, the actual watermark used is only guaranteed
        to be at least `delayThreshold` behind the actual event time.  In some cases we may still
        process records that arrive more than `delayThreshold` late.

        :param eventTime: the name of the column that contains the event time of the row.
        :param delayThreshold: the minimum delay to wait to data to arrive late, relative to the
            latest record that has been processed in the form of an interval
            (e.g. "1 minute" or "5 hours").

        .. note:: Evolving

        >>> sdf.select('name', sdf.time.cast('timestamp')).withWatermark('time', '10 minutes')
        DataFrame[name: string, time: timestamp]
        """
        if not eventTime or type(eventTime) is not str:
            raise TypeError("eventTime should be provided as a string")
        if not delayThreshold or type(delayThreshold) is not str:
            raise TypeError("delayThreshold should be provided as a string interval")
        jdf = self._jdf.withWatermark(eventTime, delayThreshold)
        return DataFrame(jdf, self.sql_ctx)

    @since(2.2)
    def hint(self, name, *parameters):
        """Specifies some hint on the current DataFrame.

        :param name: A name of the hint.
        :param parameters: Optional parameters.
        :return: :class:`DataFrame`

        >>> df.join(df2.hint("broadcast"), "name").show()
        +----+---+------+
        |name|age|height|
        +----+---+------+
        | Bob|  5|    85|
        +----+---+------+
        """
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]

        if not isinstance(name, str):
            raise TypeError("name should be provided as str, got {0}".format(type(name)))

        for p in parameters:
            if not isinstance(p, str):
                raise TypeError(
                    "all parameters should be str, got {0} of type {1}".format(p, type(p)))

        jdf = self._jdf.hint(name, self._jseq(parameters))
        return DataFrame(jdf, self.sql_ctx)

    @since(1.3)
    def count(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> df.count()
        2
        """
        return int(self._jdf.count())

    @ignore_unicode_prefix
    @since(1.3)
    def collect(self):
        """Returns all the records as a list of :class:`Row`.

        >>> df.collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            sock_info = self._jdf.collectToPython()
        return list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))

    @ignore_unicode_prefix
    @since(2.0)
    def toLocalIterator(self):
        """
        Returns an iterator that contains all of the rows in this :class:`DataFrame`.
        The iterator will consume as much memory as the largest partition in this DataFrame.

        >>> list(df.toLocalIterator())
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            sock_info = self._jdf.toPythonIterator()
        return _load_from_socket(sock_info, BatchedSerializer(PickleSerializer()))

    @ignore_unicode_prefix
    @since(1.3)
    def limit(self, num):
        """Limits the result count to the number specified.

        >>> df.limit(1).collect()
        [Row(age=2, name=u'Alice')]
        >>> df.limit(0).collect()
        []
        """
        jdf = self._jdf.limit(num)
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def take(self, num):
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`.

        >>> df.take(2)
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        return self.limit(num).collect()

    @since(1.3)
    def foreach(self, f):
        """Applies the ``f`` function to all :class:`Row` of this :class:`DataFrame`.

        This is a shorthand for ``df.rdd.foreach()``.

        >>> def f(person):
        ...     print(person.name)
        >>> df.foreach(f)
        """
        self.rdd.foreach(f)

    @since(1.3)
    def foreachPartition(self, f):
        """Applies the ``f`` function to each partition of this :class:`DataFrame`.

        This a shorthand for ``df.rdd.foreachPartition()``.

        >>> def f(people):
        ...     for person in people:
        ...         print(person.name)
        >>> df.foreachPartition(f)
        """
        self.rdd.foreachPartition(f)

    @since(1.3)
    def cache(self):
        """Persists the :class:`DataFrame` with the default storage level (C{MEMORY_AND_DISK}).

        .. note:: The default storage level has changed to C{MEMORY_AND_DISK} to match Scala in 2.0.
        """
        self.is_cached = True
        self._jdf.cache()
        return self

    @since(1.3)
    def persist(self, storageLevel=StorageLevel.MEMORY_AND_DISK):
        """Sets the storage level to persist the contents of the :class:`DataFrame` across
        operations after the first time it is computed. This can only be used to assign
        a new storage level if the :class:`DataFrame` does not have a storage level set yet.
        If no storage level is specified defaults to (C{MEMORY_AND_DISK}).

        .. note:: The default storage level has changed to C{MEMORY_AND_DISK} to match Scala in 2.0.
        """
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jdf.persist(javaStorageLevel)
        return self

    @property
    @since(2.1)
    def storageLevel(self):
        """Get the :class:`DataFrame`'s current storage level.

        >>> df.storageLevel
        StorageLevel(False, False, False, False, 1)
        >>> df.cache().storageLevel
        StorageLevel(True, True, False, True, 1)
        >>> df2.persist(StorageLevel.DISK_ONLY_2).storageLevel
        StorageLevel(True, False, False, False, 2)
        """
        java_storage_level = self._jdf.storageLevel()
        storage_level = StorageLevel(java_storage_level.useDisk(),
                                     java_storage_level.useMemory(),
                                     java_storage_level.useOffHeap(),
                                     java_storage_level.deserialized(),
                                     java_storage_level.replication())
        return storage_level

    @since(1.3)
    def unpersist(self, blocking=False):
        """Marks the :class:`DataFrame` as non-persistent, and remove all blocks for it from
        memory and disk.

        .. note:: `blocking` default has changed to False to match Scala in 2.0.
        """
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    @since(1.4)
    def coalesce(self, numPartitions):
        """
        Returns a new :class:`DataFrame` that has exactly `numPartitions` partitions.

        Similar to coalesce defined on an :class:`RDD`, this operation results in a
        narrow dependency, e.g. if you go from 1000 partitions to 100 partitions,
        there will not be a shuffle, instead each of the 100 new partitions will
        claim 10 of the current partitions. If a larger number of partitions is requested,
        it will stay at the current number of partitions.

        However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
        this may result in your computation taking place on fewer nodes than
        you like (e.g. one node in the case of numPartitions = 1). To avoid this,
        you can call repartition(). This will add a shuffle step, but means the
        current upstream partitions will be executed in parallel (per whatever
        the current partitioning is).

        >>> df.coalesce(1).rdd.getNumPartitions()
        1
        """
        return DataFrame(self._jdf.coalesce(numPartitions), self.sql_ctx)

    @since(1.3)
    def repartition(self, numPartitions, *cols):
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting DataFrame is hash partitioned.

        ``numPartitions`` can be an int to specify the target number of partitions or a Column.
        If it is a Column, it will be used as the first partitioning column. If not specified,
        the default number of partitions is used.

        .. versionchanged:: 1.6
           Added optional arguments to specify the partitioning columns. Also made numPartitions
           optional if partitioning columns are specified.

        >>> df.repartition(10).rdd.getNumPartitions()
        10
        >>> data = df.union(df).repartition("age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  5|  Bob|
        |  2|Alice|
        |  2|Alice|
        +---+-----+
        >>> data = data.repartition(7, "age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> data.rdd.getNumPartitions()
        7
        >>> data = data.repartition("name", "age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  5|  Bob|
        |  2|Alice|
        |  2|Alice|
        +---+-----+
        """
        if isinstance(numPartitions, int):
            if len(cols) == 0:
                return DataFrame(self._jdf.repartition(numPartitions), self.sql_ctx)
            else:
                return DataFrame(
                    self._jdf.repartition(numPartitions, self._jcols(*cols)), self.sql_ctx)
        elif isinstance(numPartitions, (basestring, Column)):
            cols = (numPartitions, ) + cols
            return DataFrame(self._jdf.repartition(self._jcols(*cols)), self.sql_ctx)
        else:
            raise TypeError("numPartitions should be an int or Column")

    @since(1.3)
    def distinct(self):
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        >>> df.distinct().count()
        2
        """
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    @since(1.3)
    def sample(self, withReplacement, fraction, seed=None):
        """Returns a sampled subset of this :class:`DataFrame`.

        .. note:: This is not guaranteed to provide exactly the fraction specified of the total
            count of the given :class:`DataFrame`.

        >>> df.sample(False, 0.5, 42).count()
        2
        """
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        rdd = self._jdf.sample(withReplacement, fraction, long(seed))
        return DataFrame(rdd, self.sql_ctx)

    @since(1.5)
    def sampleBy(self, col, fractions, seed=None):
        """
        Returns a stratified sample without replacement based on the
        fraction given on each stratum.

        :param col: column that defines strata
        :param fractions:
            sampling fraction for each stratum. If a stratum is not
            specified, we treat its fraction as zero.
        :param seed: random seed
        :return: a new DataFrame that represents the stratified sample

        >>> from pyspark.sql.functions import col
        >>> dataset = sqlContext.range(0, 100).select((col("id") % 3).alias("key"))
        >>> sampled = dataset.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
        >>> sampled.groupBy("key").count().orderBy("key").show()
        +---+-----+
        |key|count|
        +---+-----+
        |  0|    5|
        |  1|    9|
        +---+-----+

        """
        if not isinstance(col, str):
            raise ValueError("col must be a string, but got %r" % type(col))
        if not isinstance(fractions, dict):
            raise ValueError("fractions must be a dict but got %r" % type(fractions))
        for k, v in fractions.items():
            if not isinstance(k, (float, int, long, basestring)):
                raise ValueError("key must be float, int, long, or string, but got %r" % type(k))
            fractions[k] = float(v)
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        return DataFrame(self._jdf.stat().sampleBy(col, self._jmap(fractions), seed), self.sql_ctx)

    @since(1.4)
    def randomSplit(self, weights, seed=None):
        """Randomly splits this :class:`DataFrame` with the provided weights.

        :param weights: list of doubles as weights with which to split the DataFrame. Weights will
            be normalized if they don't sum up to 1.0.
        :param seed: The seed for sampling.

        >>> splits = df4.randomSplit([1.0, 2.0], 24)
        >>> splits[0].count()
        1

        >>> splits[1].count()
        3
        """
        for w in weights:
            if w < 0.0:
                raise ValueError("Weights must be positive. Found weight value: %s" % w)
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        rdd_array = self._jdf.randomSplit(_to_list(self.sql_ctx._sc, weights), long(seed))
        return [DataFrame(rdd, self.sql_ctx) for rdd in rdd_array]

    @property
    @since(1.3)
    def dtypes(self):
        """Returns all column names and their data types as a list.

        >>> df.dtypes
        [('age', 'int'), ('name', 'string')]
        """
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    @since(1.3)
    def columns(self):
        """Returns all column names as a list.

        >>> df.columns
        ['age', 'name']
        """
        return [f.name for f in self.schema.fields]

    @ignore_unicode_prefix
    @since(1.3)
    def alias(self, alias):
        """Returns a new :class:`DataFrame` with an alias set.

        >>> from pyspark.sql.functions import *
        >>> df_as1 = df.alias("df_as1")
        >>> df_as2 = df.alias("df_as2")
        >>> joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
        >>> joined_df.select("df_as1.name", "df_as2.name", "df_as2.age").collect()
        [Row(name=u'Bob', name=u'Bob', age=5), Row(name=u'Alice', name=u'Alice', age=2)]
        """
        assert isinstance(alias, basestring), "alias should be a string"
        return DataFrame(getattr(self._jdf, "as")(alias), self.sql_ctx)

    @ignore_unicode_prefix
    @since(2.1)
    def crossJoin(self, other):
        """Returns the cartesian product with another :class:`DataFrame`.

        :param other: Right side of the cartesian product.

        >>> df.select("age", "name").collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        >>> df2.select("name", "height").collect()
        [Row(name=u'Tom', height=80), Row(name=u'Bob', height=85)]
        >>> df.crossJoin(df2.select("height")).select("age", "name", "height").collect()
        [Row(age=2, name=u'Alice', height=80), Row(age=2, name=u'Alice', height=85),
         Row(age=5, name=u'Bob', height=80), Row(age=5, name=u'Bob', height=85)]
        """

        jdf = self._jdf.crossJoin(other._jdf)
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def join(self, other, on=None, how=None):
        """Joins with another :class:`DataFrame`, using the given join expression.

        :param other: Right side of the join
        :param on: a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        :param how: str, default ``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
            ``full``, ``full_outer``, ``left``, ``left_outer``, ``right``, ``right_outer``,
            ``left_semi``, and ``left_anti``.

        The following performs a full outer join between ``df1`` and ``df2``.

        >>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
        [Row(name=None, height=80), Row(name=u'Bob', height=85), Row(name=u'Alice', height=None)]

        >>> df.join(df2, 'name', 'outer').select('name', 'height').collect()
        [Row(name=u'Tom', height=80), Row(name=u'Bob', height=85), Row(name=u'Alice', height=None)]

        >>> cond = [df.name == df3.name, df.age == df3.age]
        >>> df.join(df3, cond, 'outer').select(df.name, df3.age).collect()
        [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]

        >>> df.join(df2, 'name').select(df.name, df2.height).collect()
        [Row(name=u'Bob', height=85)]

        >>> df.join(df4, ['name', 'age']).select(df.name, df.age).collect()
        [Row(name=u'Bob', age=5)]
        """

        if on is not None and not isinstance(on, list):
            on = [on]

        if on is not None:
            if isinstance(on[0], basestring):
                on = self._jseq(on)
            else:
                assert isinstance(on[0], Column), "on should be Column or list of Column"
                on = reduce(lambda x, y: x.__and__(y), on)
                on = on._jc

        if on is None and how is None:
            jdf = self._jdf.join(other._jdf)
        else:
            if how is None:
                how = "inner"
            assert isinstance(how, basestring), "how should be basestring"
            jdf = self._jdf.join(other._jdf, on, how)
        return DataFrame(jdf, self.sql_ctx)

    @since(1.6)
    def sortWithinPartitions(self, *cols, **kwargs):
        """Returns a new :class:`DataFrame` with each partition sorted by the specified column(s).

        :param cols: list of :class:`Column` or column names to sort by.
        :param ascending: boolean or list of boolean (default True).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        >>> df.sortWithinPartitions("age", ascending=False).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        jdf = self._jdf.sortWithinPartitions(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def sort(self, *cols, **kwargs):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        :param cols: list of :class:`Column` or column names to sort by.
        :param ascending: boolean or list of boolean (default True).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        >>> df.sort(df.age.desc()).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        >>> df.sort("age", ascending=False).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        >>> df.orderBy(df.age.desc()).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        >>> from pyspark.sql.functions import *
        >>> df.sort(asc("age")).collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        >>> df.orderBy(desc("age"), "name").collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        """
        jdf = self._jdf.sort(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sql_ctx)

    orderBy = sort

    def _jseq(self, cols, converter=None):
        """Return a JVM Seq of Columns from a list of Column or names"""
        return _to_seq(self.sql_ctx._sc, cols, converter)

    def _jmap(self, jm):
        """Return a JVM Scala Map from a dict"""
        return _to_scala_map(self.sql_ctx._sc, jm)

    def _jcols(self, *cols):
        """Return a JVM Seq of Columns from a list of Column or column names

        If `cols` has only one list in it, cols[0] will be used as the list.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        return self._jseq(cols, _to_java_column)

    def _sort_cols(self, cols, kwargs):
        """ Return a JVM Seq of Columns that describes the sort order
        """
        if not cols:
            raise ValueError("should sort by at least one column")
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        jcols = [_to_java_column(c) for c in cols]
        ascending = kwargs.get('ascending', True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                jcols = [jc.desc() for jc in jcols]
        elif isinstance(ascending, list):
            jcols = [jc if asc else jc.desc()
                     for asc, jc in zip(ascending, jcols)]
        else:
            raise TypeError("ascending can only be boolean or list, but got %s" % type(ascending))
        return self._jseq(jcols)

    @since("1.3.1")
    def describe(self, *cols):
        """Computes statistics for numeric and string columns.

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        .. note:: This function is meant for exploratory data analysis, as we make no
            guarantee about the backward compatibility of the schema of the resulting DataFrame.

        >>> df.describe(['age']).show()
        +-------+------------------+
        |summary|               age|
        +-------+------------------+
        |  count|                 2|
        |   mean|               3.5|
        | stddev|2.1213203435596424|
        |    min|                 2|
        |    max|                 5|
        +-------+------------------+
        >>> df.describe().show()
        +-------+------------------+-----+
        |summary|               age| name|
        +-------+------------------+-----+
        |  count|                 2|    2|
        |   mean|               3.5| null|
        | stddev|2.1213203435596424| null|
        |    min|                 2|Alice|
        |    max|                 5|  Bob|
        +-------+------------------+-----+
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        jdf = self._jdf.describe(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def head(self, n=None):
        """Returns the first ``n`` rows.

        .. note:: This method should only be used if the resulting array is expected
            to be small, as all the data is loaded into the driver's memory.

        :param n: int, default 1. Number of rows to return.
        :return: If n is greater than 1, return a list of :class:`Row`.
            If n is 1, return a single Row.

        >>> df.head()
        Row(age=2, name=u'Alice')
        >>> df.head(1)
        [Row(age=2, name=u'Alice')]
        """
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    @ignore_unicode_prefix
    @since(1.3)
    def first(self):
        """Returns the first row as a :class:`Row`.

        >>> df.first()
        Row(age=2, name=u'Alice')
        """
        return self.head()

    @ignore_unicode_prefix
    @since(1.3)
    def __getitem__(self, item):
        """Returns the column as a :class:`Column`.

        >>> df.select(df['age']).collect()
        [Row(age=2), Row(age=5)]
        >>> df[ ["name", "age"]].collect()
        [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
        >>> df[ df.age > 3 ].collect()
        [Row(age=5, name=u'Bob')]
        >>> df[df[0] > 3].collect()
        [Row(age=5, name=u'Bob')]
        """
        if isinstance(item, basestring):
            jc = self._jdf.apply(item)
            return Column(jc)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            jc = self._jdf.apply(self.columns[item])
            return Column(jc)
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    @since(1.3)
    def __getattr__(self, name):
        """Returns the :class:`Column` denoted by ``name``.

        >>> df.select(df.age).collect()
        [Row(age=2), Row(age=5)]
        """
        if name not in self.columns:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
        jc = self._jdf.apply(name)
        return Column(jc)

    @ignore_unicode_prefix
    @since(1.3)
    def select(self, *cols):
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        :param cols: list of column names (string) or expressions (:class:`Column`).
            If one of the column names is '*', that column is expanded to include all columns
            in the current DataFrame.

        >>> df.select('*').collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        >>> df.select('name', 'age').collect()
        [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
        >>> df.select(df.name, (df.age + 10).alias('age')).collect()
        [Row(name=u'Alice', age=12), Row(name=u'Bob', age=15)]
        """
        jdf = self._jdf.select(self._jcols(*cols))
        return DataFrame(jdf, self.sql_ctx)

    @since(1.3)
    def selectExpr(self, *expr):
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        >>> df.selectExpr("age * 2", "abs(age)").collect()
        [Row((age * 2)=4, abs(age)=2), Row((age * 2)=10, abs(age)=5)]
        """
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]
        jdf = self._jdf.selectExpr(self._jseq(expr))
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def filter(self, condition):
        """Filters rows using the given condition.

        :func:`where` is an alias for :func:`filter`.

        :param condition: a :class:`Column` of :class:`types.BooleanType`
            or a string of SQL expression.

        >>> df.filter(df.age > 3).collect()
        [Row(age=5, name=u'Bob')]
        >>> df.where(df.age == 2).collect()
        [Row(age=2, name=u'Alice')]

        >>> df.filter("age > 3").collect()
        [Row(age=5, name=u'Bob')]
        >>> df.where("age = 2").collect()
        [Row(age=2, name=u'Alice')]
        """
        if isinstance(condition, basestring):
            jdf = self._jdf.filter(condition)
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def groupBy(self, *cols):
        """Groups the :class:`DataFrame` using the specified columns,
        so we can run aggregation on them. See :class:`GroupedData`
        for all the available aggregate functions.

        :func:`groupby` is an alias for :func:`groupBy`.

        :param cols: list of columns to group by.
            Each element should be a column name (string) or an expression (:class:`Column`).

        >>> df.groupBy().avg().collect()
        [Row(avg(age)=3.5)]
        >>> sorted(df.groupBy('name').agg({'age': 'mean'}).collect())
        [Row(name=u'Alice', avg(age)=2.0), Row(name=u'Bob', avg(age)=5.0)]
        >>> sorted(df.groupBy(df.name).avg().collect())
        [Row(name=u'Alice', avg(age)=2.0), Row(name=u'Bob', avg(age)=5.0)]
        >>> sorted(df.groupBy(['name', df.age]).count().collect())
        [Row(name=u'Alice', age=2, count=1), Row(name=u'Bob', age=5, count=1)]
        """
        jgd = self._jdf.groupBy(self._jcols(*cols))
        from pyspark.sql.group import GroupedData
        return GroupedData(jgd, self.sql_ctx)

    @since(1.4)
    def rollup(self, *cols):
        """
        Create a multi-dimensional rollup for the current :class:`DataFrame` using
        the specified columns, so we can run aggregation on them.

        >>> df.rollup("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | null|null|    2|
        |Alice|null|    1|
        |Alice|   2|    1|
        |  Bob|null|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        jgd = self._jdf.rollup(self._jcols(*cols))
        from pyspark.sql.group import GroupedData
        return GroupedData(jgd, self.sql_ctx)

    @since(1.4)
    def cube(self, *cols):
        """
        Create a multi-dimensional cube for the current :class:`DataFrame` using
        the specified columns, so we can run aggregation on them.

        >>> df.cube("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | null|null|    2|
        | null|   2|    1|
        | null|   5|    1|
        |Alice|null|    1|
        |Alice|   2|    1|
        |  Bob|null|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        jgd = self._jdf.cube(self._jcols(*cols))
        from pyspark.sql.group import GroupedData
        return GroupedData(jgd, self.sql_ctx)

    @since(1.3)
    def agg(self, *exprs):
        """ Aggregate on the entire :class:`DataFrame` without groups
        (shorthand for ``df.groupBy.agg()``).

        >>> df.agg({"age": "max"}).collect()
        [Row(max(age)=5)]
        >>> from pyspark.sql import functions as F
        >>> df.agg(F.min(df.age)).collect()
        [Row(min(age)=2)]
        """
        return self.groupBy().agg(*exprs)

    @since(2.0)
    def union(self, other):
        """ Return a new :class:`DataFrame` containing union of rows in this and another frame.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by a distinct.

        Also as standard in SQL, this function resolves columns by position (not by name).
        """
        return DataFrame(self._jdf.union(other._jdf), self.sql_ctx)

    @since(1.3)
    def unionAll(self, other):
        """ Return a new :class:`DataFrame` containing union of rows in this and another frame.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by a distinct.

        Also as standard in SQL, this function resolves columns by position (not by name).

        .. note:: Deprecated in 2.0, use union instead.
        """
        return self.union(other)

    @since(1.3)
    def intersect(self, other):
        """ Return a new :class:`DataFrame` containing rows only in
        both this frame and another frame.

        This is equivalent to `INTERSECT` in SQL.
        """
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    @since(1.3)
    def subtract(self, other):
        """ Return a new :class:`DataFrame` containing rows in this frame
        but not in another frame.

        This is equivalent to `EXCEPT` in SQL.
        """
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    @since(1.4)
    def dropDuplicates(self, subset=None):
        """Return a new :class:`DataFrame` with duplicate rows removed,
        optionally only considering certain columns.

        For a static batch :class:`DataFrame`, it just drops duplicate rows. For a streaming
        :class:`DataFrame`, it will keep all data across triggers as intermediate state to drop
        duplicates rows. You can use :func:`withWatermark` to limit how late the duplicate data can
        be and system will accordingly limit the state. In addition, too late data older than
        watermark will be dropped to avoid any possibility of duplicates.

        :func:`drop_duplicates` is an alias for :func:`dropDuplicates`.

        >>> from pyspark.sql import Row
        >>> df = sc.parallelize([ \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=10, height=80)]).toDF()
        >>> df.dropDuplicates().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        |  5|    80|Alice|
        | 10|    80|Alice|
        +---+------+-----+

        >>> df.dropDuplicates(['name', 'height']).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        |  5|    80|Alice|
        +---+------+-----+
        """
        if subset is None:
            jdf = self._jdf.dropDuplicates()
        else:
            jdf = self._jdf.dropDuplicates(self._jseq(subset))
        return DataFrame(jdf, self.sql_ctx)

    @since("1.3.1")
    def dropna(self, how='any', thresh=None, subset=None):
        """Returns a new :class:`DataFrame` omitting rows with null values.
        :func:`DataFrame.dropna` and :func:`DataFrameNaFunctions.drop` are aliases of each other.

        :param how: 'any' or 'all'.
            If 'any', drop a row if it contains any nulls.
            If 'all', drop a row only if all its values are null.
        :param thresh: int, default None
            If specified, drop rows that have less than `thresh` non-null values.
            This overwrites the `how` parameter.
        :param subset: optional list of column names to consider.

        >>> df4.na.drop().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        +---+------+-----+
        """
        if how is not None and how not in ['any', 'all']:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, basestring):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == 'any' else 1

        return DataFrame(self._jdf.na().drop(thresh, self._jseq(subset)), self.sql_ctx)

    @since("1.3.1")
    def fillna(self, value, subset=None):
        """Replace null values, alias for ``na.fill()``.
        :func:`DataFrame.fillna` and :func:`DataFrameNaFunctions.fill` are aliases of each other.

        :param value: int, long, float, string, or dict.
            Value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, long, float, boolean, or string.
        :param subset: optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        >>> df4.na.fill(50).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        |  5|    50|  Bob|
        | 50|    50|  Tom|
        | 50|    50| null|
        +---+------+-----+

        >>> df4.na.fill({'age': 50, 'name': 'unknown'}).show()
        +---+------+-------+
        |age|height|   name|
        +---+------+-------+
        | 10|    80|  Alice|
        |  5|  null|    Bob|
        | 50|  null|    Tom|
        | 50|  null|unknown|
        +---+------+-------+
        """
        if not isinstance(value, (float, int, long, basestring, dict)):
            raise ValueError("value should be a float, int, long, string, or dict")

        if isinstance(value, (int, long)):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        elif subset is None:
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        else:
            if isinstance(subset, basestring):
                subset = [subset]
            elif not isinstance(subset, (list, tuple)):
                raise ValueError("subset should be a list or tuple of column names")

            return DataFrame(self._jdf.na().fill(value, self._jseq(subset)), self.sql_ctx)

    @since(1.4)
    def replace(self, to_replace, value=None, subset=None):
        """Returns a new :class:`DataFrame` replacing a value with another value.
        :func:`DataFrame.replace` and :func:`DataFrameNaFunctions.replace` are
        aliases of each other.
        Values to_replace and value should contain either all numerics, all booleans,
        or all strings. When replacing, the new value will be cast
        to the type of the existing column.
        For numeric replacements all values to be replaced should have unique
        floating point representation. In case of conflicts (for example with `{42: -1, 42.0: 1}`)
        and arbitrary replacement will be used.

        :param to_replace: bool, int, long, float, string, list or dict.
            Value to be replaced.
            If the value is a dict, then `value` is ignored and `to_replace` must be a
            mapping between a value and a replacement.
        :param value: int, long, float, string, or list.
            The replacement value must be an int, long, float, or string. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.
        :param subset: optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        >>> df4.na.replace(10, 20).show()
        +----+------+-----+
        | age|height| name|
        +----+------+-----+
        |  20|    80|Alice|
        |   5|  null|  Bob|
        |null|  null|  Tom|
        |null|  null| null|
        +----+------+-----+

        >>> df4.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|   A|
        |   5|  null|   B|
        |null|  null| Tom|
        |null|  null|null|
        +----+------+----+
        """
        # Helper functions
        def all_of(types):
            """Given a type or tuple of types and a sequence of xs
            check if each x is instance of type(s)

            >>> all_of(bool)([True, False])
            True
            >>> all_of(basestring)(["a", 1])
            False
            """
            def all_of_(xs):
                return all(isinstance(x, types) for x in xs)
            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(basestring)
        all_of_numeric = all_of((float, int, long))

        # Validate input types
        valid_types = (bool, float, int, long, basestring, list, tuple)
        if not isinstance(to_replace, valid_types + (dict, )):
            raise ValueError(
                "to_replace should be a float, int, long, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace)))

        if not isinstance(value, valid_types) and not isinstance(to_replace, dict):
            raise ValueError("If to_replace is not a dict, value should be "
                             "a float, int, long, string, list, or tuple. "
                             "Got {0}".format(type(value)))

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError("to_replace and value lists should be of the same length. "
                                 "Got {0} and {1}".format(len(to_replace), len(value)))

        if not (subset is None or isinstance(subset, (list, tuple, basestring))):
            raise ValueError("subset should be a list or tuple of column names, "
                             "column name or None. Got {0}".format(type(subset)))

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, long, basestring)):
            to_replace = [to_replace]

        if isinstance(value, (float, int, long, basestring)):
            value = [value for _ in range(len(to_replace))]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            rep_dict = dict(zip(to_replace, value))

        if isinstance(subset, basestring):
            subset = [subset]

        # Verify we were not passed in mixed type generics."
        if not any(all_of_type(rep_dict.keys()) and all_of_type(rep_dict.values())
                   for all_of_type in [all_of_bool, all_of_str, all_of_numeric]):
            raise ValueError("Mixed type replacements are not supported")

        if subset is None:
            return DataFrame(self._jdf.na().replace('*', rep_dict), self.sql_ctx)
        else:
            return DataFrame(
                self._jdf.na().replace(self._jseq(subset), self._jmap(rep_dict)), self.sql_ctx)

    @since(2.0)
    def approxQuantile(self, col, probabilities, relativeError):
        """
        Calculates the approximate quantiles of numerical columns of a
        DataFrame.

        The result of this algorithm has the following deterministic bound:
        If the DataFrame has N elements and if we request the quantile at
        probability `p` up to error `err`, then the algorithm will return
        a sample `x` from the DataFrame so that the *exact* rank of `x` is
        close to (p * N). More precisely,

          floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

        This method implements a variation of the Greenwald-Khanna
        algorithm (with some speed optimizations). The algorithm was first
        present in [[http://dx.doi.org/10.1145/375663.375670
        Space-efficient Online Computation of Quantile Summaries]]
        by Greenwald and Khanna.

        Note that null values will be ignored in numerical columns before calculation.
        For columns only containing null values, an empty list is returned.

        :param col: str, list.
          Can be a single column name, or a list of names for multiple columns.
        :param probabilities: a list of quantile probabilities
          Each number must belong to [0, 1].
          For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
        :param relativeError:  The relative target precision to achieve
          (>= 0). If set to zero, the exact quantiles are computed, which
          could be very expensive. Note that values greater than 1 are
          accepted but give the same result as 1.
        :return:  the approximate quantiles at the given probabilities. If
          the input `col` is a string, the output is a list of floats. If the
          input `col` is a list or tuple of strings, the output is also a
          list, but each element in it is a list of floats, i.e., the output
          is a list of list of floats.

        .. versionchanged:: 2.2
           Added support for multiple columns.
        """

        if not isinstance(col, (str, list, tuple)):
            raise ValueError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isinstance(col, str):
            col = [col]

        for c in col:
            if not isinstance(c, str):
                raise ValueError("columns should be strings, but got %r" % type(c))
        col = _to_list(self._sc, col)

        if not isinstance(probabilities, (list, tuple)):
            raise ValueError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int, long)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int, long) in [0,1].")
        probabilities = _to_list(self._sc, probabilities)

        if not isinstance(relativeError, (float, int, long)) or relativeError < 0:
            raise ValueError("relativeError should be numerical (float, int, long) >= 0.")
        relativeError = float(relativeError)

        jaq = self._jdf.stat().approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    @since(1.4)
    def corr(self, col1, col2, method=None):
        """
        Calculates the correlation of two columns of a DataFrame as a double value.
        Currently only supports the Pearson Correlation Coefficient.
        :func:`DataFrame.corr` and :func:`DataFrameStatFunctions.corr` are aliases of each other.

        :param col1: The name of the first column
        :param col2: The name of the second column
        :param method: The correlation method. Currently only supports "pearson"
        """
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise ValueError("Currently only the calculation of the Pearson Correlation " +
                             "coefficient is supported.")
        return self._jdf.stat().corr(col1, col2, method)

    @since(1.4)
    def cov(self, col1, col2):
        """
        Calculate the sample covariance for the given columns, specified by their names, as a
        double value. :func:`DataFrame.cov` and :func:`DataFrameStatFunctions.cov` are aliases.

        :param col1: The name of the first column
        :param col2: The name of the second column
        """
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        return self._jdf.stat().cov(col1, col2)

    @since(1.4)
    def crosstab(self, col1, col2):
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency
        table. The number of distinct values for each column should be less than 1e4. At most 1e6
        non-zero pair frequencies will be returned.
        The first column of each row will be the distinct values of `col1` and the column names
        will be the distinct values of `col2`. The name of the first column will be `$col1_$col2`.
        Pairs that have no occurrences will have zero as their counts.
        :func:`DataFrame.crosstab` and :func:`DataFrameStatFunctions.crosstab` are aliases.

        :param col1: The name of the first column. Distinct items will make the first item of
            each row.
        :param col2: The name of the second column. Distinct items will make the column names
            of the DataFrame.
        """
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        return DataFrame(self._jdf.stat().crosstab(col1, col2), self.sql_ctx)

    @since(1.4)
    def freqItems(self, cols, support=None):
        """
        Finding frequent items for columns, possibly with false positives. Using the
        frequent element count algorithm described in
        "http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou".
        :func:`DataFrame.freqItems` and :func:`DataFrameStatFunctions.freqItems` are aliases.

        .. note:: This function is meant for exploratory data analysis, as we make no
            guarantee about the backward compatibility of the schema of the resulting DataFrame.

        :param cols: Names of the columns to calculate frequent items for as a list or tuple of
            strings.
        :param support: The frequency with which to consider an item 'frequent'. Default is 1%.
            The support must be greater than 1e-4.
        """
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise ValueError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.stat().freqItems(_to_seq(self._sc, cols), support), self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def withColumn(self, colName, col):
        """
        Returns a new :class:`DataFrame` by adding a column or replacing the
        existing column that has the same name.

        :param colName: string, name of the new column.
        :param col: a :class:`Column` expression for the new column.

        >>> df.withColumn('age2', df.age + 2).collect()
        [Row(age=2, name=u'Alice', age2=4), Row(age=5, name=u'Bob', age2=7)]
        """
        assert isinstance(col, Column), "col should be Column"
        return DataFrame(self._jdf.withColumn(colName, col._jc), self.sql_ctx)

    @ignore_unicode_prefix
    @since(1.3)
    def withColumnRenamed(self, existing, new):
        """Returns a new :class:`DataFrame` by renaming an existing column.
        This is a no-op if schema doesn't contain the given column name.

        :param existing: string, name of the existing column to rename.
        :param col: string, new name of the column.

        >>> df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
        """
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)

    @since(1.4)
    @ignore_unicode_prefix
    def drop(self, *cols):
        """Returns a new :class:`DataFrame` that drops the specified column.
        This is a no-op if schema doesn't contain the given column name(s).

        :param cols: a string name of the column to drop, or a
            :class:`Column` to drop, or a list of string name of the columns to drop.

        >>> df.drop('age').collect()
        [Row(name=u'Alice'), Row(name=u'Bob')]

        >>> df.drop(df.age).collect()
        [Row(name=u'Alice'), Row(name=u'Bob')]

        >>> df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()
        [Row(age=5, height=85, name=u'Bob')]

        >>> df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()
        [Row(age=5, name=u'Bob', height=85)]

        >>> df.join(df2, 'name', 'inner').drop('age', 'height').collect()
        [Row(name=u'Bob')]
        """
        if len(cols) == 1:
            col = cols[0]
            if isinstance(col, basestring):
                jdf = self._jdf.drop(col)
            elif isinstance(col, Column):
                jdf = self._jdf.drop(col._jc)
            else:
                raise TypeError("col should be a string or a Column")
        else:
            for col in cols:
                if not isinstance(col, basestring):
                    raise TypeError("each col in the param list should be a string")
            jdf = self._jdf.drop(self._jseq(cols))

        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    def toDF(self, *cols):
        """Returns a new class:`DataFrame` that with new specified column names

        :param cols: list of new column names (string)

        >>> df.toDF('f1', 'f2').collect()
        [Row(f1=2, f2=u'Alice'), Row(f1=5, f2=u'Bob')]
        """
        jdf = self._jdf.toDF(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    @since(1.3)
    def toPandas(self):
        """Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This is only available if Pandas is installed and available.

        .. note:: This method should only be used if the resulting Pandas's DataFrame is expected
            to be small, as all the data is loaded into the driver's memory.

        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        import pandas as pd
        return pd.DataFrame.from_records(self.collect(), columns=self.columns)

    ##########################################################################################
    # Pandas compatibility
    ##########################################################################################

    groupby = copy_func(
        groupBy,
        sinceversion=1.4,
        doc=":func:`groupby` is an alias for :func:`groupBy`.")

    drop_duplicates = copy_func(
        dropDuplicates,
        sinceversion=1.4,
        doc=":func:`drop_duplicates` is an alias for :func:`dropDuplicates`.")

    where = copy_func(
        filter,
        sinceversion=1.3,
        doc=":func:`where` is an alias for :func:`filter`.")


def _to_scala_map(sc, jm):
    """
    Convert a dict into a JVM Map.
    """
    return sc._jvm.PythonUtils.toScalaMap(jm)


class DataFrameNaFunctions(object):
    """Functionality for working with missing data in :class:`DataFrame`.

    .. versionadded:: 1.4
    """

    def __init__(self, df):
        self.df = df

    def drop(self, how='any', thresh=None, subset=None):
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def fill(self, value, subset=None):
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__

    def replace(self, to_replace, value, subset=None):
        return self.df.replace(to_replace, value, subset)

    replace.__doc__ = DataFrame.replace.__doc__


class DataFrameStatFunctions(object):
    """Functionality for statistic functions with :class:`DataFrame`.

    .. versionadded:: 1.4
    """

    def __init__(self, df):
        self.df = df

    def approxQuantile(self, col, probabilities, relativeError):
        return self.df.approxQuantile(col, probabilities, relativeError)

    approxQuantile.__doc__ = DataFrame.approxQuantile.__doc__

    def corr(self, col1, col2, method=None):
        return self.df.corr(col1, col2, method)

    corr.__doc__ = DataFrame.corr.__doc__

    def cov(self, col1, col2):
        return self.df.cov(col1, col2)

    cov.__doc__ = DataFrame.cov.__doc__

    def crosstab(self, col1, col2):
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__

    def freqItems(self, cols, support=None):
        return self.df.freqItems(cols, support)

    freqItems.__doc__ = DataFrame.freqItems.__doc__

    def sampleBy(self, col, fractions, seed=None):
        return self.df.sampleBy(col, fractions, seed)

    sampleBy.__doc__ = DataFrame.sampleBy.__doc__


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext, SparkSession
    import pyspark.sql.dataframe
    from pyspark.sql.functions import from_unixtime
    globs = pyspark.sql.dataframe.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['spark'] = SparkSession(sc)
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')])\
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))
    globs['df2'] = sc.parallelize([Row(name='Tom', height=80), Row(name='Bob', height=85)]).toDF()
    globs['df3'] = sc.parallelize([Row(name='Alice', age=2),
                                   Row(name='Bob', age=5)]).toDF()
    globs['df4'] = sc.parallelize([Row(name='Alice', age=10, height=80),
                                   Row(name='Bob', age=5, height=None),
                                   Row(name='Tom', age=None, height=None),
                                   Row(name=None, age=None, height=None)]).toDF()
    globs['sdf'] = sc.parallelize([Row(name='Tom', time=1479441846),
                                   Row(name='Bob', time=1479442946)]).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
