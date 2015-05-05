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
import warnings
import random

if sys.version >= '3':
    basestring = unicode = str
    long = int
else:
    from itertools import imap as map

from pyspark.context import SparkContext
from pyspark.rdd import RDD, _load_from_socket, ignore_unicode_prefix
from pyspark.serializers import BatchedSerializer, PickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.sql.types import *
from pyspark.sql.types import _create_cls, _parse_datatype_json_string


__all__ = ["DataFrame", "GroupedData", "Column", "SchemaRDD", "DataFrameNaFunctions",
           "DataFrameStatFunctions"]


class DataFrame(object):
    """A distributed collection of data grouped into named columns.

    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SQLContext`::

        people = sqlContext.parquetFile("...")

    Once created, it can be manipulated using the various domain-specific-language
    (DSL) functions defined in: :class:`DataFrame`, :class:`Column`.

    To select a column from the data frame, use the apply method::

        ageCol = people.age

    A more concrete example::

        # To create DataFrame using SQLContext
        people = sqlContext.parquetFile("...")
        department = sqlContext.parquetFile("...")

        people.filter(people.age > 30).join(department, people.deptId == department.id)) \
          .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})
    """

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc
        self.is_cached = False
        self._schema = None  # initialized lazily
        self._lazy_rdd = None

    @property
    def rdd(self):
        """Returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        """
        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            rdd = RDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()))
            schema = self.schema

            def applySchema(it):
                cls = _create_cls(schema)
                return map(cls, it)

            self._lazy_rdd = rdd.mapPartitions(applySchema)

        return self._lazy_rdd

    @property
    def na(self):
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.
        """
        return DataFrameNaFunctions(self)

    @property
    def stat(self):
        """Returns a :class:`DataFrameStatFunctions` for statistic functions.
        """
        return DataFrameStatFunctions(self)

    @ignore_unicode_prefix
    def toJSON(self, use_unicode=True):
        """Converts a :class:`DataFrame` into a :class:`RDD` of string.

        Each row is turned into a JSON document as one element in the returned RDD.

        >>> df.toJSON().first()
        u'{"age":2,"name":"Alice"}'
        """
        rdd = self._jdf.toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

    def saveAsParquetFile(self, path):
        """Saves the contents as a Parquet file, preserving the schema.

        Files that are written out using this method can be read back in as
        a :class:`DataFrame` using :func:`SQLContext.parquetFile`.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> df.saveAsParquetFile(parquetFile)
        >>> df2 = sqlContext.parquetFile(parquetFile)
        >>> sorted(df2.collect()) == sorted(df.collect())
        True
        """
        self._jdf.saveAsParquetFile(path)

    def registerTempTable(self, name):
        """Registers this RDD as a temporary table using the given name.

        The lifetime of this temporary table is tied to the :class:`SQLContext`
        that was used to create this :class:`DataFrame`.

        >>> df.registerTempTable("people")
        >>> df2 = sqlContext.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        self._jdf.registerTempTable(name)

    def registerAsTable(self, name):
        """DEPRECATED: use :func:`registerTempTable` instead"""
        warnings.warn("Use registerTempTable instead of registerAsTable.", DeprecationWarning)
        self.registerTempTable(name)

    def insertInto(self, tableName, overwrite=False):
        """Inserts the contents of this :class:`DataFrame` into the specified table.

        Optionally overwriting any existing data.
        """
        self._jdf.insertInto(tableName, overwrite)

    def _java_save_mode(self, mode):
        """Returns the Java save mode based on the Python save mode represented by a string.
        """
        jSaveMode = self._sc._jvm.org.apache.spark.sql.SaveMode
        jmode = jSaveMode.ErrorIfExists
        mode = mode.lower()
        if mode == "append":
            jmode = jSaveMode.Append
        elif mode == "overwrite":
            jmode = jSaveMode.Overwrite
        elif mode == "ignore":
            jmode = jSaveMode.Ignore
        elif mode == "error":
            pass
        else:
            raise ValueError(
                "Only 'append', 'overwrite', 'ignore', and 'error' are acceptable save mode.")
        return jmode

    def saveAsTable(self, tableName, source=None, mode="error", **options):
        """Saves the contents of this :class:`DataFrame` to a data source as a table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Additionally, mode is used to specify the behavior of the saveAsTable operation when
        table already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.
        """
        if source is None:
            source = self.sql_ctx.getConf("spark.sql.sources.default",
                                          "org.apache.spark.sql.parquet")
        jmode = self._java_save_mode(mode)
        self._jdf.saveAsTable(tableName, source, jmode, options)

    def save(self, path=None, source=None, mode="error", **options):
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Additionally, mode is used to specify the behavior of the save operation when
        data already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self.sql_ctx.getConf("spark.sql.sources.default",
                                          "org.apache.spark.sql.parquet")
        jmode = self._java_save_mode(mode)
        self._jdf.save(source, jmode, options)

    @property
    def schema(self):
        """Returns the schema of this :class:`DataFrame` as a :class:`types.StructType`.

        >>> df.schema
        StructType(List(StructField(age,IntegerType,true),StructField(name,StringType,true)))
        """
        if self._schema is None:
            self._schema = _parse_datatype_json_string(self._jdf.schema().json())
        return self._schema

    def printSchema(self):
        """Prints out the schema in the tree format.

        >>> df.printSchema()
        root
         |-- age: integer (nullable = true)
         |-- name: string (nullable = true)
        <BLANKLINE>
        """
        print(self._jdf.schema().treeString())

    def explain(self, extended=False):
        """Prints the (logical and physical) plans to the console for debugging purpose.

        :param extended: boolean, default ``False``. If ``False``, prints only the physical plan.

        >>> df.explain()
        PhysicalRDD [age#0,name#1], MapPartitionsRDD[...] at applySchemaToPythonRDD at\
          NativeMethodAccessorImpl.java:...

        >>> df.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        == RDD ==
        """
        if extended:
            print(self._jdf.queryExecution().toString())
        else:
            print(self._jdf.queryExecution().executedPlan().toString())

    def isLocal(self):
        """Returns ``True`` if the :func:`collect` and :func:`take` methods can be run locally
        (without any Spark executors).
        """
        return self._jdf.isLocal()

    def show(self, n=20):
        """Prints the first ``n`` rows to the console.

        >>> df
        DataFrame[age: int, name: string]
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        print(self._jdf.showString(n))

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def count(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> df.count()
        2
        """
        return int(self._jdf.count())

    @ignore_unicode_prefix
    def collect(self):
        """Returns all the records as a list of :class:`Row`.

        >>> df.collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            port = self._sc._jvm.PythonRDD.collectAndServe(self._jdf.javaToPython().rdd())
        rs = list(_load_from_socket(port, BatchedSerializer(PickleSerializer())))
        cls = _create_cls(self.schema)
        return [cls(r) for r in rs]

    @ignore_unicode_prefix
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
    def take(self, num):
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`.

        >>> df.take(2)
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        return self.limit(num).collect()

    @ignore_unicode_prefix
    def map(self, f):
        """ Returns a new :class:`RDD` by applying a the ``f`` function to each :class:`Row`.

        This is a shorthand for ``df.rdd.map()``.

        >>> df.map(lambda p: p.name).collect()
        [u'Alice', u'Bob']
        """
        return self.rdd.map(f)

    @ignore_unicode_prefix
    def flatMap(self, f):
        """ Returns a new :class:`RDD` by first applying the ``f`` function to each :class:`Row`,
        and then flattening the results.

        This is a shorthand for ``df.rdd.flatMap()``.

        >>> df.flatMap(lambda p: p.name).collect()
        [u'A', u'l', u'i', u'c', u'e', u'B', u'o', u'b']
        """
        return self.rdd.flatMap(f)

    def mapPartitions(self, f, preservesPartitioning=False):
        """Returns a new :class:`RDD` by applying the ``f`` function to each partition.

        This is a shorthand for ``df.rdd.mapPartitions()``.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(iterator): yield 1
        >>> rdd.mapPartitions(f).sum()
        4
        """
        return self.rdd.mapPartitions(f, preservesPartitioning)

    def foreach(self, f):
        """Applies the ``f`` function to all :class:`Row` of this :class:`DataFrame`.

        This is a shorthand for ``df.rdd.foreach()``.

        >>> def f(person):
        ...     print(person.name)
        >>> df.foreach(f)
        """
        return self.rdd.foreach(f)

    def foreachPartition(self, f):
        """Applies the ``f`` function to each partition of this :class:`DataFrame`.

        This a shorthand for ``df.rdd.foreachPartition()``.

        >>> def f(people):
        ...     for person in people:
        ...         print(person.name)
        >>> df.foreachPartition(f)
        """
        return self.rdd.foreachPartition(f)

    def cache(self):
        """ Persists with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self._jdf.cache()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        """Sets the storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the RDD does not have a storage level set yet.
        If no storage level is specified defaults to (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jdf.persist(javaStorageLevel)
        return self

    def unpersist(self, blocking=True):
        """Marks the :class:`DataFrame` as non-persistent, and remove all blocks for it from
        memory and disk.
        """
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    # def coalesce(self, numPartitions, shuffle=False):
    #     rdd = self._jdf.coalesce(numPartitions, shuffle, None)
    #     return DataFrame(rdd, self.sql_ctx)

    def repartition(self, numPartitions):
        """Returns a new :class:`DataFrame` that has exactly ``numPartitions`` partitions.

        >>> df.repartition(10).rdd.getNumPartitions()
        10
        """
        return DataFrame(self._jdf.repartition(numPartitions), self.sql_ctx)

    def distinct(self):
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        >>> df.distinct().count()
        2
        """
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    def sample(self, withReplacement, fraction, seed=None):
        """Returns a sampled subset of this :class:`DataFrame`.

        >>> df.sample(False, 0.5, 42).count()
        1
        """
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        rdd = self._jdf.sample(withReplacement, fraction, long(seed))
        return DataFrame(rdd, self.sql_ctx)

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
        rdd_array = self._jdf.randomSplit(_to_seq(self.sql_ctx._sc, weights), long(seed))
        return [DataFrame(rdd, self.sql_ctx) for rdd in rdd_array]

    @property
    def dtypes(self):
        """Returns all column names and their data types as a list.

        >>> df.dtypes
        [('age', 'int'), ('name', 'string')]
        """
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    @ignore_unicode_prefix
    def columns(self):
        """Returns all column names as a list.

        >>> df.columns
        [u'age', u'name']
        """
        return [f.name for f in self.schema.fields]

    @ignore_unicode_prefix
    def alias(self, alias):
        """Returns a new :class:`DataFrame` with an alias set.

        >>> from pyspark.sql.functions import *
        >>> df_as1 = df.alias("df_as1")
        >>> df_as2 = df.alias("df_as2")
        >>> joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
        >>> joined_df.select(col("df_as1.name"), col("df_as2.name"), col("df_as2.age")).collect()
        [Row(name=u'Alice', name=u'Alice', age=2), Row(name=u'Bob', name=u'Bob', age=5)]
        """
        assert isinstance(alias, basestring), "alias should be a string"
        return DataFrame(getattr(self._jdf, "as")(alias), self.sql_ctx)

    @ignore_unicode_prefix
    def join(self, other, joinExprs=None, joinType=None):
        """Joins with another :class:`DataFrame`, using the given join expression.

        The following performs a full outer join between ``df1`` and ``df2``.

        :param other: Right side of the join
        :param joinExprs: a string for join column name, or a join expression (Column).
            If joinExprs is a string indicating the name of the join column,
            the column must exist on both sides, and this performs an inner equi-join.
        :param joinType: str, default 'inner'.
            One of `inner`, `outer`, `left_outer`, `right_outer`, `semijoin`.

        >>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
        [Row(name=None, height=80), Row(name=u'Alice', height=None), Row(name=u'Bob', height=85)]

        >>> df.join(df2, 'name').select(df.name, df2.height).collect()
        [Row(name=u'Bob', height=85)]
        """

        if joinExprs is None:
            jdf = self._jdf.join(other._jdf)
        elif isinstance(joinExprs, basestring):
            jdf = self._jdf.join(other._jdf, joinExprs)
        else:
            assert isinstance(joinExprs, Column), "joinExprs should be Column"
            if joinType is None:
                jdf = self._jdf.join(other._jdf, joinExprs._jc)
            else:
                assert isinstance(joinType, basestring), "joinType should be basestring"
                jdf = self._jdf.join(other._jdf, joinExprs._jc, joinType)
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
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

        jdf = self._jdf.sort(self._jseq(jcols))
        return DataFrame(jdf, self.sql_ctx)

    orderBy = sort

    def _jseq(self, cols, converter=None):
        """Return a JVM Seq of Columns from a list of Column or names"""
        return _to_seq(self.sql_ctx._sc, cols, converter)

    def _jcols(self, *cols):
        """Return a JVM Seq of Columns from a list of Column or column names

        If `cols` has only one list in it, cols[0] will be used as the list.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        return self._jseq(cols, _to_java_column)

    def describe(self, *cols):
        """Computes statistics for numeric columns.

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical columns.

        >>> df.describe().show()
        +-------+---+
        |summary|age|
        +-------+---+
        |  count|  2|
        |   mean|3.5|
        | stddev|1.5|
        |    min|  2|
        |    max|  5|
        +-------+---+
        """
        jdf = self._jdf.describe(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
    def head(self, n=None):
        """
        Returns the first ``n`` rows as a list of :class:`Row`,
        or the first :class:`Row` if ``n`` is ``None.``

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
    def first(self):
        """Returns the first row as a :class:`Row`.

        >>> df.first()
        Row(age=2, name=u'Alice')
        """
        return self.head()

    @ignore_unicode_prefix
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
            if item not in self.columns:
                raise IndexError("no such column: %s" % item)
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

    def selectExpr(self, *expr):
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        >>> df.selectExpr("age * 2", "abs(age)").collect()
        [Row((age * 2)=4, Abs(age)=2), Row((age * 2)=10, Abs(age)=5)]
        """
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]
        jdf = self._jdf.selectExpr(self._jseq(expr))
        return DataFrame(jdf, self.sql_ctx)

    @ignore_unicode_prefix
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

    where = filter

    @ignore_unicode_prefix
    def groupBy(self, *cols):
        """Groups the :class:`DataFrame` using the specified columns,
        so we can run aggregation on them. See :class:`GroupedData`
        for all the available aggregate functions.

        :func:`groupby` is an alias for :func:`groupBy`.

        :param cols: list of columns to group by.
            Each element should be a column name (string) or an expression (:class:`Column`).

        >>> df.groupBy().avg().collect()
        [Row(AVG(age)=3.5)]
        >>> df.groupBy('name').agg({'age': 'mean'}).collect()
        [Row(name=u'Alice', AVG(age)=2.0), Row(name=u'Bob', AVG(age)=5.0)]
        >>> df.groupBy(df.name).avg().collect()
        [Row(name=u'Alice', AVG(age)=2.0), Row(name=u'Bob', AVG(age)=5.0)]
        >>> df.groupBy(['name', df.age]).count().collect()
        [Row(name=u'Bob', age=5, count=1), Row(name=u'Alice', age=2, count=1)]
        """
        jdf = self._jdf.groupBy(self._jcols(*cols))
        return GroupedData(jdf, self.sql_ctx)

    groupby = groupBy

    def agg(self, *exprs):
        """ Aggregate on the entire :class:`DataFrame` without groups
        (shorthand for ``df.groupBy.agg()``).

        >>> df.agg({"age": "max"}).collect()
        [Row(MAX(age)=5)]
        >>> from pyspark.sql import functions as F
        >>> df.agg(F.min(df.age)).collect()
        [Row(MIN(age)=2)]
        """
        return self.groupBy().agg(*exprs)

    def unionAll(self, other):
        """ Return a new :class:`DataFrame` containing union of rows in this
        frame and another frame.

        This is equivalent to `UNION ALL` in SQL.
        """
        return DataFrame(self._jdf.unionAll(other._jdf), self.sql_ctx)

    def intersect(self, other):
        """ Return a new :class:`DataFrame` containing rows only in
        both this frame and another frame.

        This is equivalent to `INTERSECT` in SQL.
        """
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    def subtract(self, other):
        """ Return a new :class:`DataFrame` containing rows in this frame
        but not in another frame.

        This is equivalent to `EXCEPT` in SQL.
        """
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    def dropna(self, how='any', thresh=None, subset=None):
        """Returns a new :class:`DataFrame` omitting rows with null values.

        This is an alias for ``na.drop()``.

        :param how: 'any' or 'all'.
            If 'any', drop a row if it contains any nulls.
            If 'all', drop a row only if all its values are null.
        :param thresh: int, default None
            If specified, drop rows that have less than `thresh` non-null values.
            This overwrites the `how` parameter.
        :param subset: optional list of column names to consider.

        >>> df4.dropna().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        +---+------+-----+

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

    def fillna(self, value, subset=None):
        """Replace null values, alias for ``na.fill()``.

        :param value: int, long, float, string, or dict.
            Value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, long, float, or string.
        :param subset: optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        >>> df4.fillna(50).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        |  5|    50|  Bob|
        | 50|    50|  Tom|
        | 50|    50| null|
        +---+------+-----+

        >>> df4.fillna({'age': 50, 'name': 'unknown'}).show()
        +---+------+-------+
        |age|height|   name|
        +---+------+-------+
        | 10|    80|  Alice|
        |  5|  null|    Bob|
        | 50|  null|    Tom|
        | 50|  null|unknown|
        +---+------+-------+

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

    def corr(self, col1, col2, method=None):
        """
        Calculates the correlation of two columns of a DataFrame as a double value. Currently only
        supports the Pearson Correlation Coefficient.
        :func:`DataFrame.corr` and :func:`DataFrameStatFunctions.corr` are aliases.

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

    def crosstab(self, col1, col2):
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency
        table. The number of distinct values for each column should be less than 1e4. The first
        column of each row will be the distinct values of `col1` and the column names will be the
        distinct values of `col2`. The name of the first column will be `$col1_$col2`. Pairs that
        have no occurrences will have `null` as their counts.
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

    def freqItems(self, cols, support=None):
        """
        Finding frequent items for columns, possibly with false positives. Using the
        frequent element count algorithm described in
        "http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou".
        :func:`DataFrame.freqItems` and :func:`DataFrameStatFunctions.freqItems` are aliases.

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
    def withColumn(self, colName, col):
        """Returns a new :class:`DataFrame` by adding a column.

        :param colName: string, name of the new column.
        :param col: a :class:`Column` expression for the new column.

        >>> df.withColumn('age2', df.age + 2).collect()
        [Row(age=2, name=u'Alice', age2=4), Row(age=5, name=u'Bob', age2=7)]
        """
        return self.select('*', col.alias(colName))

    @ignore_unicode_prefix
    def withColumnRenamed(self, existing, new):
        """REturns a new :class:`DataFrame` by renaming an existing column.

        :param existing: string, name of the existing column to rename.
        :param col: string, new name of the column.

        >>> df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
        """
        cols = [Column(_to_java_column(c)).alias(new)
                if c == existing else c
                for c in self.columns]
        return self.select(*cols)

    def toPandas(self):
        """Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This is only available if Pandas is installed and available.

        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        import pandas as pd
        return pd.DataFrame.from_records(self.collect(), columns=self.columns)


# Having SchemaRDD for backward compatibility (for docs)
class SchemaRDD(DataFrame):
    """SchemaRDD is deprecated, please use :class:`DataFrame`.
    """


def dfapi(f):
    def _api(self):
        name = f.__name__
        jdf = getattr(self._jdf, name)()
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


def df_varargs_api(f):
    def _api(self, *args):
        name = f.__name__
        jdf = getattr(self._jdf, name)(_to_seq(self.sql_ctx._sc, args))
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class GroupedData(object):
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.
    """

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

    @ignore_unicode_prefix
    def agg(self, *exprs):
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        :param exprs: a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        >>> gdf = df.groupBy(df.name)
        >>> gdf.agg({"*": "count"}).collect()
        [Row(name=u'Alice', COUNT(1)=1), Row(name=u'Bob', COUNT(1)=1)]

        >>> from pyspark.sql import functions as F
        >>> gdf.agg(F.min(df.age)).collect()
        [Row(MIN(age)=2), Row(MIN(age)=5)]
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            jdf = self._jdf.agg(exprs[0])
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            jdf = self._jdf.agg(exprs[0]._jc,
                                _to_seq(self.sql_ctx._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(jdf, self.sql_ctx)

    @dfapi
    def count(self):
        """Counts the number of records for each group.

        >>> df.groupBy(df.age).count().collect()
        [Row(age=2, count=1), Row(age=5, count=1)]
        """

    @df_varargs_api
    def mean(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().mean('age').collect()
        [Row(AVG(age)=3.5)]
        >>> df3.groupBy().mean('age', 'height').collect()
        [Row(AVG(age)=3.5, AVG(height)=82.5)]
        """

    @df_varargs_api
    def avg(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().avg('age').collect()
        [Row(AVG(age)=3.5)]
        >>> df3.groupBy().avg('age', 'height').collect()
        [Row(AVG(age)=3.5, AVG(height)=82.5)]
        """

    @df_varargs_api
    def max(self, *cols):
        """Computes the max value for each numeric columns for each group.

        >>> df.groupBy().max('age').collect()
        [Row(MAX(age)=5)]
        >>> df3.groupBy().max('age', 'height').collect()
        [Row(MAX(age)=5, MAX(height)=85)]
        """

    @df_varargs_api
    def min(self, *cols):
        """Computes the min value for each numeric column for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().min('age').collect()
        [Row(MIN(age)=2)]
        >>> df3.groupBy().min('age', 'height').collect()
        [Row(MIN(age)=2, MIN(height)=80)]
        """

    @df_varargs_api
    def sum(self, *cols):
        """Compute the sum for each numeric columns for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().sum('age').collect()
        [Row(SUM(age)=7)]
        >>> df3.groupBy().sum('age', 'height').collect()
        [Row(SUM(age)=7, SUM(height)=165)]
        """


def _create_column_from_literal(literal):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.lit(literal)


def _create_column_from_name(name):
    sc = SparkContext._active_spark_context
    return sc._jvm.functions.col(name)


def _to_java_column(col):
    if isinstance(col, Column):
        jcol = col._jc
    else:
        jcol = _create_column_from_name(col)
    return jcol


def _to_seq(sc, cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return sc._jvm.PythonUtils.toSeq(cols)


def _unary_op(name, doc="unary operator"):
    """ Create a method for given unary operator """
    def _(self):
        jc = getattr(self._jc, name)()
        return Column(jc)
    _.__doc__ = doc
    return _


def _func_op(name, doc=''):
    def _(self):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)(self._jc)
        return Column(jc)
    _.__doc__ = doc
    return _


def _bin_op(name, doc="binary operator"):
    """ Create a method for given binary operator
    """
    def _(self, other):
        jc = other._jc if isinstance(other, Column) else other
        njc = getattr(self._jc, name)(jc)
        return Column(njc)
    _.__doc__ = doc
    return _


def _reverse_op(name, doc="binary operator"):
    """ Create a method for binary operator (this object is on right side)
    """
    def _(self, other):
        jother = _create_column_from_literal(other)
        jc = getattr(jother, name)(self._jc)
        return Column(jc)
    _.__doc__ = doc
    return _


class Column(object):

    """
    A column in a DataFrame.

    :class:`Column` instances can be created by::

        # 1. Select a column out of a DataFrame

        df.colName
        df["colName"]

        # 2. Create from an expression
        df.colName + 1
        1 / df.colName
    """

    def __init__(self, jc):
        self._jc = jc

    # arithmetic operators
    __neg__ = _func_op("negate")
    __add__ = _bin_op("plus")
    __sub__ = _bin_op("minus")
    __mul__ = _bin_op("multiply")
    __div__ = _bin_op("divide")
    __truediv__ = _bin_op("divide")
    __mod__ = _bin_op("mod")
    __radd__ = _bin_op("plus")
    __rsub__ = _reverse_op("minus")
    __rmul__ = _bin_op("multiply")
    __rdiv__ = _reverse_op("divide")
    __rtruediv__ = _reverse_op("divide")
    __rmod__ = _reverse_op("mod")

    # logistic operators
    __eq__ = _bin_op("equalTo")
    __ne__ = _bin_op("notEqual")
    __lt__ = _bin_op("lt")
    __le__ = _bin_op("leq")
    __ge__ = _bin_op("geq")
    __gt__ = _bin_op("gt")

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op('and')
    __or__ = _bin_op('or')
    __invert__ = _func_op('not')
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # container operators
    __contains__ = _bin_op("contains")
    __getitem__ = _bin_op("getItem")

    def getItem(self, key):
        """An expression that gets an item at position `ordinal` out of a list,
         or gets an item by key out of a dict.

        >>> df = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])
        >>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        >>> df.select(df.l[0], df.d["key"]).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        """
        return self[key]

    def getField(self, name):
        """An expression that gets a field by name in a StructField.

        >>> from pyspark.sql import Row
        >>> df = sc.parallelize([Row(r=Row(a=1, b="b"))]).toDF()
        >>> df.select(df.r.getField("b")).show()
        +---+
        |r.b|
        +---+
        |  b|
        +---+
        >>> df.select(df.r.a).show()
        +---+
        |r.a|
        +---+
        |  1|
        +---+
        """
        return Column(self._jc.getField(name))

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        return self.getField(item)

    # string methods
    rlike = _bin_op("rlike")
    like = _bin_op("like")
    startswith = _bin_op("startsWith")
    endswith = _bin_op("endsWith")

    @ignore_unicode_prefix
    def substr(self, startPos, length):
        """
        Return a :class:`Column` which is a substring of the column

        :param startPos: start position (int or Column)
        :param length:  length of the substring (int or Column)

        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col=u'Ali'), Row(col=u'Bob')]
        """
        if type(startPos) != type(length):
            raise TypeError("Can not mix the type")
        if isinstance(startPos, (int, long)):
            jc = self._jc.substr(startPos, length)
        elif isinstance(startPos, Column):
            jc = self._jc.substr(startPos._jc, length._jc)
        else:
            raise TypeError("Unexpected type: %s" % type(startPos))
        return Column(jc)

    __getslice__ = substr

    @ignore_unicode_prefix
    def inSet(self, *cols):
        """ A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        >>> df[df.name.inSet("Bob", "Mike")].collect()
        [Row(age=5, name=u'Bob')]
        >>> df[df.age.inSet([1, 2, 3])].collect()
        [Row(age=2, name=u'Alice')]
        """
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cols[0]
        cols = [c._jc if isinstance(c, Column) else _create_column_from_literal(c) for c in cols]
        sc = SparkContext._active_spark_context
        jc = getattr(self._jc, "in")(_to_seq(sc, cols))
        return Column(jc)

    # order
    asc = _unary_op("asc", "Returns a sort expression based on the"
                           " ascending order of the given column name.")
    desc = _unary_op("desc", "Returns a sort expression based on the"
                             " descending order of the given column name.")

    isNull = _unary_op("isNull", "True if the current expression is null.")
    isNotNull = _unary_op("isNotNull", "True if the current expression is not null.")

    def alias(self, alias):
        """Return a alias for this column

        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        """
        return Column(getattr(self._jc, "as")(alias))

    @ignore_unicode_prefix
    def cast(self, dataType):
        """ Convert the column into type `dataType`

        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        """
        if isinstance(dataType, basestring):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            sc = SparkContext._active_spark_context
            ssql_ctx = sc._jvm.SQLContext(sc._jsc.sc())
            jdt = ssql_ctx.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        else:
            raise TypeError("unexpected type: %s" % type(dataType))
        return Column(jc)

    def __repr__(self):
        return 'Column<%s>' % self._jc.toString().encode('utf8')


class DataFrameNaFunctions(object):
    """Functionality for working with missing data in :class:`DataFrame`.
    """

    def __init__(self, df):
        self.df = df

    def drop(self, how='any', thresh=None, subset=None):
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def fill(self, value, subset=None):
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__


class DataFrameStatFunctions(object):
    """Functionality for statistic functions with :class:`DataFrame`.
    """

    def __init__(self, df):
        self.df = df

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


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.dataframe
    globs = pyspark.sql.dataframe.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')])\
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))
    globs['df2'] = sc.parallelize([Row(name='Tom', height=80), Row(name='Bob', height=85)]).toDF()
    globs['df3'] = sc.parallelize([Row(name='Alice', age=2, height=80),
                                  Row(name='Bob', age=5, height=85)]).toDF()

    globs['df4'] = sc.parallelize([Row(name='Alice', age=10, height=80),
                                  Row(name='Bob', age=5, height=None),
                                  Row(name='Tom', age=None, height=None),
                                  Row(name=None, age=None, height=None)]).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
