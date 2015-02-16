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
import itertools
import warnings
import random
import os
from tempfile import NamedTemporaryFile

from py4j.java_collections import ListConverter, MapConverter

from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.sql.types import *
from pyspark.sql.types import _create_cls, _parse_datatype_json_string


__all__ = ["DataFrame", "GroupedData", "Column", "SchemaRDD"]


class DataFrame(object):

    """A collection of rows that have the same columns.

    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SQLContext`::

        people = sqlContext.parquetFile("...")

    Once created, it can be manipulated using the various domain-specific-language
    (DSL) functions defined in: :class:`DataFrame`, :class:`Column`.

    To select a column from the data frame, use the apply method::

        ageCol = people.age

    Note that the :class:`Column` type can also be manipulated
    through its various functions::

        # The following creates a new column that increases everybody's age by 10.
        people.age + 10


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

    @property
    def rdd(self):
        """
        Return the content of the :class:`DataFrame` as an :class:`RDD`
        of :class:`Row` s.
        """
        if not hasattr(self, '_lazy_rdd'):
            jrdd = self._jdf.javaToPython()
            rdd = RDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()))
            schema = self.schema

            def applySchema(it):
                cls = _create_cls(schema)
                return itertools.imap(cls, it)

            self._lazy_rdd = rdd.mapPartitions(applySchema)

        return self._lazy_rdd

    def toJSON(self, use_unicode=False):
        """Convert a DataFrame into a MappedRDD of JSON documents; one document per row.

        >>> df.toJSON().first()
        '{"age":2,"name":"Alice"}'
        """
        rdd = self._jdf.toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

    def saveAsParquetFile(self, path):
        """Save the contents as a Parquet file, preserving the schema.

        Files that are written out using this method can be read back in as
        a DataFrame using the L{SQLContext.parquetFile} method.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> df.saveAsParquetFile(parquetFile)
        >>> df2 = sqlCtx.parquetFile(parquetFile)
        >>> sorted(df2.collect()) == sorted(df.collect())
        True
        """
        self._jdf.saveAsParquetFile(path)

    def registerTempTable(self, name):
        """Registers this RDD as a temporary table using the given name.

        The lifetime of this temporary table is tied to the L{SQLContext}
        that was used to create this DataFrame.

        >>> df.registerTempTable("people")
        >>> df2 = sqlCtx.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        self._jdf.registerTempTable(name)

    def registerAsTable(self, name):
        """DEPRECATED: use registerTempTable() instead"""
        warnings.warn("Use registerTempTable instead of registerAsTable.", DeprecationWarning)
        self.registerTempTable(name)

    def insertInto(self, tableName, overwrite=False):
        """Inserts the contents of this DataFrame into the specified table.

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

    def saveAsTable(self, tableName, source=None, mode="append", **options):
        """Saves the contents of the DataFrame to a data source as a table.

        The data source is specified by the `source` and a set of `options`.
        If `source` is not specified, the default data source configured by
        spark.sql.sources.default will be used.

        Additionally, mode is used to specify the behavior of the saveAsTable operation when
        table already exists in the data source. There are four modes:

        * append: Contents of this DataFrame are expected to be appended to existing table.
        * overwrite: Data in the existing table is expected to be overwritten by the contents of \
            this DataFrame.
        * error: An exception is expected to be thrown.
        * ignore: The save operation is expected to not save the contents of the DataFrame and \
            to not change the existing table.
        """
        if source is None:
            source = self.sql_ctx.getConf("spark.sql.sources.default",
                                          "org.apache.spark.sql.parquet")
        jmode = self._java_save_mode(mode)
        joptions = MapConverter().convert(options,
                                          self.sql_ctx._sc._gateway._gateway_client)
        self._jdf.saveAsTable(tableName, source, jmode, joptions)

    def save(self, path=None, source=None, mode="append", **options):
        """Saves the contents of the DataFrame to a data source.

        The data source is specified by the `source` and a set of `options`.
        If `source` is not specified, the default data source configured by
        spark.sql.sources.default will be used.

        Additionally, mode is used to specify the behavior of the save operation when
        data already exists in the data source. There are four modes:

        * append: Contents of this DataFrame are expected to be appended to existing data.
        * overwrite: Existing data is expected to be overwritten by the contents of this DataFrame.
        * error: An exception is expected to be thrown.
        * ignore: The save operation is expected to not save the contents of the DataFrame and \
            to not change the existing data.
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self.sql_ctx.getConf("spark.sql.sources.default",
                                          "org.apache.spark.sql.parquet")
        jmode = self._java_save_mode(mode)
        joptions = MapConverter().convert(options,
                                          self._sc._gateway._gateway_client)
        self._jdf.save(source, jmode, joptions)

    @property
    def schema(self):
        """Returns the schema of this DataFrame (represented by
        a L{StructType}).

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
        print (self._jdf.schema().treeString())

    def show(self):
        """
        Print the first 20 rows.

        >>> df.show()
        age name
        2   Alice
        5   Bob
        >>> df
        age name
        2   Alice
        5   Bob
        """
        print (self)

    def __repr__(self):
        return self._jdf.showString()

    def count(self):
        """Return the number of elements in this RDD.

        Unlike the base RDD implementation of count, this implementation
        leverages the query optimizer to compute the count on the DataFrame,
        which supports features such as filter pushdown.

        >>> df.count()
        2L
        """
        return self._jdf.count()

    def collect(self):
        """Return a list that contains all of the rows.

        Each object in the list is a Row, the fields can be accessed as
        attributes.

        >>> df.collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            bytesInJava = self._jdf.javaToPython().collect().iterator()
        tempFile = NamedTemporaryFile(delete=False, dir=self._sc._temp_dir)
        tempFile.close()
        self._sc._writeToFile(bytesInJava, tempFile.name)
        # Read the data into Python and deserialize it:
        with open(tempFile.name, 'rb') as tempFile:
            rs = list(BatchedSerializer(PickleSerializer()).load_stream(tempFile))
        os.unlink(tempFile.name)
        cls = _create_cls(self.schema)
        return [cls(r) for r in rs]

    def limit(self, num):
        """Limit the result count to the number specified.

        >>> df.limit(1).collect()
        [Row(age=2, name=u'Alice')]
        >>> df.limit(0).collect()
        []
        """
        jdf = self._jdf.limit(num)
        return DataFrame(jdf, self.sql_ctx)

    def take(self, num):
        """Take the first num rows of the RDD.

        Each object in the list is a Row, the fields can be accessed as
        attributes.

        >>> df.take(2)
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        """
        return self.limit(num).collect()

    def map(self, f):
        """ Return a new RDD by applying a function to each Row

        It's a shorthand for df.rdd.map()

        >>> df.map(lambda p: p.name).collect()
        [u'Alice', u'Bob']
        """
        return self.rdd.map(f)

    def flatMap(self, f):
        """ Return a new RDD by first applying a function to all elements of this,
        and then flattening the results.

        It's a shorthand for df.rdd.flatMap()

        >>> df.flatMap(lambda p: p.name).collect()
        [u'A', u'l', u'i', u'c', u'e', u'B', u'o', u'b']
        """
        return self.rdd.flatMap(f)

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each partition.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(iterator): yield 1
        >>> rdd.mapPartitions(f).sum()
        4
        """
        return self.rdd.mapPartitions(f, preservesPartitioning)

    def cache(self):
        """ Persist with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self._jdf.cache()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        """ Set the storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the RDD does not have a storage level set yet.
        If no storage level is specified defaults to (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jdf.persist(javaStorageLevel)
        return self

    def unpersist(self, blocking=True):
        """ Mark it as non-persistent, and remove all blocks for it from
        memory and disk.
        """
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    # def coalesce(self, numPartitions, shuffle=False):
    #     rdd = self._jdf.coalesce(numPartitions, shuffle, None)
    #     return DataFrame(rdd, self.sql_ctx)

    def repartition(self, numPartitions):
        """ Return a new :class:`DataFrame` that has exactly `numPartitions`
        partitions.
        """
        rdd = self._jdf.repartition(numPartitions, None)
        return DataFrame(rdd, self.sql_ctx)

    def sample(self, withReplacement, fraction, seed=None):
        """
        Return a sampled subset of this DataFrame.

        >>> df.sample(False, 0.5, 97).count()
        1L
        """
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        seed = seed if seed is not None else random.randint(0, sys.maxint)
        rdd = self._jdf.sample(withReplacement, fraction, long(seed))
        return DataFrame(rdd, self.sql_ctx)

    @property
    def dtypes(self):
        """Return all column names and their data types as a list.

        >>> df.dtypes
        [('age', 'int'), ('name', 'string')]
        """
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self):
        """ Return all column names as a list.

        >>> df.columns
        [u'age', u'name']
        """
        return [f.name for f in self.schema.fields]

    def join(self, other, joinExprs=None, joinType=None):
        """
        Join with another DataFrame, using the given join expression.
        The following performs a full outer join between `df1` and `df2`::

        :param other: Right side of the join
        :param joinExprs: Join expression
        :param joinType: One of `inner`, `outer`, `left_outer`, `right_outer`, `semijoin`.

        >>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
        [Row(name=None, height=80), Row(name=u'Bob', height=85), Row(name=u'Alice', height=None)]
        """

        if joinExprs is None:
            jdf = self._jdf.join(other._jdf)
        else:
            assert isinstance(joinExprs, Column), "joinExprs should be Column"
            if joinType is None:
                jdf = self._jdf.join(other._jdf, joinExprs._jc)
            else:
                assert isinstance(joinType, basestring), "joinType should be basestring"
                jdf = self._jdf.join(other._jdf, joinExprs._jc, joinType)
        return DataFrame(jdf, self.sql_ctx)

    def sort(self, *cols):
        """ Return a new :class:`DataFrame` sorted by the specified column.

        :param cols: The columns or expressions used for sorting

        >>> df.sort(df.age.desc()).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        >>> df.sortBy(df.age.desc()).collect()
        [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
        """
        if not cols:
            raise ValueError("should sort by at least one column")
        jcols = ListConverter().convert([_to_java_column(c) for c in cols],
                                        self._sc._gateway._gateway_client)
        jdf = self._jdf.sort(self._sc._jvm.PythonUtils.toSeq(jcols))
        return DataFrame(jdf, self.sql_ctx)

    sortBy = sort

    def head(self, n=None):
        """ Return the first `n` rows or the first row if n is None.

        >>> df.head()
        Row(age=2, name=u'Alice')
        >>> df.head(1)
        [Row(age=2, name=u'Alice')]
        """
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    def first(self):
        """ Return the first row.

        >>> df.first()
        Row(age=2, name=u'Alice')
        """
        return self.head()

    def __getitem__(self, item):
        """ Return the column by given name

        >>> df['age'].collect()
        [Row(age=2), Row(age=5)]
        >>> df[ ["name", "age"]].collect()
        [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
        >>> df[ df.age > 3 ].collect()
        [Row(age=5, name=u'Bob')]
        """
        if isinstance(item, basestring):
            jc = self._jdf.apply(item)
            return Column(jc, self.sql_ctx)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, list):
            return self.select(*item)
        else:
            raise IndexError("unexpected index: %s" % item)

    def __getattr__(self, name):
        """ Return the column by given name

        >>> df.age.collect()
        [Row(age=2), Row(age=5)]
        """
        if name.startswith("__"):
            raise AttributeError(name)
        jc = self._jdf.apply(name)
        return Column(jc, self.sql_ctx)

    def select(self, *cols):
        """ Selecting a set of expressions.

        >>> df.select().collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        >>> df.select('*').collect()
        [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
        >>> df.select('name', 'age').collect()
        [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
        >>> df.select(df.name, (df.age + 10).alias('age')).collect()
        [Row(name=u'Alice', age=12), Row(name=u'Bob', age=15)]
        """
        if not cols:
            cols = ["*"]
        jcols = ListConverter().convert([_to_java_column(c) for c in cols],
                                        self._sc._gateway._gateway_client)
        jdf = self._jdf.select(self.sql_ctx._sc._jvm.PythonUtils.toSeq(jcols))
        return DataFrame(jdf, self.sql_ctx)

    def selectExpr(self, *expr):
        """
        Selects a set of SQL expressions. This is a variant of
        `select` that accepts SQL expressions.

        >>> df.selectExpr("age * 2", "abs(age)").collect()
        [Row((age * 2)=4, Abs(age)=2), Row((age * 2)=10, Abs(age)=5)]
        """
        jexpr = ListConverter().convert(expr, self._sc._gateway._gateway_client)
        jdf = self._jdf.selectExpr(self._sc._jvm.PythonUtils.toSeq(jexpr))
        return DataFrame(jdf, self.sql_ctx)

    def filter(self, condition):
        """ Filtering rows using the given condition, which could be
        Column expression or string of SQL expression.

        where() is an alias for filter().

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

    def groupBy(self, *cols):
        """ Group the :class:`DataFrame` using the specified columns,
        so we can run aggregation on them. See :class:`GroupedData`
        for all the available aggregate functions.

        >>> df.groupBy().avg().collect()
        [Row(AVG(age#0)=3.5)]
        >>> df.groupBy('name').agg({'age': 'mean'}).collect()
        [Row(name=u'Bob', AVG(age#0)=5.0), Row(name=u'Alice', AVG(age#0)=2.0)]
        >>> df.groupBy(df.name).avg().collect()
        [Row(name=u'Bob', AVG(age#0)=5.0), Row(name=u'Alice', AVG(age#0)=2.0)]
        """
        jcols = ListConverter().convert([_to_java_column(c) for c in cols],
                                        self._sc._gateway._gateway_client)
        jdf = self._jdf.groupBy(self.sql_ctx._sc._jvm.PythonUtils.toSeq(jcols))
        return GroupedData(jdf, self.sql_ctx)

    def agg(self, *exprs):
        """ Aggregate on the entire :class:`DataFrame` without groups
        (shorthand for df.groupBy.agg()).

        >>> df.agg({"age": "max"}).collect()
        [Row(MAX(age#0)=5)]
        >>> from pyspark.sql import functions as F
        >>> df.agg(F.min(df.age)).collect()
        [Row(MIN(age#0)=2)]
        """
        return self.groupBy().agg(*exprs)

    def unionAll(self, other):
        """ Return a new DataFrame containing union of rows in this
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

    def withColumn(self, colName, col):
        """ Return a new :class:`DataFrame` by adding a column.

        >>> df.withColumn('age2', df.age + 2).collect()
        [Row(age=2, name=u'Alice', age2=4), Row(age=5, name=u'Bob', age2=7)]
        """
        return self.select('*', col.alias(colName))

    def withColumnRenamed(self, existing, new):
        """ Rename an existing column to a new name

        >>> df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
        """
        cols = [Column(_to_java_column(c), self.sql_ctx).alias(new)
                if c == existing else c
                for c in self.columns]
        return self.select(*cols)

    def toPandas(self):
        """
        Collect all the rows and return a `pandas.DataFrame`.

        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        import pandas as pd
        return pd.DataFrame.from_records(self.collect(), columns=self.columns)


# Having SchemaRDD for backward compatibility (for docs)
class SchemaRDD(DataFrame):
    """
    SchemaRDD is deprecated, please use DataFrame
    """


def dfapi(f):
    def _api(self):
        name = f.__name__
        jdf = getattr(self._jdf, name)()
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class GroupedData(object):

    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by DataFrame.groupBy().
    """

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

    def agg(self, *exprs):
        """ Compute aggregates by specifying a map from column name
        to aggregate methods.

        The available aggregate methods are `avg`, `max`, `min`,
        `sum`, `count`.

        :param exprs: list or aggregate columns or a map from column
                      name to aggregate methods.

        >>> gdf = df.groupBy(df.name)
        >>> gdf.agg({"*": "count"}).collect()
        [Row(name=u'Bob', COUNT(1)=1), Row(name=u'Alice', COUNT(1)=1)]

        >>> from pyspark.sql import functions as F
        >>> gdf.agg(F.min(df.age)).collect()
        [Row(MIN(age#0)=5), Row(MIN(age#0)=2)]
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            jmap = MapConverter().convert(exprs[0],
                                          self.sql_ctx._sc._gateway._gateway_client)
            jdf = self._jdf.agg(jmap)
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            jcols = ListConverter().convert([c._jc for c in exprs[1:]],
                                            self.sql_ctx._sc._gateway._gateway_client)
            jdf = self._jdf.agg(exprs[0]._jc, self.sql_ctx._sc._jvm.PythonUtils.toSeq(jcols))
        return DataFrame(jdf, self.sql_ctx)

    @dfapi
    def count(self):
        """ Count the number of rows for each group.

        >>> df.groupBy(df.age).count().collect()
        [Row(age=2, count=1), Row(age=5, count=1)]
        """

    @dfapi
    def mean(self):
        """Compute the average value for each numeric columns
        for each group. This is an alias for `avg`."""

    @dfapi
    def avg(self):
        """Compute the average value for each numeric columns
        for each group."""

    @dfapi
    def max(self):
        """Compute the max value for each numeric columns for
        each group. """

    @dfapi
    def min(self):
        """Compute the min value for each numeric column for
        each group."""

    @dfapi
    def sum(self):
        """Compute the sum for each numeric columns for each
        group."""


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


def _unary_op(name, doc="unary operator"):
    """ Create a method for given unary operator """
    def _(self):
        jc = getattr(self._jc, name)()
        return Column(jc, self.sql_ctx)
    _.__doc__ = doc
    return _


def _func_op(name, doc=''):
    def _(self):
        jc = getattr(self._sc._jvm.functions, name)(self._jc)
        return Column(jc, self.sql_ctx)
    _.__doc__ = doc
    return _


def _bin_op(name, doc="binary operator"):
    """ Create a method for given binary operator
    """
    def _(self, other):
        jc = other._jc if isinstance(other, Column) else other
        njc = getattr(self._jc, name)(jc)
        return Column(njc, self.sql_ctx)
    _.__doc__ = doc
    return _


def _reverse_op(name, doc="binary operator"):
    """ Create a method for binary operator (this object is on right side)
    """
    def _(self, other):
        jother = _create_column_from_literal(other)
        jc = getattr(jother, name)(self._jc)
        return Column(jc, self.sql_ctx)
    _.__doc__ = doc
    return _


class Column(DataFrame):

    """
    A column in a DataFrame.

    `Column` instances can be created by::

        # 1. Select a column out of a DataFrame
        df.colName
        df["colName"]

        # 2. Create from an expression
        df.colName + 1
        1 / df.colName
    """

    def __init__(self, jc, sql_ctx=None):
        self._jc = jc
        super(Column, self).__init__(jc, sql_ctx)

    # arithmetic operators
    __neg__ = _func_op("negate")
    __add__ = _bin_op("plus")
    __sub__ = _bin_op("minus")
    __mul__ = _bin_op("multiply")
    __div__ = _bin_op("divide")
    __mod__ = _bin_op("mod")
    __radd__ = _bin_op("plus")
    __rsub__ = _reverse_op("minus")
    __rmul__ = _bin_op("multiply")
    __rdiv__ = _reverse_op("divide")
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
    getField = _bin_op("getField", "An expression that gets a field by name in a StructField.")

    # string methods
    rlike = _bin_op("rlike")
    like = _bin_op("like")
    startswith = _bin_op("startsWith")
    endswith = _bin_op("endsWith")

    def substr(self, startPos, length):
        """
        Return a Column which is a substring of the column

        :param startPos: start position (int or Column)
        :param length:  length of the substring (int or Column)

        >>> df.name.substr(1, 3).collect()
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
        return Column(jc, self.sql_ctx)

    __getslice__ = substr

    # order
    asc = _unary_op("asc")
    desc = _unary_op("desc")

    isNull = _unary_op("isNull", "True if the current expression is null.")
    isNotNull = _unary_op("isNotNull", "True if the current expression is not null.")

    def alias(self, alias):
        """Return a alias for this column

        >>> df.age.alias("age2").collect()
        [Row(age2=2), Row(age2=5)]
        """
        return Column(getattr(self._jc, "as")(alias), self.sql_ctx)

    def cast(self, dataType):
        """ Convert the column into type `dataType`

        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        """
        if self.sql_ctx is None:
            sc = SparkContext._active_spark_context
            ssql_ctx = sc._jvm.SQLContext(sc._jsc.sc())
        else:
            ssql_ctx = self.sql_ctx._ssql_ctx
        if isinstance(dataType, basestring):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            jdt = ssql_ctx.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        return Column(jc, self.sql_ctx)

    def __repr__(self):
        if self._jdf.isComputable():
            return self._jdf.samples()
        else:
            return 'Column<%s>' % self._jdf.toString()

    def toPandas(self):
        """
        Return a pandas.Series from the column

        >>> df.age.toPandas()  # doctest: +SKIP
        0    2
        1    5
        dtype: int64
        """
        import pandas as pd
        data = [c for c, in self.collect()]
        return pd.Series(data)


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.dataframe
    globs = pyspark.sql.dataframe.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    globs['df'] = sc.parallelize([Row(name='Alice', age=2), Row(name='Bob', age=5)]).toDF()
    globs['df2'] = sc.parallelize([Row(name='Tom', height=80), Row(name='Bob', height=85)]).toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
