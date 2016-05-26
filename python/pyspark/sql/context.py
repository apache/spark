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

from __future__ import print_function
import sys
import warnings

if sys.version >= '3':
    basestring = unicode = str

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.session import _monkey_patch_RDD, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import Row, StringType
from pyspark.sql.utils import install_exception_handler

__all__ = ["SQLContext", "HiveContext", "UDFRegistration"]


class SQLContext(object):
    """The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.

    As of Spark 2.0, this is replaced by :class:`SparkSession`. However, we are keeping the class
    here for backward compatibility.

    A SQLContext can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    :param sparkContext: The :class:`SparkContext` backing this SQLContext.
    :param sparkSession: The :class:`SparkSession` around which this SQLContext wraps.
    :param jsqlContext: An optional JVM Scala SQLContext. If set, we do not instantiate a new
        SQLContext in the JVM, instead we make all calls to this object.
    """

    _instantiatedContext = None

    @ignore_unicode_prefix
    def __init__(self, sparkContext, sparkSession=None, jsqlContext=None):
        """Creates a new SQLContext.

        >>> from datetime import datetime
        >>> sqlContext = SQLContext(sc)
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> df = allTypes.toDF()
        >>> df.createOrReplaceTempView("allTypes")
        >>> sqlContext.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row((i + CAST(1 AS BIGINT))=2, (d + CAST(1 AS DOUBLE))=2.0, (NOT b)=False, list[1]=2, \
            dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
        >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        if sparkSession is None:
            sparkSession = SparkSession(sparkContext)
        if jsqlContext is None:
            jsqlContext = sparkSession._jwrapped
        self.sparkSession = sparkSession
        self._jsqlContext = jsqlContext
        _monkey_patch_RDD(self.sparkSession)
        install_exception_handler()
        if SQLContext._instantiatedContext is None:
            SQLContext._instantiatedContext = self

    @property
    def _ssql_ctx(self):
        """Accessor for the JVM Spark SQL context.

        Subclasses can override this property to provide their own
        JVM Contexts.
        """
        return self._jsqlContext

    @classmethod
    @since(1.6)
    def getOrCreate(cls, sc):
        """
        Get the existing SQLContext or create a new one with given SparkContext.

        :param sc: SparkContext
        """
        if cls._instantiatedContext is None:
            jsqlContext = sc._jvm.SQLContext.getOrCreate(sc._jsc.sc())
            sparkSession = SparkSession(sc, jsqlContext.sparkSession())
            cls(sc, sparkSession, jsqlContext)
        return cls._instantiatedContext

    @since(1.6)
    def newSession(self):
        """
        Returns a new SQLContext as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc, self.sparkSession.newSession())

    @since(1.3)
    def setConf(self, key, value):
        """Sets the given Spark SQL configuration property.
        """
        self.sparkSession.conf.set(key, value)

    @ignore_unicode_prefix
    @since(1.3)
    def getConf(self, key, defaultValue=None):
        """Returns the value of Spark SQL configuration property for the given key.

        If the key is not set and defaultValue is not None, return
        defaultValue. If the key is not set and defaultValue is None, return
        the system default value.

        >>> sqlContext.getConf("spark.sql.shuffle.partitions")
        u'200'
        >>> sqlContext.getConf("spark.sql.shuffle.partitions", u"10")
        u'10'
        >>> sqlContext.setConf("spark.sql.shuffle.partitions", u"50")
        >>> sqlContext.getConf("spark.sql.shuffle.partitions", u"10")
        u'50'
        """
        return self.sparkSession.conf.get(key, defaultValue)

    @property
    @since("1.3.1")
    def udf(self):
        """Returns a :class:`UDFRegistration` for UDF registration.

        :return: :class:`UDFRegistration`
        """
        return UDFRegistration(self)

    @since(1.4)
    def range(self, start, end=None, step=1, numPartitions=None):
        """
        Create a :class:`DataFrame` with single LongType column named `id`,
        containing elements in a range from `start` to `end` (exclusive) with
        step value `step`.

        :param start: the start value
        :param end: the end value (exclusive)
        :param step: the incremental step (default: 1)
        :param numPartitions: the number of partitions of the DataFrame
        :return: :class:`DataFrame`

        >>> sqlContext.range(1, 7, 2).collect()
        [Row(id=1), Row(id=3), Row(id=5)]

        If only one argument is specified, it will be used as the end value.

        >>> sqlContext.range(3).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        """
        return self.sparkSession.range(start, end, step, numPartitions)

    @ignore_unicode_prefix
    @since(1.2)
    def registerFunction(self, name, f, returnType=StringType()):
        """Registers a python function (including lambda function) as a UDF
        so it can be used in SQL statements.

        In addition to a name and the function itself, the return type can be optionally specified.
        When the return type is not given it default to a string and conversion will automatically
        be done.  For any other return type, the produced object must match the specified type.

        :param name: name of the UDF
        :param f: python function
        :param returnType: a :class:`DataType` object

        >>> sqlContext.registerFunction("stringLengthString", lambda x: len(x))
        >>> sqlContext.sql("SELECT stringLengthString('test')").collect()
        [Row(stringLengthString(test)=u'4')]

        >>> from pyspark.sql.types import IntegerType
        >>> sqlContext.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
        >>> sqlContext.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]

        >>> from pyspark.sql.types import IntegerType
        >>> sqlContext.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
        >>> sqlContext.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]
        """
        self.sparkSession.catalog.registerFunction(name, f, returnType)

    # TODO(andrew): delete this once we refactor things to take in SparkSession
    def _inferSchema(self, rdd, samplingRatio=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: StructType
        """
        return self.sparkSession._inferSchema(rdd, samplingRatio)

    @since(1.3)
    @ignore_unicode_prefix
    def createDataFrame(self, data, schema=None, samplingRatio=None):
        """
        Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.

        When ``schema`` is a list of column names, the type of each column
        will be inferred from ``data``.

        When ``schema`` is ``None``, it will try to infer the schema (column names and types)
        from ``data``, which should be an RDD of :class:`Row`,
        or :class:`namedtuple`, or :class:`dict`.

        When ``schema`` is :class:`DataType` or datatype string, it must match the real data, or
        exception will be thrown at runtime. If the given schema is not StructType, it will be
        wrapped into a StructType as its only field, and the field name will be "value", each record
        will also be wrapped into a tuple, which can be converted to row later.

        If schema inference is needed, ``samplingRatio`` is used to determined the ratio of
        rows used for schema inference. The first row will be used if ``samplingRatio`` is ``None``.

        :param data: an RDD of any kind of SQL data representation(e.g. row, tuple, int, boolean,
            etc.), or :class:`list`, or :class:`pandas.DataFrame`.
        :param schema: a :class:`DataType` or a datatype string or a list of column names, default
            is None.  The data type string format equals to `DataType.simpleString`, except that
            top level struct type can omit the `struct<>` and atomic types use `typeName()` as
            their format, e.g. use `byte` instead of `tinyint` for ByteType. We can also use `int`
            as a short name for IntegerType.
        :param samplingRatio: the sample ratio of rows used for inferring
        :return: :class:`DataFrame`

        .. versionchanged:: 2.0
           The schema parameter can be a DataType or a datatype string after 2.0. If it's not a
           StructType, it will be wrapped into a StructType and each record will also be wrapped
           into a tuple.

        >>> l = [('Alice', 1)]
        >>> sqlContext.createDataFrame(l).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> sqlContext.createDataFrame(l, ['name', 'age']).collect()
        [Row(name=u'Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> sqlContext.createDataFrame(d).collect()
        [Row(age=1, name=u'Alice')]

        >>> rdd = sc.parallelize(l)
        >>> sqlContext.createDataFrame(rdd).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> df = sqlContext.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = sqlContext.createDataFrame(person)
        >>> df2.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = sqlContext.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name=u'Alice', age=1)]

        >>> sqlContext.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name=u'Alice', age=1)]
        >>> sqlContext.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        >>> sqlContext.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a=u'Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> sqlContext.createDataFrame(rdd, "int").collect()
        [Row(value=1)]
        >>> sqlContext.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        return self.sparkSession.createDataFrame(data, schema, samplingRatio)

    @since(1.3)
    def registerDataFrameAsTable(self, df, tableName):
        """Registers the given :class:`DataFrame` as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of :class:`SQLContext`.

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        """
        df.createOrReplaceTempView(tableName)

    @since(1.6)
    def dropTempTable(self, tableName):
        """ Remove the temp table from catalog.

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> sqlContext.dropTempTable("table1")
        """
        self.sparkSession.catalog.dropTempView(tableName)

    @since(1.3)
    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        """Creates an external table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        :return: :class:`DataFrame`
        """
        return self.sparkSession.catalog.createExternalTable(
            tableName, path, source, schema, **options)

    @ignore_unicode_prefix
    @since(1.0)
    def sql(self, sqlQuery):
        """Returns a :class:`DataFrame` representing the result of the given query.

        :return: :class:`DataFrame`

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return self.sparkSession.sql(sqlQuery)

    @since(1.0)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :return: :class:`DataFrame`

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return self.sparkSession.table(tableName)

    @ignore_unicode_prefix
    @since(1.3)
    def tables(self, dbName=None):
        """Returns a :class:`DataFrame` containing names of tables in the given database.

        If ``dbName`` is not specified, the current database will be used.

        The returned DataFrame has two columns: ``tableName`` and ``isTemporary``
        (a column with :class:`BooleanType` indicating if a table is a temporary one or not).

        :param dbName: string, name of the database to use.
        :return: :class:`DataFrame`

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.tables()
        >>> df2.filter("tableName = 'table1'").first()
        Row(tableName=u'table1', isTemporary=True)
        """
        if dbName is None:
            return DataFrame(self._ssql_ctx.tables(), self)
        else:
            return DataFrame(self._ssql_ctx.tables(dbName), self)

    @since(1.3)
    def tableNames(self, dbName=None):
        """Returns a list of names of tables in the database ``dbName``.

        :param dbName: string, name of the database to use. Default to the current database.
        :return: list of table names, in string

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> "table1" in sqlContext.tableNames()
        True
        >>> "table1" in sqlContext.tableNames("default")
        True
        """
        if dbName is None:
            return [name for name in self._ssql_ctx.tableNames()]
        else:
            return [name for name in self._ssql_ctx.tableNames(dbName)]

    @since(1.0)
    def cacheTable(self, tableName):
        """Caches the specified table in-memory."""
        self._ssql_ctx.cacheTable(tableName)

    @since(1.0)
    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache."""
        self._ssql_ctx.uncacheTable(tableName)

    @since(1.3)
    def clearCache(self):
        """Removes all cached tables from the in-memory cache. """
        self._ssql_ctx.clearCache()

    @property
    @since(1.4)
    def read(self):
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        :return: :class:`DataFrameReader`
        """
        return DataFrameReader(self)

    @property
    @since(2.0)
    def streams(self):
        """Returns a :class:`ContinuousQueryManager` that allows managing all the
        :class:`ContinuousQuery` ContinuousQueries active on `this` context.
        """
        from pyspark.sql.streaming import ContinuousQueryManager
        return ContinuousQueryManager(self._ssql_ctx.streams())


class HiveContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in Hive.

    Configuration for Hive is read from ``hive-site.xml`` on the classpath.
    It supports running both SQL and HiveQL commands.

    :param sparkContext: The SparkContext to wrap.
    :param jhiveContext: An optional JVM Scala HiveContext. If set, we do not instantiate a new
        :class:`HiveContext` in the JVM, instead we make all calls to this object.

    .. note:: Deprecated in 2.0.0. Use SparkSession.builder.enableHiveSupport().getOrCreate().
    """

    warnings.warn(
        "HiveContext is deprecated in Spark 2.0.0. Please use " +
        "SparkSession.builder.enableHiveSupport().getOrCreate() instead.",
        DeprecationWarning)

    def __init__(self, sparkContext, jhiveContext=None):
        if jhiveContext is None:
            sparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
        else:
            sparkSession = SparkSession(sparkContext, jhiveContext.sparkSession())
        SQLContext.__init__(self, sparkContext, sparkSession, jhiveContext)

    @classmethod
    def _createForTesting(cls, sparkContext):
        """(Internal use only) Create a new HiveContext for testing.

        All test code that touches HiveContext *must* go through this method. Otherwise,
        you may end up launching multiple derby instances and encounter with incredibly
        confusing error messages.
        """
        jsc = sparkContext._jsc.sc()
        jtestHive = sparkContext._jvm.org.apache.spark.sql.hive.test.TestHiveContext(jsc)
        return cls(sparkContext, jtestHive)

    def refreshTable(self, tableName):
        """Invalidate and refresh all the cached the metadata of the given
        table. For performance reasons, Spark SQL or the external data source
        library it uses might cache certain metadata about a table, such as the
        location of blocks. When those change outside of Spark SQL, users should
        call this function to invalidate the cache.
        """
        self._ssql_ctx.refreshTable(tableName)


class UDFRegistration(object):
    """Wrapper for user-defined function registration."""

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext

    def register(self, name, f, returnType=StringType()):
        return self.sqlContext.registerFunction(name, f, returnType)

    register.__doc__ = SQLContext.registerFunction.__doc__


def _test():
    import os
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.context

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.context.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['rdd'] = rdd = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")]
    )
    globs['df'] = rdd.toDF()
    jsonStrings = [
        '{"field1": 1, "field2": "row1", "field3":{"field4":11}}',
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]},'
        '"field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", '
        '"field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    globs['json'] = sc.parallelize(jsonStrings)
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.context, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
