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
from typing import (
    Optional,
    Union,
    Callable,
    Any,
    Iterable,
    List,
    Tuple,
    overload,
    Type,
    ClassVar,
    TYPE_CHECKING, cast
)

from py4j.java_gateway import JavaObject  # type: ignore[import]

from pyspark import since, _NoValue  # type: ignore[attr-defined]
from pyspark.sql.session import _monkey_patch_RDD, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.udf import UDFRegistration  # noqa: F401
from pyspark.sql.utils import install_exception_handler
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.sql.types import AtomicType, DataType, StructType
from pyspark.sql.streaming import StreamingQueryManager
from pyspark.conf import SparkConf

if TYPE_CHECKING:
    from pyspark.sql._typing import (
        AtomicValue,
        RowLike,
        UserDefinedFunctionLike,
    )
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["SQLContext", "HiveContext"]


# TODO: ignore[attr-defined] will be removed, once SparkContext is inlined
class SQLContext(object):
    """The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.

    As of Spark 2.0, this is replaced by :class:`SparkSession`. However, we are keeping the class
    here for backward compatibility.

    A SQLContext can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    .. deprecated:: 3.0.0
        Use :func:`SparkSession.builder.getOrCreate()` instead.

    Parameters
    ----------
    sparkContext : :class:`SparkContext`
        The :class:`SparkContext` backing this SQLContext.
    sparkSession : :class:`SparkSession`
        The :class:`SparkSession` around which this SQLContext wraps.
    jsqlContext : optional
        An optional JVM Scala SQLContext. If set, we do not instantiate a new
        SQLContext in the JVM, instead we make all calls to this object.
        This is only for internal.

    Examples
    --------
    >>> from datetime import datetime
    >>> from pyspark.sql import Row
    >>> sqlContext = SQLContext(sc)
    >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
    ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
    ...     time=datetime(2014, 8, 1, 14, 1, 5))])
    >>> df = allTypes.toDF()
    >>> df.createOrReplaceTempView("allTypes")
    >>> sqlContext.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
    ...            'from allTypes where b and i > 0').collect()
    [Row((i + 1)=2, (d + 1)=2.0, (NOT b)=False, list[1]=2, \
        dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
    >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
    [(1, 'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
    """

    _instantiatedContext: ClassVar[Optional["SQLContext"]] = None

    def __init__(
        self,
        sparkContext: SparkContext,
        sparkSession: Optional[SparkSession] = None,
        jsqlContext: Optional[JavaObject] = None
    ):
        if sparkSession is None:
            warnings.warn(
                "Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.",
                FutureWarning
            )

        self._sc = sparkContext
        self._jsc = self._sc._jsc  # type: ignore[attr-defined]
        self._jvm = self._sc._jvm  # type: ignore[attr-defined]
        if sparkSession is None:
            sparkSession = SparkSession.builder.getOrCreate()
        if jsqlContext is None:
            jsqlContext = sparkSession._jwrapped
        self.sparkSession = sparkSession
        self._jsqlContext = jsqlContext
        _monkey_patch_RDD(self.sparkSession)
        install_exception_handler()
        if (SQLContext._instantiatedContext is None
                or SQLContext._instantiatedContext._sc._jsc is None):  # type: ignore[attr-defined]
            SQLContext._instantiatedContext = self

    @property
    def _ssql_ctx(self) -> JavaObject:
        """Accessor for the JVM Spark SQL context.

        Subclasses can override this property to provide their own
        JVM Contexts.
        """
        return self._jsqlContext

    @property
    def _conf(self) -> SparkConf:
        """Accessor for the JVM SQL-specific configurations"""
        return self.sparkSession._jsparkSession.sessionState().conf()

    @classmethod
    def getOrCreate(cls: Type["SQLContext"], sc: SparkContext) -> "SQLContext":
        """
        Get the existing SQLContext or create a new one with given SparkContext.

        .. versionadded:: 1.6.0

        .. deprecated:: 3.0.0
            Use :func:`SparkSession.builder.getOrCreate()` instead.

        Parameters
        ----------
        sc : :class:`SparkContext`
        """
        warnings.warn(
            "Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.",
            FutureWarning
        )

        if (cls._instantiatedContext is None
                or SQLContext._instantiatedContext._sc._jsc is None):  # type: ignore[union-attr]
            jsqlContext = sc._jvm.SparkSession.builder().sparkContext(  # type: ignore[attr-defined]
                sc._jsc.sc()).getOrCreate().sqlContext()  # type: ignore[attr-defined]
            sparkSession = SparkSession(sc, jsqlContext.sparkSession())
            cls(sc, sparkSession, jsqlContext)
        return cast(SQLContext, cls._instantiatedContext)

    def newSession(self) -> "SQLContext":
        """
        Returns a new SQLContext as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.

        .. versionadded:: 1.6.0
        """
        return self.__class__(self._sc, self.sparkSession.newSession())

    def setConf(self, key: str, value: Union[bool, int, str]) -> None:
        """Sets the given Spark SQL configuration property.

        .. versionadded:: 1.3.0
        """
        self.sparkSession.conf.set(key, value)  # type: ignore[arg-type]

    def getConf(self, key: str, defaultValue: Optional[str] = _NoValue) -> str:
        """Returns the value of Spark SQL configuration property for the given key.

        If the key is not set and defaultValue is set, return
        defaultValue. If the key is not set and defaultValue is not set, return
        the system default value.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> sqlContext.getConf("spark.sql.shuffle.partitions")
        '200'
        >>> sqlContext.getConf("spark.sql.shuffle.partitions", "10")
        '10'
        >>> sqlContext.setConf("spark.sql.shuffle.partitions", "50")
        >>> sqlContext.getConf("spark.sql.shuffle.partitions", "10")
        '50'
        """
        return self.sparkSession.conf.get(key, defaultValue)

    @property
    def udf(self) -> UDFRegistration:
        """Returns a :class:`UDFRegistration` for UDF registration.

        .. versionadded:: 1.3.1

        Returns
        -------
        :class:`UDFRegistration`
        """
        return self.sparkSession.udf

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None
    ) -> DataFrame:
        """
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        start : int
            the start value
        end : int, optional
            the end value (exclusive)
        step : int, optional
            the incremental step (default: 1)
        numPartitions : int, optional
            the number of partitions of the DataFrame

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> sqlContext.range(1, 7, 2).collect()
        [Row(id=1), Row(id=3), Row(id=5)]

        If only one argument is specified, it will be used as the end value.

        >>> sqlContext.range(3).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        """
        return self.sparkSession.range(start, end, step, numPartitions)

    def registerFunction(
        self,
        name: str,
        f: Callable[..., Any],
        returnType: Optional[DataType] = None
    ) -> "UserDefinedFunctionLike":
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 1.2.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.register` instead.
        """
        warnings.warn(
            "Deprecated in 2.3.0. Use spark.udf.register instead.",
            FutureWarning
        )
        return self.sparkSession.udf.register(name, f, returnType)

    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: Optional[DataType] = None
    ) -> None:
        """An alias for :func:`spark.udf.registerJavaFunction`.
        See :meth:`pyspark.sql.UDFRegistration.registerJavaFunction`.

        .. versionadded:: 2.1.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.registerJavaFunction` instead.
        """
        warnings.warn(
            "Deprecated in 2.3.0. Use spark.udf.registerJavaFunction instead.",
            FutureWarning
        )
        return self.sparkSession.udf.registerJavaFunction(name, javaClassName, returnType)

    # TODO(andrew): delete this once we refactor things to take in SparkSession
    def _inferSchema(self, rdd: RDD, samplingRatio: Optional[float] = None) -> StructType:
        """
        Infer schema from an RDD of Row or tuple.

        Parameters
        ----------
        rdd : :class:`RDD`
            an RDD of Row or tuple
        samplingRatio : float, optional
            sampling ratio, or no sampling (default)

        Returns
        -------
        :class:`pyspark.sql.types.StructType`
        """
        return self.sparkSession._inferSchema(rdd, samplingRatio)

    @overload
    def createDataFrame(
        self,
        data: Union["RDD[RowLike]", Iterable["RowLike"]],
        schema: Union[List[str], Tuple[str, ...]] = ...,
        samplingRatio: Optional[float] = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: Union["RDD[RowLike]", Iterable["RowLike"]],
        schema: Union[StructType, str],
        *,
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: Union[
            "RDD[AtomicValue]",
            Iterable["AtomicValue"],
        ],
        schema: Union[AtomicType, str],
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self, data: "PandasDataFrameLike", samplingRatio: Optional[float] = ...
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: "PandasDataFrameLike",
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    def createDataFrame(  # type: ignore[misc]
        self,
        data: Union["RDD[Any]", Iterable[Any], "PandasDataFrameLike"],
        schema: Optional[Union[AtomicType, StructType, str]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True
    ) -> DataFrame:
        """
        Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.

        When ``schema`` is a list of column names, the type of each column
        will be inferred from ``data``.

        When ``schema`` is ``None``, it will try to infer the schema (column names and types)
        from ``data``, which should be an RDD of :class:`Row`,
        or :class:`namedtuple`, or :class:`dict`.

        When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string it must match
        the real data, or an exception will be thrown at runtime. If the given schema is not
        :class:`pyspark.sql.types.StructType`, it will be wrapped into a
        :class:`pyspark.sql.types.StructType` as its only field, and the field name will be "value",
        each record will also be wrapped into a tuple, which can be converted to row later.

        If schema inference is needed, ``samplingRatio`` is used to determined the ratio of
        rows used for schema inference. The first row will be used if ``samplingRatio`` is ``None``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 2.0.0
           The ``schema`` parameter can be a :class:`pyspark.sql.types.DataType` or a
           datatype string after 2.0.
           If it's not a :class:`pyspark.sql.types.StructType`, it will be wrapped into a
           :class:`pyspark.sql.types.StructType` and each record will also be wrapped into a tuple.

        .. versionchanged:: 2.1.0
           Added verifySchema.

        Parameters
        ----------
        data : :class:`RDD` or iterable
            an RDD of any kind of SQL data representation (:class:`Row`,
            :class:`tuple`, ``int``, ``boolean``, etc.), or :class:`list`, or
            :class:`pandas.DataFrame`.
        schema : :class:`pyspark.sql.types.DataType`, str or list, optional
            a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is None.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
            ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`.
            We can also use ``int`` as a short name for :class:`pyspark.sql.types.IntegerType`.
        samplingRatio : float, optional
            the sample ratio of rows used for inferring
        verifySchema : bool, optional
            verify data types of every row against schema. Enabled by default.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> l = [('Alice', 1)]
        >>> sqlContext.createDataFrame(l).collect()
        [Row(_1='Alice', _2=1)]
        >>> sqlContext.createDataFrame(l, ['name', 'age']).collect()
        [Row(name='Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> sqlContext.createDataFrame(d).collect()
        [Row(age=1, name='Alice')]

        >>> rdd = sc.parallelize(l)
        >>> sqlContext.createDataFrame(rdd).collect()
        [Row(_1='Alice', _2=1)]
        >>> df = sqlContext.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name='Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = sqlContext.createDataFrame(person)
        >>> df2.collect()
        [Row(name='Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = sqlContext.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name='Alice', age=1)]

        >>> sqlContext.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name='Alice', age=1)]
        >>> sqlContext.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        >>> sqlContext.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a='Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> sqlContext.createDataFrame(rdd, "int").collect()
        [Row(value=1)]
        >>> sqlContext.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        return self.sparkSession.createDataFrame(  # type: ignore[call-overload]
            data, schema, samplingRatio, verifySchema)

    def registerDataFrameAsTable(self, df: DataFrame, tableName: str) -> None:
        """Registers the given :class:`DataFrame` as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of :class:`SQLContext`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        """
        df.createOrReplaceTempView(tableName)

    def dropTempTable(self, tableName: str) -> None:
        """ Remove the temporary table from catalog.

        .. versionadded:: 1.6.0

        Examples
        --------
        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> sqlContext.dropTempTable("table1")
        """
        self.sparkSession.catalog.dropTempView(tableName)

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str
    ) -> DataFrame:
        """Creates an external table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        .. versionadded:: 1.3.0

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.catalog.createExternalTable(
            tableName, path, source, schema, **options)

    def sql(self, sqlQuery: str) -> DataFrame:
        """Returns a :class:`DataFrame` representing the result of the given query.

        .. versionadded:: 1.0.0

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), Row(f1=3, f2='row3')]
        """
        return self.sparkSession.sql(sqlQuery)

    def table(self, tableName: str) -> DataFrame:
        """Returns the specified table or view as a :class:`DataFrame`.

        .. versionadded:: 1.0.0

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return self.sparkSession.table(tableName)

    def tables(self, dbName: Optional[str] = None) -> DataFrame:
        """Returns a :class:`DataFrame` containing names of tables in the given database.

        If ``dbName`` is not specified, the current database will be used.

        The returned DataFrame has two columns: ``tableName`` and ``isTemporary``
        (a column with :class:`BooleanType` indicating if a table is a temporary one or not).

        .. versionadded:: 1.3.0

        Parameters
        ----------
        dbName: str, optional
            name of the database to use.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.tables()
        >>> df2.filter("tableName = 'table1'").first()
        Row(namespace='', tableName='table1', isTemporary=True)
        """
        if dbName is None:
            return DataFrame(self._ssql_ctx.tables(), self)
        else:
            return DataFrame(self._ssql_ctx.tables(dbName), self)

    def tableNames(self, dbName: Optional[str] = None) -> List[str]:
        """Returns a list of names of tables in the database ``dbName``.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        dbName: str
            name of the database to use. Default to the current database.

        Returns
        -------
        list
            list of table names, in string

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
    def cacheTable(self, tableName: str) -> None:
        """Caches the specified table in-memory."""
        self._ssql_ctx.cacheTable(tableName)

    @since(1.0)
    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache."""
        self._ssql_ctx.uncacheTable(tableName)

    @since(1.3)
    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache. """
        self._ssql_ctx.clearCache()

    @property
    def read(self) -> DataFrameReader:
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        Returns
        -------
        :class:`DataFrameReader`
        """
        return DataFrameReader(self)

    @property
    def readStream(self) -> DataStreamReader:
        """
        Returns a :class:`DataStreamReader` that can be used to read data streams
        as a streaming :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`DataStreamReader`

        >>> text_sdf = sqlContext.readStream.text(tempfile.mkdtemp())
        >>> text_sdf.isStreaming
        True
        """
        return DataStreamReader(self)

    @property
    def streams(self) -> StreamingQueryManager:
        """Returns a :class:`StreamingQueryManager` that allows managing all the
        :class:`StreamingQuery` StreamingQueries active on `this` context.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.
        """
        from pyspark.sql.streaming import StreamingQueryManager
        return StreamingQueryManager(self._ssql_ctx.streams())


# TODO: ignore[attr-defined] will be removed, once SparkContext is inlined
class HiveContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in Hive.

    Configuration for Hive is read from ``hive-site.xml`` on the classpath.
    It supports running both SQL and HiveQL commands.

    .. deprecated:: 2.0.0
        Use SparkSession.builder.enableHiveSupport().getOrCreate().

    Parameters
    ----------
    sparkContext : :class:`SparkContext`
        The SparkContext to wrap.
    jhiveContext : optional
        An optional JVM Scala HiveContext. If set, we do not instantiate a new
        :class:`HiveContext` in the JVM, instead we make all calls to this object.
        This is only for internal use.

    """

    def __init__(
        self,
        sparkContext: SparkContext,
        jhiveContext: Optional[JavaObject] = None
    ):
        warnings.warn(
            "HiveContext is deprecated in Spark 2.0.0. Please use " +
            "SparkSession.builder.enableHiveSupport().getOrCreate() instead.",
            FutureWarning
        )
        if jhiveContext is None:
            sparkContext._conf.set(  # type: ignore[attr-defined]
                "spark.sql.catalogImplementation", "hive")
            sparkSession = SparkSession.builder._sparkContext(sparkContext).getOrCreate()
        else:
            sparkSession = SparkSession(sparkContext, jhiveContext.sparkSession())
        SQLContext.__init__(self, sparkContext, sparkSession, jhiveContext)

    @classmethod
    def _createForTesting(cls, sparkContext: SparkContext) -> "HiveContext":
        """(Internal use only) Create a new HiveContext for testing.

        All test code that touches HiveContext *must* go through this method. Otherwise,
        you may end up launching multiple derby instances and encounter with incredibly
        confusing error messages.
        """
        jsc = sparkContext._jsc.sc()  # type: ignore[attr-defined]
        jtestHive = sparkContext.\
            _jvm.org.apache.spark.sql.hive.test.TestHiveContext(  # type: ignore[attr-defined]
                jsc, False)
        return cls(sparkContext, jtestHive)

    def refreshTable(self, tableName: str) -> None:
        """Invalidate and refresh all the cached the metadata of the given
        table. For performance reasons, Spark SQL or the external data source
        library it uses might cache certain metadata about a table, such as the
        location of blocks. When those change outside of Spark SQL, users should
        call this function to invalidate the cache.
        """
        self._ssql_ctx.refreshTable(tableName)


def _test() -> None:
    import os
    import doctest
    import tempfile
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.context

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.context.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['tempfile'] = tempfile
    globs['os'] = os
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
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]},"field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", "field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    globs['json'] = sc.parallelize(jsonStrings)
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.context, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
