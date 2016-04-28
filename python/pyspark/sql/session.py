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
from functools import reduce

if sys.version >= '3':
    basestring = unicode = str
else:
    from itertools import imap as map

from pyspark import since
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import Row, DataType, StringType, StructType, _verify_type, \
    _infer_schema, _has_nulltype, _merge_type, _create_converter, _parse_datatype_string
from pyspark.sql.utils import install_exception_handler

__all__ = ["SparkSession"]


def _monkey_patch_RDD(sparkSession):
    def toDF(self, schema=None, sampleRatio=None):
        """
        Converts current :class:`RDD` into a :class:`DataFrame`

        This is a shorthand for ``spark.createDataFrame(rdd, schema, sampleRatio)``

        :param schema: a StructType or list of names of columns
        :param samplingRatio: the sample ratio of rows used for inferring
        :return: a DataFrame

        >>> rdd.toDF().collect()
        [Row(name=u'Alice', age=1)]
        """
        return sparkSession.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF


# TODO(andrew): implement conf and catalog namespaces
class SparkSession(object):
    """Main entry point for Spark SQL functionality.

    A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    :param sparkContext: The :class:`SparkContext` backing this SparkSession.
    :param jsparkSession: An optional JVM Scala SparkSession. If set, we do not instantiate a new
        SparkSession in the JVM, instead we make all calls to this object.
    """

    _instantiatedContext = None

    @ignore_unicode_prefix
    def __init__(self, sparkContext, jsparkSession=None):
        """Creates a new SparkSession.

        >>> from datetime import datetime
        >>> spark = SparkSession(sc)
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> df = allTypes.toDF()
        >>> df.registerTempTable("allTypes")
        >>> spark.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row((i + CAST(1 AS BIGINT))=2, (d + CAST(1 AS DOUBLE))=2.0, (NOT b)=False, list[1]=2, \
            dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
        >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        from pyspark.sql.context import SQLContext
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        if jsparkSession is None:
            jsparkSession = self._jvm.SparkSession(self._jsc.sc())
        self._jsparkSession = jsparkSession
        self._jwrapped = self._jsparkSession.wrapped()
        self._wrapped = SQLContext(self._sc, self, self._jwrapped)
        _monkey_patch_RDD(self)
        install_exception_handler()
        if SparkSession._instantiatedContext is None:
            SparkSession._instantiatedContext = self

    @classmethod
    @since(2.0)
    def withHiveSupport(cls, sparkContext):
        """Returns a new SparkSession with a catalog backed by Hive

        :param sparkContext: The underlying :class:`SparkContext`.
        """
        jsparkSession = sparkContext._jvm.SparkSession.withHiveSupport(sparkContext._jsc.sc())
        return cls(sparkContext, jsparkSession)

    @since(2.0)
    def newSession(self):
        """
        Returns a new SparkSession as new session, that has separate SQLConf,
        registered temporary tables and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    @since(2.0)
    def setConf(self, key, value):
        """
        Sets the given Spark SQL configuration property.
        """
        self._jsparkSession.setConf(key, value)

    @ignore_unicode_prefix
    @since(2.0)
    def getConf(self, key, defaultValue=None):
        """Returns the value of Spark SQL configuration property for the given key.

        If the key is not set and defaultValue is not None, return
        defaultValue. If the key is not set and defaultValue is None, return
        the system default value.

        >>> spark.getConf("spark.sql.shuffle.partitions")
        u'200'
        >>> spark.getConf("spark.sql.shuffle.partitions", "10")
        u'10'
        >>> spark.setConf("spark.sql.shuffle.partitions", "50")
        >>> spark.getConf("spark.sql.shuffle.partitions", "10")
        u'50'
        """
        if defaultValue is not None:
            return self._jsparkSession.getConf(key, defaultValue)
        else:
            return self._jsparkSession.getConf(key)

    @property
    @since(2.0)
    def udf(self):
        """Returns a :class:`UDFRegistration` for UDF registration.

        :return: :class:`UDFRegistration`
        """
        return UDFRegistration(self)

    @since(2.0)
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

        >>> spark.range(1, 7, 2).collect()
        [Row(id=1), Row(id=3), Row(id=5)]

        If only one argument is specified, it will be used as the end value.

        >>> spark.range(3).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        if end is None:
            jdf = self._jsparkSession.range(0, int(start), int(step), int(numPartitions))
        else:
            jdf = self._jsparkSession.range(int(start), int(end), int(step), int(numPartitions))

        return DataFrame(jdf, self._wrapped)

    @ignore_unicode_prefix
    @since(2.0)
    def registerFunction(self, name, f, returnType=StringType()):
        """Registers a python function (including lambda function) as a UDF
        so it can be used in SQL statements.

        In addition to a name and the function itself, the return type can be optionally specified.
        When the return type is not given it default to a string and conversion will automatically
        be done.  For any other return type, the produced object must match the specified type.

        :param name: name of the UDF
        :param f: python function
        :param returnType: a :class:`DataType` object

        >>> spark.registerFunction("stringLengthString", lambda x: len(x))
        >>> spark.sql("SELECT stringLengthString('test')").collect()
        [Row(stringLengthString(test)=u'4')]

        >>> from pyspark.sql.types import IntegerType
        >>> spark.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
        >>> spark.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]

        >>> from pyspark.sql.types import IntegerType
        >>> spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
        >>> spark.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]
        """
        udf = UserDefinedFunction(f, returnType, name)
        self._jsparkSession.udf().registerPython(name, udf._judf)

    def _inferSchemaFromList(self, data):
        """
        Infer schema from list of Row or tuple.

        :param data: list of Row or tuple
        :return: StructType
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        first = data[0]
        if type(first) is dict:
            warnings.warn("inferring schema from dict is deprecated,"
                          "please use pyspark.sql.Row instead")
        schema = reduce(_merge_type, map(_infer_schema, data))
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def _inferSchema(self, rdd, samplingRatio=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: StructType
        """
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            warnings.warn("Using RDD of dict to inferSchema is deprecated. "
                          "Use pyspark.sql.Row instead")

        if samplingRatio is None:
            schema = _infer_schema(first)
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(schema, _infer_schema(row))
                    if not _has_nulltype(schema):
                        break
                else:
                    raise ValueError("Some of types cannot be determined by the "
                                     "first 100 rows, please try again with sampling")
        else:
            if samplingRatio < 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(_infer_schema).reduce(_merge_type)
        return schema

    def _createFromRDD(self, rdd, schema, samplingRatio):
        """
        Create an RDD for DataFrame from an existing RDD, returns the RDD and schema.
        """
        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchema(rdd, samplingRatio)
            converter = _create_converter(struct)
            rdd = rdd.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        rdd = rdd.map(schema.toInternal)
        return rdd, schema

    def _createFromLocal(self, data, schema):
        """
        Create an RDD for DataFrame from an list or pandas.DataFrame, returns
        the RDD and schema.
        """
        # make sure data could consumed multiple times
        if not isinstance(data, list):
            data = list(data)

        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchemaFromList(data)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif isinstance(schema, StructType):
            for row in data:
                _verify_type(row, schema)

        else:
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        data = [schema.toInternal(row) for row in data]
        return self._sc.parallelize(data), schema

    @since(2.0)
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
        >>> spark.createDataFrame(l).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> spark.createDataFrame(l, ['name', 'age']).collect()
        [Row(name=u'Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> spark.createDataFrame(d).collect()
        [Row(age=1, name=u'Alice')]

        >>> rdd = sc.parallelize(l)
        >>> spark.createDataFrame(rdd).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> df = spark.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = spark.createDataFrame(person)
        >>> df2.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = spark.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name=u'Alice', age=1)]

        >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name=u'Alice', age=1)]
        >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a=u'Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> spark.createDataFrame(rdd, "int").collect()
        [Row(value=1)]
        >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, basestring):
            schema = _parse_datatype_string(schema)

        try:
            import pandas
            has_pandas = True
        except Exception:
            has_pandas = False
        if has_pandas and isinstance(data, pandas.DataFrame):
            if schema is None:
                schema = [str(x) for x in data.columns]
            data = [r.tolist() for r in data.to_records(index=False)]

        if isinstance(schema, StructType):
            def prepare(obj):
                _verify_type(obj, schema)
                return obj
        elif isinstance(schema, DataType):
            datatype = schema

            def prepare(obj):
                _verify_type(obj, datatype)
                return (obj, )
            schema = StructType().add("value", datatype)
        else:
            prepare = lambda obj: obj

        if isinstance(data, RDD):
            rdd, schema = self._createFromRDD(data.map(prepare), schema, samplingRatio)
        else:
            rdd, schema = self._createFromLocal(map(prepare, data), schema)
        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
        df = DataFrame(jdf, self._wrapped)
        df._schema = schema
        return df

    @since(2.0)
    def registerDataFrameAsTable(self, df, tableName):
        """Registers the given :class:`DataFrame` as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of :class:`SparkSession`.

        >>> spark.registerDataFrameAsTable(df, "table1")
        """
        if (df.__class__ is DataFrame):
            self._jsparkSession.registerDataFrameAsTable(df._jdf, tableName)
        else:
            raise ValueError("Can only register DataFrame as table")

    @since(2.0)
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
        if path is not None:
            options["path"] = path
        if source is None:
            source = self.getConf("spark.sql.sources.default",
                                  "org.apache.spark.sql.parquet")
        if schema is None:
            df = self._jsparkSession.catalog().createExternalTable(tableName, source, options)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jsparkSession.catalog().createExternalTable(
                tableName, source, scala_datatype, options)
        return DataFrame(df, self._wrapped)

    @ignore_unicode_prefix
    @since(2.0)
    def sql(self, sqlQuery):
        """Returns a :class:`DataFrame` representing the result of the given query.

        :return: :class:`DataFrame`

        >>> spark.registerDataFrameAsTable(df, "table1")
        >>> df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return DataFrame(self._jsparkSession.sql(sqlQuery), self._wrapped)

    @since(2.0)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :return: :class:`DataFrame`

        >>> spark.registerDataFrameAsTable(df, "table1")
        >>> df2 = spark.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return DataFrame(self._jsparkSession.table(tableName), self._wrapped)

    @property
    @since(2.0)
    def read(self):
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        :return: :class:`DataFrameReader`
        """
        return DataFrameReader(self._wrapped)
