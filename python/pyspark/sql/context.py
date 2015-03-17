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

import warnings
import json
from itertools import imap

from py4j.protocol import Py4JError
from py4j.java_collections import MapConverter

from pyspark.rdd import RDD, _prepare_for_python_RDD
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.sql.types import Row, StringType, StructType, _verify_type, \
    _infer_schema, _has_nulltype, _merge_type, _create_converter, _python_to_sql_converter
from pyspark.sql.dataframe import DataFrame

try:
    import pandas
    has_pandas = True
except ImportError:
    has_pandas = False

__all__ = ["SQLContext", "HiveContext"]


def _monkey_patch_RDD(sqlCtx):
    def toDF(self, schema=None, sampleRatio=None):
        """
        Convert current :class:`RDD` into a :class:`DataFrame`

        This is a shorthand for `sqlCtx.createDataFrame(rdd, schema, sampleRatio)`

        :param schema: a StructType or list of names of columns
        :param samplingRatio: the sample ratio of rows used for inferring
        :return: a DataFrame

        >>> rdd.toDF().collect()
        [Row(name=u'Alice', age=1)]
        """
        return sqlCtx.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF


class SQLContext(object):

    """Main entry point for Spark SQL functionality.

    A SQLContext can be used create L{DataFrame}, register L{DataFrame} as
    tables, execute SQL over tables, cache tables, and read parquet files.
    """

    def __init__(self, sparkContext, sqlContext=None):
        """Create a new SQLContext.

        It will add a method called `toDF` to :class:`RDD`, which could be
        used to convert an RDD into a DataFrame, it's a shorthand for
        :func:`SQLContext.createDataFrame`.

        :param sparkContext: The SparkContext to wrap.
        :param sqlContext: An optional JVM Scala SQLContext. If set, we do not instatiate a new
        SQLContext in the JVM, instead we make all calls to this object.

        >>> from datetime import datetime
        >>> sqlCtx = SQLContext(sc)
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1L,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> df = allTypes.toDF()
        >>> df.registerTempTable("allTypes")
        >>> sqlCtx.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row(c0=2, c1=2.0, c2=False, c3=2, c4=0...8, 1, 14, 1, 5), a=1)]
        >>> df.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time,
        ...                     x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, ...(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        self._scala_SQLContext = sqlContext
        _monkey_patch_RDD(self)

    @property
    def _ssql_ctx(self):
        """Accessor for the JVM Spark SQL context.

        Subclasses can override this property to provide their own
        JVM Contexts.
        """
        if self._scala_SQLContext is None:
            self._scala_SQLContext = self._jvm.SQLContext(self._jsc.sc())
        return self._scala_SQLContext

    def setConf(self, key, value):
        """Sets the given Spark SQL configuration property.
        """
        self._ssql_ctx.setConf(key, value)

    def getConf(self, key, defaultValue):
        """Returns the value of Spark SQL configuration property for the given key.

        If the key is not set, returns defaultValue.
        """
        return self._ssql_ctx.getConf(key, defaultValue)

    def registerFunction(self, name, f, returnType=StringType()):
        """Registers a lambda function as a UDF so it can be used in SQL statements.

        In addition to a name and the function itself, the return type can be optionally specified.
        When the return type is not given it default to a string and conversion will automatically
        be done.  For any other return type, the produced object must match the specified type.

        >>> sqlCtx.registerFunction("stringLengthString", lambda x: len(x))
        >>> sqlCtx.sql("SELECT stringLengthString('test')").collect()
        [Row(c0=u'4')]

        >>> from pyspark.sql.types import IntegerType
        >>> sqlCtx.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
        >>> sqlCtx.sql("SELECT stringLengthInt('test')").collect()
        [Row(c0=4)]
        """
        func = lambda _, it: imap(lambda x: f(*x), it)
        ser = AutoBatchedSerializer(PickleSerializer())
        command = (func, None, ser, ser)
        pickled_cmd, bvars, env, includes = _prepare_for_python_RDD(self._sc, command, self)
        self._ssql_ctx.udf().registerPython(name,
                                            bytearray(pickled_cmd),
                                            env,
                                            includes,
                                            self._sc.pythonExec,
                                            bvars,
                                            self._sc._javaAccumulator,
                                            returnType.json())

    def _inferSchema(self, rdd, samplingRatio=None):
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            warnings.warn("Using RDD of dict to inferSchema is deprecated,"
                          "please use pyspark.sql.Row instead")

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

    def inferSchema(self, rdd, samplingRatio=None):
        """Infer and apply a schema to an RDD of L{Row}.

        ::note:
            Deprecated in 1.3, use :func:`createDataFrame` instead

        When samplingRatio is specified, the schema is inferred by looking
        at the types of each row in the sampled dataset. Otherwise, the
        first 100 rows of the RDD are inspected. Nested collections are
        supported, which can include array, dict, list, Row, tuple,
        namedtuple, or object.

        Each row could be L{pyspark.sql.Row} object or namedtuple or objects.
        Using top level dicts is deprecated, as dict is used to represent Maps.

        If a single column has multiple distinct inferred types, it may cause
        runtime exceptions.

        >>> rdd = sc.parallelize(
        ...     [Row(field1=1, field2="row1"),
        ...      Row(field1=2, field2="row2"),
        ...      Row(field1=3, field2="row3")])
        >>> df = sqlCtx.inferSchema(rdd)
        >>> df.collect()[0]
        Row(field1=1, field2=u'row1')
        """

        if isinstance(rdd, DataFrame):
            raise TypeError("Cannot apply schema to DataFrame")

        schema = self._inferSchema(rdd, samplingRatio)
        converter = _create_converter(schema)
        rdd = rdd.map(converter)
        return self.applySchema(rdd, schema)

    def applySchema(self, rdd, schema):
        """
        Applies the given schema to the given RDD of L{tuple} or L{list}.

        ::note:
            Deprecated in 1.3, use :func:`createDataFrame` instead

        These tuples or lists can contain complex nested structures like
        lists, maps or nested rows.

        The schema should be a StructType.

        It is important that the schema matches the types of the objects
        in each row or exceptions could be thrown at runtime.

        >>> from pyspark.sql.types import *
        >>> rdd2 = sc.parallelize([(1, "row1"), (2, "row2"), (3, "row3")])
        >>> schema = StructType([StructField("field1", IntegerType(), False),
        ...     StructField("field2", StringType(), False)])
        >>> df = sqlCtx.applySchema(rdd2, schema)
        >>> df.collect()
        [Row(field1=1, field2=u'row1'),..., Row(field1=3, field2=u'row3')]
        """

        if isinstance(rdd, DataFrame):
            raise TypeError("Cannot apply schema to DataFrame")

        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType, but got %s" % schema)

        # take the first few rows to verify schema
        rows = rdd.take(10)
        # Row() cannot been deserialized by Pyrolite
        if rows and isinstance(rows[0], tuple) and rows[0].__class__.__name__ == 'Row':
            rdd = rdd.map(tuple)
            rows = rdd.take(10)

        for row in rows:
            _verify_type(row, schema)

        # convert python objects to sql data
        converter = _python_to_sql_converter(schema)
        rdd = rdd.map(converter)

        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        df = self._ssql_ctx.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
        return DataFrame(df, self)

    def createDataFrame(self, data, schema=None, samplingRatio=None):
        """
        Create a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.

        `schema` could be :class:`StructType` or a list of column names.

        When `schema` is a list of column names, the type of each column
        will be inferred from `rdd`.

        When `schema` is None, it will try to infer the column name and type
        from `rdd`, which should be an RDD of :class:`Row`, or namedtuple,
        or dict.

        If referring needed, `samplingRatio` is used to determined how many
        rows will be used to do referring. The first row will be used if
        `samplingRatio` is None.

        :param data: an RDD of Row/tuple/list/dict, list, or pandas.DataFrame
        :param schema: a StructType or list of names of columns
        :param samplingRatio: the sample ratio of rows used for inferring
        :return: a DataFrame

        >>> l = [('Alice', 1)]
        >>> sqlCtx.createDataFrame(l).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> sqlCtx.createDataFrame(l, ['name', 'age']).collect()
        [Row(name=u'Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> sqlCtx.createDataFrame(d).collect()
        [Row(age=1, name=u'Alice')]

        >>> rdd = sc.parallelize(l)
        >>> sqlCtx.createDataFrame(rdd).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> df = sqlCtx.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = sqlCtx.createDataFrame(person)
        >>> df2.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = sqlCtx.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name=u'Alice', age=1)]

        >>> sqlCtx.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name=u'Alice', age=1)]
        """
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if has_pandas and isinstance(data, pandas.DataFrame):
            if schema is None:
                schema = list(data.columns)
            data = [r.tolist() for r in data.to_records(index=False)]

        if not isinstance(data, RDD):
            try:
                # data could be list, tuple, generator ...
                data = self._sc.parallelize(data)
            except Exception:
                raise ValueError("cannot create an RDD from type: %s" % type(data))

        if schema is None:
            return self.inferSchema(data, samplingRatio)

        if isinstance(schema, (list, tuple)):
            first = data.first()
            if not isinstance(first, (list, tuple)):
                raise ValueError("each row in `rdd` should be list or tuple, "
                                 "but got %r" % type(first))
            row_cls = Row(*schema)
            schema = self._inferSchema(data.map(lambda r: row_cls(*r)), samplingRatio)

        return self.applySchema(data, schema)

    def registerDataFrameAsTable(self, rdd, tableName):
        """Registers the given RDD as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of
        SQLContext.

        >>> sqlCtx.registerDataFrameAsTable(df, "table1")
        """
        if (rdd.__class__ is DataFrame):
            df = rdd._jdf
            self._ssql_ctx.registerDataFrameAsTable(df, tableName)
        else:
            raise ValueError("Can only register DataFrame as table")

    def parquetFile(self, *paths):
        """Loads a Parquet file, returning the result as a L{DataFrame}.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> df.saveAsParquetFile(parquetFile)
        >>> df2 = sqlCtx.parquetFile(parquetFile)
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        gateway = self._sc._gateway
        jpaths = gateway.new_array(gateway.jvm.java.lang.String, len(paths))
        for i in range(0, len(paths)):
            jpaths[i] = paths[i]
        jdf = self._ssql_ctx.parquetFile(jpaths)
        return DataFrame(jdf, self)

    def jsonFile(self, path, schema=None, samplingRatio=1.0):
        """
        Loads a text file storing one JSON object per line as a
        L{DataFrame}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it samples the dataset with ratio `samplingRatio` to
        determine the schema.

        >>> import tempfile, shutil
        >>> jsonFile = tempfile.mkdtemp()
        >>> shutil.rmtree(jsonFile)
        >>> with open(jsonFile, 'w') as f:
        ...     f.writelines(jsonStrings)
        >>> df1 = sqlCtx.jsonFile(jsonFile)
        >>> df1.printSchema()
        root
         |-- field1: long (nullable = true)
         |-- field2: string (nullable = true)
         |-- field3: struct (nullable = true)
         |    |-- field4: long (nullable = true)

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...     StructField("field2", StringType()),
        ...     StructField("field3",
        ...         StructType([StructField("field5", ArrayType(IntegerType()))]))])
        >>> df2 = sqlCtx.jsonFile(jsonFile, schema)
        >>> df2.printSchema()
        root
         |-- field2: string (nullable = true)
         |-- field3: struct (nullable = true)
         |    |-- field5: array (nullable = true)
         |    |    |-- element: integer (containsNull = true)
        """
        if schema is None:
            df = self._ssql_ctx.jsonFile(path, samplingRatio)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            df = self._ssql_ctx.jsonFile(path, scala_datatype)
        return DataFrame(df, self)

    def jsonRDD(self, rdd, schema=None, samplingRatio=1.0):
        """Loads an RDD storing one JSON object per string as a L{DataFrame}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it samples the dataset with ratio `samplingRatio` to
        determine the schema.

        >>> df1 = sqlCtx.jsonRDD(json)
        >>> df1.first()
        Row(field1=1, field2=u'row1', field3=Row(field4=11, field5=None), field6=None)

        >>> df2 = sqlCtx.jsonRDD(json, df1.schema)
        >>> df2.first()
        Row(field1=1, field2=u'row1', field3=Row(field4=11, field5=None), field6=None)

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...     StructField("field2", StringType()),
        ...     StructField("field3",
        ...                 StructType([StructField("field5", ArrayType(IntegerType()))]))
        ... ])
        >>> df3 = sqlCtx.jsonRDD(json, schema)
        >>> df3.first()
        Row(field2=u'row1', field3=Row(field5=None))

        """

        def func(iterator):
            for x in iterator:
                if not isinstance(x, basestring):
                    x = unicode(x)
                if isinstance(x, unicode):
                    x = x.encode("utf-8")
                yield x
        keyed = rdd.mapPartitions(func)
        keyed._bypass_serializer = True
        jrdd = keyed._jrdd.map(self._jvm.BytesToString())
        if schema is None:
            df = self._ssql_ctx.jsonRDD(jrdd.rdd(), samplingRatio)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            df = self._ssql_ctx.jsonRDD(jrdd.rdd(), scala_datatype)
        return DataFrame(df, self)

    def load(self, path=None, source=None, schema=None, **options):
        """Returns the dataset in a data source as a DataFrame.

        The data source is specified by the `source` and a set of `options`.
        If `source` is not specified, the default data source configured by
        spark.sql.sources.default will be used.

        Optionally, a schema can be provided as the schema of the returned DataFrame.
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self.getConf("spark.sql.sources.default",
                                  "org.apache.spark.sql.parquet")
        joptions = MapConverter().convert(options,
                                          self._sc._gateway._gateway_client)
        if schema is None:
            df = self._ssql_ctx.load(source, joptions)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            df = self._ssql_ctx.load(source, scala_datatype, joptions)
        return DataFrame(df, self)

    def createExternalTable(self, tableName, path=None, source=None,
                            schema=None, **options):
        """Creates an external table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the `source` and a set of `options`.
        If `source` is not specified, the default data source configured by
        spark.sql.sources.default will be used.

        Optionally, a schema can be provided as the schema of the returned DataFrame and
        created external table.
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self.getConf("spark.sql.sources.default",
                                  "org.apache.spark.sql.parquet")
        joptions = MapConverter().convert(options,
                                          self._sc._gateway._gateway_client)
        if schema is None:
            df = self._ssql_ctx.createExternalTable(tableName, source, joptions)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            df = self._ssql_ctx.createExternalTable(tableName, source, scala_datatype,
                                                    joptions)
        return DataFrame(df, self)

    def sql(self, sqlQuery):
        """Return a L{DataFrame} representing the result of the given query.

        >>> sqlCtx.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlCtx.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return DataFrame(self._ssql_ctx.sql(sqlQuery), self)

    def table(self, tableName):
        """Returns the specified table as a L{DataFrame}.

        >>> sqlCtx.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlCtx.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return DataFrame(self._ssql_ctx.table(tableName), self)

    def tables(self, dbName=None):
        """Returns a DataFrame containing names of tables in the given database.

        If `dbName` is not specified, the current database will be used.

        The returned DataFrame has two columns, tableName and isTemporary
        (a column with BooleanType indicating if a table is a temporary one or not).

        >>> sqlCtx.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlCtx.tables()
        >>> df2.filter("tableName = 'table1'").first()
        Row(tableName=u'table1', isTemporary=True)
        """
        if dbName is None:
            return DataFrame(self._ssql_ctx.tables(), self)
        else:
            return DataFrame(self._ssql_ctx.tables(dbName), self)

    def tableNames(self, dbName=None):
        """Returns a list of names of tables in the database `dbName`.

        If `dbName` is not specified, the current database will be used.

        >>> sqlCtx.registerDataFrameAsTable(df, "table1")
        >>> "table1" in sqlCtx.tableNames()
        True
        >>> "table1" in sqlCtx.tableNames("db")
        True
        """
        if dbName is None:
            return [name for name in self._ssql_ctx.tableNames()]
        else:
            return [name for name in self._ssql_ctx.tableNames(dbName)]

    def cacheTable(self, tableName):
        """Caches the specified table in-memory."""
        self._ssql_ctx.cacheTable(tableName)

    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache."""
        self._ssql_ctx.uncacheTable(tableName)

    def clearCache(self):
        """Removes all cached tables from the in-memory cache. """
        self._ssql_ctx.clearCache()


class HiveContext(SQLContext):

    """A variant of Spark SQL that integrates with data stored in Hive.

    Configuration for Hive is read from hive-site.xml on the classpath.
    It supports running both SQL and HiveQL commands.
    """

    def __init__(self, sparkContext, hiveContext=None):
        """Create a new HiveContext.

        :param sparkContext: The SparkContext to wrap.
        :param hiveContext: An optional JVM Scala HiveContext. If set, we do not instatiate a new
        HiveContext in the JVM, instead we make all calls to this object.
        """
        SQLContext.__init__(self, sparkContext)

        if hiveContext:
            self._scala_HiveContext = hiveContext

    @property
    def _ssql_ctx(self):
        try:
            if not hasattr(self, '_scala_HiveContext'):
                self._scala_HiveContext = self._get_hive_ctx()
            return self._scala_HiveContext
        except Py4JError as e:
            raise Exception("You must build Spark with Hive. "
                            "Export 'SPARK_HIVE=true' and run "
                            "build/sbt assembly", e)

    def _get_hive_ctx(self):
        return self._jvm.HiveContext(self._jsc.sc())


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.context
    globs = pyspark.sql.context.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlCtx'] = sqlCtx = SQLContext(sc)
    globs['rdd'] = rdd = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")]
    )
    _monkey_patch_RDD(sqlCtx)
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
