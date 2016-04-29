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

from collections import namedtuple

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType, StringType, StructType


Database = namedtuple("Database", "name description locationUri")
Table = namedtuple("Table", "name database description tableType isTemporary")
Column = namedtuple("Column", "name description dataType nullable isPartition isBucket")
Function = namedtuple("Function", "name description className isTemporary")


class Catalog(object):
    """User-facing catalog API, accessible through `SparkSession.catalog`.

    This is a thin wrapper around its Scala implementation org.apache.spark.sql.catalog.Catalog.
    """

    def __init__(self, sparkSession):
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession
        self._jsparkSession = sparkSession._jsparkSession
        self._jcatalog = sparkSession._jsparkSession.catalog()

    @ignore_unicode_prefix
    @since(2.0)
    def currentDatabase(self):
        """Returns the current default database in this session.

        >>> spark.catalog._reset()
        >>> spark.catalog.currentDatabase()
        u'default'
        """
        return self._jcatalog.currentDatabase()

    @ignore_unicode_prefix
    @since(2.0)
    def setCurrentDatabase(self, dbName):
        """Sets the current default database in this session.

        >>> spark.catalog._reset()
        >>> spark.sql("CREATE DATABASE some_db")
        DataFrame[]
        >>> spark.catalog.setCurrentDatabase("some_db")
        >>> spark.catalog.currentDatabase()
        u'some_db'
        >>> spark.catalog.setCurrentDatabase("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.setCurrentDatabase(dbName)

    @ignore_unicode_prefix
    @since(2.0)
    def listDatabases(self):
        """Returns a list of databases available across all sessions.

        >>> spark.catalog._reset()
        >>> [db.name for db in spark.catalog.listDatabases()]
        [u'default']
        >>> spark.sql("CREATE DATABASE some_db")
        DataFrame[]
        >>> [db.name for db in spark.catalog.listDatabases()]
        [u'default', u'some_db']
        """
        iter = self._jcatalog.listDatabases().toLocalIterator()
        databases = []
        while iter.hasNext():
            jdb = iter.next()
            databases.append(Database(
                name=jdb.name(),
                description=jdb.description(),
                locationUri=jdb.locationUri()))
        return databases

    @ignore_unicode_prefix
    @since(2.0)
    def listTables(self, dbName=None):
        """Returns a list of tables in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary tables.

        >>> spark.catalog._reset()
        >>> spark.sql("CREATE DATABASE some_db")
        DataFrame[]
        >>> spark.catalog.listTables()
        []
        >>> spark.catalog.listTables("some_db")
        []
        >>> spark.createDataFrame([(1, 1)]).registerTempTable("my_temp_tab")
        >>> spark.sql("CREATE TABLE my_tab1 (name STRING, age INT)")
        DataFrame[]
        >>> spark.sql("CREATE TABLE some_db.my_tab2 (name STRING, age INT)")
        DataFrame[]
        >>> spark.catalog.listTables()
        [Table(name=u'my_tab1', database=u'default', description=None, tableType=u'MANAGED',
        isTemporary=False), Table(name=u'my_temp_tab', database=None, description=None,
        tableType=u'TEMPORARY', isTemporary=True)]
        >>> spark.catalog.listTables("some_db")
        [Table(name=u'my_tab2', database=u'some_db', description=None, tableType=u'MANAGED',
        isTemporary=False), Table(name=u'my_temp_tab', database=None, description=None,
        tableType=u'TEMPORARY', isTemporary=True)]
        >>> spark.catalog.listTables("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listTables(dbName).toLocalIterator()
        tables = []
        while iter.hasNext():
            jtable = iter.next()
            tables.append(Table(
                name=jtable.name(),
                database=jtable.database(),
                description=jtable.description(),
                tableType=jtable.tableType(),
                isTemporary=jtable.isTemporary()))
        return tables

    @ignore_unicode_prefix
    @since(2.0)
    def listFunctions(self, dbName=None):
        """Returns a list of functions registered in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary functions.

        >>> spark.catalog._reset()
        >>> spark.sql("CREATE DATABASE my_db")
        DataFrame[]
        >>> funcNames = set(f.name for f in spark.catalog.listFunctions())
        >>> set(["+", "floor", "to_unix_timestamp", "current_database"]).issubset(funcNames)
        True
        >>> spark.sql("CREATE FUNCTION my_func1 AS 'org.apache.spark.whatever'")
        DataFrame[]
        >>> spark.sql("CREATE FUNCTION my_db.my_func2 AS 'org.apache.spark.whatever'")
        DataFrame[]
        >>> spark.catalog.registerFunction("temp_func", lambda x: str(x))
        >>> newFuncNames = set(f.name for f in spark.catalog.listFunctions()) - funcNames
        >>> newFuncNamesDb = set(f.name for f in spark.catalog.listFunctions("my_db")) - funcNames
        >>> sorted(list(newFuncNames))
        [u'my_func1', u'temp_func']
        >>> sorted(list(newFuncNamesDb))
        [u'my_func2', u'temp_func']
        >>> spark.catalog.listFunctions("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listFunctions(dbName).toLocalIterator()
        functions = []
        while iter.hasNext():
            jfunction = iter.next()
            functions.append(Function(
                name=jfunction.name(),
                description=jfunction.description(),
                className=jfunction.className(),
                isTemporary=jfunction.isTemporary()))
        return functions

    @ignore_unicode_prefix
    @since(2.0)
    def listColumns(self, tableName, dbName=None):
        """Returns a list of columns for the given table in the specified database.

        If no database is specified, the current database is used.

        Note: the order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.

        >>> spark.catalog._reset()
        >>> spark.sql("CREATE DATABASE some_db")
        DataFrame[]
        >>> spark.sql("CREATE TABLE my_tab1 (name STRING, age INT)")
        DataFrame[]
        >>> spark.sql("CREATE TABLE some_db.my_tab2 (nickname STRING, tolerance FLOAT)")
        DataFrame[]
        >>> spark.catalog.listColumns("my_tab1")
        [Column(name=u'name', description=None, dataType=u'string', nullable=True,
        isPartition=False, isBucket=False), Column(name=u'age', description=None,
        dataType=u'int', nullable=True, isPartition=False, isBucket=False)]
        >>> spark.catalog.listColumns("my_tab2", "some_db")
        [Column(name=u'nickname', description=None, dataType=u'string', nullable=True,
        isPartition=False, isBucket=False), Column(name=u'tolerance', description=None,
        dataType=u'float', nullable=True, isPartition=False, isBucket=False)]
        >>> spark.catalog.listColumns("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listColumns(dbName, tableName).toLocalIterator()
        columns = []
        while iter.hasNext():
            jcolumn = iter.next()
            columns.append(Column(
                name=jcolumn.name(),
                description=jcolumn.description(),
                dataType=jcolumn.dataType(),
                nullable=jcolumn.nullable(),
                isPartition=jcolumn.isPartition(),
                isBucket=jcolumn.isBucket()))
        return columns

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
            source = self._sparkSession.getConf(
                "spark.sql.sources.default", "org.apache.spark.sql.parquet")
        if schema is None:
            df = self._jcatalog.createExternalTable(tableName, source, options)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jcatalog.createExternalTable(tableName, source, scala_datatype, options)
        return DataFrame(df, self._sparkSession._wrapped)

    @since(2.0)
    def dropTempTable(self, tableName):
        """Drops the temporary table with the given table name in the catalog.
        If the table has been cached before, then it will also be uncached.

        >>> spark.createDataFrame([(1, 1)]).registerTempTable("my_table")
        >>> spark.table("my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropTempTable("my_table")
        >>> spark.table("my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.dropTempTable(tableName)

    @since(2.0)
    def registerDataFrameAsTable(self, df, tableName):
        """Registers the given :class:`DataFrame` as a temporary table in the catalog.

        >>> df = spark.createDataFrame([(2, 1), (3, 1)])
        >>> spark.catalog.registerDataFrameAsTable(df, "my_cool_table")
        >>> spark.table("my_cool_table").collect()
        [Row(_1=2, _2=1), Row(_1=3, _2=1)]
        """
        if isinstance(df, DataFrame):
            self._jsparkSession.registerDataFrameAsTable(df._jdf, tableName)
        else:
            raise ValueError("Can only register DataFrame as table")

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

        >>> spark.catalog.registerFunction("stringLengthString", lambda x: len(x))
        >>> spark.sql("SELECT stringLengthString('test')").collect()
        [Row(stringLengthString(test)=u'4')]

        >>> from pyspark.sql.types import IntegerType
        >>> spark.catalog.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
        >>> spark.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]

        >>> from pyspark.sql.types import IntegerType
        >>> spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
        >>> spark.sql("SELECT stringLengthInt('test')").collect()
        [Row(stringLengthInt(test)=4)]
        """
        udf = UserDefinedFunction(f, returnType, name)
        self._jsparkSession.udf().registerPython(name, udf._judf)

    @since(2.0)
    def isCached(self, tableName):
        """Returns true if the table is currently cached in-memory.

        >>> spark.catalog._reset()
        >>> spark.createDataFrame([(2, 2), (3, 3)]).registerTempTable("my_tab")
        >>> spark.catalog.isCached("my_tab")
        False
        >>> spark.catalog.cacheTable("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.isCached(tableName)

    @since(2.0)
    def cacheTable(self, tableName):
        """Caches the specified table in-memory.

        >>> spark.catalog._reset()
        >>> spark.createDataFrame([(2, 2), (3, 3)]).registerTempTable("my_tab")
        >>> spark.catalog.isCached("my_tab")
        False
        >>> spark.catalog.cacheTable("my_tab")
        >>> spark.catalog.isCached("my_tab")
        True
        >>> spark.catalog.cacheTable("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.cacheTable(tableName)

    @since(2.0)
    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache.

        >>> spark.catalog._reset()
        >>> spark.createDataFrame([(2, 2), (3, 3)]).registerTempTable("my_tab")
        >>> spark.catalog.cacheTable("my_tab")
        >>> spark.catalog.isCached("my_tab")
        True
        >>> spark.catalog.uncacheTable("my_tab")
        >>> spark.catalog.isCached("my_tab")
        False
        >>> spark.catalog.uncacheTable("does_not_exist") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.uncacheTable(tableName)

    @since(2.0)
    def clearCache(self):
        """Removes all cached tables from the in-memory cache.

        >>> spark.catalog._reset()
        >>> spark.createDataFrame([(2, 2), (3, 3)]).registerTempTable("my_tab1")
        >>> spark.createDataFrame([(2, 2), (3, 3)]).registerTempTable("my_tab2")
        >>> spark.catalog.cacheTable("my_tab1")
        >>> spark.catalog.cacheTable("my_tab2")
        >>> spark.catalog.isCached("my_tab1")
        True
        >>> spark.catalog.isCached("my_tab2")
        True
        >>> spark.catalog.clearCache()
        >>> spark.catalog.isCached("my_tab1")
        False
        >>> spark.catalog.isCached("my_tab2")
        False
        """
        self._jcatalog.clearCache()

    def _reset(self):
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        self._jsparkSession.sessionState().catalog().reset()


def _test():
    import os
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession(sc)
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.catalog,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
