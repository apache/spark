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
from typing import Any, Callable, NamedTuple, List, Optional, TYPE_CHECKING

from pyspark import since
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql._typing import UserDefinedFunctionLike
    from pyspark.sql.types import DataType


class CatalogMetadata(NamedTuple):
    name: str
    description: Optional[str]


class Database(NamedTuple):
    name: str
    catalog: Optional[str]
    description: Optional[str]
    locationUri: str


class Table(NamedTuple):
    name: str
    catalog: Optional[str]
    namespace: Optional[List[str]]
    description: Optional[str]
    tableType: str
    isTemporary: bool

    @property
    def database(self) -> Optional[str]:
        if self.namespace is not None and len(self.namespace) == 1:
            return self.namespace[0]
        else:
            return None


class Column(NamedTuple):
    name: str
    description: Optional[str]
    dataType: str
    nullable: bool
    isPartition: bool
    isBucket: bool


class Function(NamedTuple):
    name: str
    catalog: Optional[str]
    namespace: Optional[List[str]]
    description: Optional[str]
    className: str
    isTemporary: bool


class Catalog:
    """User-facing catalog API, accessible through `SparkSession.catalog`.

    This is a thin wrapper around its Scala implementation org.apache.spark.sql.catalog.Catalog.
    """

    def __init__(self, sparkSession: SparkSession) -> None:
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession
        self._jsparkSession = sparkSession._jsparkSession
        self._jcatalog = sparkSession._jsparkSession.catalog()

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        return self._jcatalog.currentCatalog()

    def setCurrentCatalog(self, catalogName: str) -> None:
        """Sets the current default catalog in this session.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        catalogName : str
            name of the catalog to set
        """
        return self._jcatalog.setCurrentCatalog(catalogName)

    def listCatalogs(self) -> List[CatalogMetadata]:
        """Returns a list of catalogs in this session.

        .. versionadded:: 3.4.0
        """
        iter = self._jcatalog.listCatalogs().toLocalIterator()
        catalogs = []
        while iter.hasNext():
            jcatalog = iter.next()
            catalogs.append(CatalogMetadata(name=jcatalog.name, description=jcatalog.description))
        return catalogs

    @since(2.0)
    def currentDatabase(self) -> str:
        """Returns the current default database in this session."""
        return self._jcatalog.currentDatabase()

    @since(2.0)
    def setCurrentDatabase(self, dbName: str) -> None:
        """Sets the current default database in this session."""
        return self._jcatalog.setCurrentDatabase(dbName)

    @since(2.0)
    def listDatabases(self) -> List[Database]:
        """Returns a list of databases available across all sessions."""
        iter = self._jcatalog.listDatabases().toLocalIterator()
        databases = []
        while iter.hasNext():
            jdb = iter.next()
            databases.append(
                Database(
                    name=jdb.name(),
                    catalog=jdb.catalog(),
                    description=jdb.description(),
                    locationUri=jdb.locationUri(),
                )
            )
        return databases

    def getDatabase(self, dbName: str) -> Database:
        """Get the database with the specified name.
        This throws an :class:`AnalysisException` when the database cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str
             name of the database to check existence.

        Examples
        --------
        >>> spark.catalog.getDatabase("default")
        Database(name='default', catalog=None, description='default database', ...
        >>> spark.catalog.getDatabase("spark_catalog.default")
        Database(name='default', catalog='spark_catalog', description='default database', ...
        """
        jdb = self._jcatalog.getDatabase(dbName)
        return Database(
            name=jdb.name(),
            catalog=jdb.catalog(),
            description=jdb.description(),
            locationUri=jdb.locationUri(),
        )

    def databaseExists(self, dbName: str) -> bool:
        """Check if the database with the specified name exists.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        dbName : str
             name of the database to check existence

        Returns
        -------
        bool
            Indicating whether the database exists

        .. versionchanged:: 3.4
           Allowed ``dbName`` to be qualified with catalog name.

        Examples
        --------
        >>> spark.catalog.databaseExists("test_new_database")
        False
        >>> df = spark.sql("CREATE DATABASE test_new_database")
        >>> spark.catalog.databaseExists("test_new_database")
        True
        >>> spark.catalog.databaseExists("spark_catalog.test_new_database")
        True
        >>> df = spark.sql("DROP DATABASE test_new_database")
        """
        return self._jcatalog.databaseExists(dbName)

    @since(2.0)
    def listTables(self, dbName: Optional[str] = None) -> List[Table]:
        """Returns a list of tables/views in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary views.

        .. versionchanged:: 3.4
           Allowed ``dbName`` to be qualified with catalog name.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listTables(dbName).toLocalIterator()
        tables = []
        while iter.hasNext():
            jtable = iter.next()

            jnamespace = jtable.namespace()
            if jnamespace is not None:
                namespace = [jnamespace[i] for i in range(0, len(jnamespace))]
            else:
                namespace = None

            tables.append(
                Table(
                    name=jtable.name(),
                    catalog=jtable.catalog(),
                    namespace=namespace,
                    description=jtable.description(),
                    tableType=jtable.tableType(),
                    isTemporary=jtable.isTemporary(),
                )
            )
        return tables

    def getTable(self, tableName: str) -> Table:
        """Get the table or view with the specified name. This table can be a temporary view or a
        table/view. This throws an :class:`AnalysisException` when no Table can be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        tableName : str
                    name of the table to check existence.

        Examples
        --------
        >>> df = spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.getTable("tab1")
        Table(name='tab1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getTable("default.tab1")
        Table(name='tab1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getTable("spark_catalog.default.tab1")
        Table(name='tab1', catalog='spark_catalog', namespace=['default'], ...
        >>> df = spark.sql("DROP TABLE tab1")
        >>> spark.catalog.getTable("tab1")
        Traceback (most recent call last):
            ...
        pyspark.sql.utils.AnalysisException: ...
        """
        jtable = self._jcatalog.getTable(tableName)
        jnamespace = jtable.namespace()
        if jnamespace is not None:
            namespace = [jnamespace[i] for i in range(0, len(jnamespace))]
        else:
            namespace = None
        return Table(
            name=jtable.name(),
            catalog=jtable.catalog(),
            namespace=namespace,
            description=jtable.description(),
            tableType=jtable.tableType(),
            isTemporary=jtable.isTemporary(),
        )

    @since(2.0)
    def listFunctions(self, dbName: Optional[str] = None) -> List[Function]:
        """Returns a list of functions registered in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary functions.

        .. versionchanged:: 3.4
           Allowed ``dbName`` to be qualified with catalog name.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listFunctions(dbName).toLocalIterator()
        functions = []
        while iter.hasNext():
            jfunction = iter.next()
            jnamespace = jfunction.namespace()
            if jnamespace is not None:
                namespace = [jnamespace[i] for i in range(0, len(jnamespace))]
            else:
                namespace = None

            functions.append(
                Function(
                    name=jfunction.name(),
                    catalog=jfunction.catalog(),
                    namespace=namespace,
                    description=jfunction.description(),
                    className=jfunction.className(),
                    isTemporary=jfunction.isTemporary(),
                )
            )
        return functions

    def functionExists(self, functionName: str, dbName: Optional[str] = None) -> bool:
        """Check if the function with the specified name exists.
        This can either be a temporary function or a function.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        functionName : str
            name of the function to check existence
        dbName : str, optional
            name of the database to check function existence in.
            If no database is specified, the current database is used

           .. deprecated:: 3.4.0


        Returns
        -------
        bool
            Indicating whether the function exists

        .. versionchanged:: 3.4
           Allowed ``functionName`` to be qualified with catalog name

        Examples
        --------
        >>> spark.catalog.functionExists("unexisting_function")
        False
        >>> spark.catalog.functionExists("default.unexisting_function")
        False
        >>> spark.catalog.functionExists("spark_catalog.default.unexisting_function")
        False
        """
        if dbName is None:
            return self._jcatalog.functionExists(functionName)
        else:
            warnings.warn(
                "`dbName` has been deprecated since Spark 3.4 and might be removed in "
                "a future version. Use functionExists(`dbName.tableName`) instead.",
                FutureWarning,
            )
            return self._jcatalog.functionExists(dbName, functionName)

    def getFunction(self, functionName: str) -> Function:
        """Get the function with the specified name. This function can be a temporary function or a
        function. This throws an :class:`AnalysisException` when the function cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        tableName : str
                    name of the function to check existence.

        Examples
        --------
        >>> func = spark.sql("CREATE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
        >>> spark.catalog.getFunction("my_func1")
        Function(name='my_func1', catalog=None, namespace=['default'], ...
        >>> spark.catalog.getFunction("default.my_func1")
        Function(name='my_func1', catalog=None, namespace=['default'], ...
        >>> spark.catalog.getFunction("spark_catalog.default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getFunction("my_func2")
        Traceback (most recent call last):
            ...
        pyspark.sql.utils.AnalysisException: ...
        """
        jfunction = self._jcatalog.getFunction(functionName)
        jnamespace = jfunction.namespace()
        if jnamespace is not None:
            namespace = [jnamespace[i] for i in range(0, len(jnamespace))]
        else:
            namespace = None
        return Function(
            name=jfunction.name(),
            catalog=jfunction.catalog(),
            namespace=namespace,
            description=jfunction.description(),
            className=jfunction.className(),
            isTemporary=jfunction.isTemporary(),
        )

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[Column]:
        """Returns a list of columns for the given table/view in the specified database.

         If no database is specified, the current database is used.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
                    name of the table to check existence
        dbName : str, optional
                 name of the database to check table existence in.

           .. deprecated:: 3.4.0

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name when ``dbName`` is None.

         Notes
         -----
         the order of arguments here is different from that of its JVM counterpart
         because Python does not support method overloading.
        """
        if dbName is None:
            iter = self._jcatalog.listColumns(tableName).toLocalIterator()
        else:
            warnings.warn(
                "`dbName` has been deprecated since Spark 3.4 and might be removed in "
                "a future version. Use listColumns(`dbName.tableName`) instead.",
                FutureWarning,
            )
            iter = self._jcatalog.listColumns(dbName, tableName).toLocalIterator()

        columns = []
        while iter.hasNext():
            jcolumn = iter.next()
            columns.append(
                Column(
                    name=jcolumn.name(),
                    description=jcolumn.description(),
                    dataType=jcolumn.dataType(),
                    nullable=jcolumn.nullable(),
                    isPartition=jcolumn.isPartition(),
                    isBucket=jcolumn.isBucket(),
                )
            )
        return columns

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        """Check if the table or view with the specified name exists.
        This can either be a temporary view or a table/view.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        tableName : str
                    name of the table to check existence
                    If no database is specified, first try to treat ``tableName`` as a
                    multi-layer-namespace identifier, then try to ``tableName`` as a normal table
                    name in current database if necessary.
        dbName : str, optional
                 name of the database to check table existence in.

           .. deprecated:: 3.4.0


        Returns
        -------
        bool
            Indicating whether the table/view exists

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        Examples
        --------

        This function can check if a table is defined or not:

        >>> spark.catalog.tableExists("unexisting_table")
        False
        >>> df = spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.tableExists("tab1")
        True
        >>> spark.catalog.tableExists("default.tab1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.tab1")
        True
        >>> spark.catalog.tableExists("tab1", "default")
        True
        >>> df = spark.sql("DROP TABLE tab1")
        >>> spark.catalog.tableExists("unexisting_table")
        False

        It also works for views:

        >>> spark.catalog.tableExists("view1")
        False
        >>> df = spark.sql("CREATE VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True
        >>> spark.catalog.tableExists("default.view1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.view1")
        True
        >>> spark.catalog.tableExists("view1", "default")
        True
        >>> df = spark.sql("DROP VIEW view1")
        >>> spark.catalog.tableExists("view1")
        False

        And also for temporary views:

        >>> df = spark.sql("CREATE TEMPORARY VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True
        >>> df = spark.sql("DROP VIEW view1")
        >>> spark.catalog.tableExists("view1")
        False
        """
        if dbName is None:
            return self._jcatalog.tableExists(tableName)
        else:
            warnings.warn(
                "`dbName` has been deprecated since Spark 3.4 and might be removed in "
                "a future version. Use tableExists(`dbName.tableName`) instead.",
                FutureWarning,
            )
            return self._jcatalog.tableExists(dbName, tableName)

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str,
    ) -> DataFrame:
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`DataFrame`
        """
        warnings.warn(
            "createExternalTable is deprecated since Spark 2.2, please use createTable instead.",
            FutureWarning,
        )
        return self.createTable(tableName, path, source, schema, **options)

    def createTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        description: Optional[str] = None,
        **options: str,
    ) -> DataFrame:
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used. When ``path`` is specified, an external table is
        created from the data at the given path. Otherwise a managed table is created.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created table.

        .. versionadded:: 2.2.0

        Returns
        -------
        :class:`DataFrame`

        .. versionchanged:: 3.1
           Added the ``description`` parameter.

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name.
        """
        if path is not None:
            options["path"] = path
        if source is None:
            c = self._sparkSession._jconf
            source = c.defaultDataSourceName()
        if description is None:
            description = ""
        if schema is None:
            df = self._jcatalog.createTable(tableName, source, description, options)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jcatalog.createTable(tableName, source, scala_datatype, description, options)
        return DataFrame(df, self._sparkSession)

    def dropTempView(self, viewName: str) -> None:
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        .. versionadded:: 2.0.0

        Notes
        -----
        The return type of this method was None in Spark 2.0, but changed to Boolean
        in Spark 2.1.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")
        >>> spark.table("my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropTempView("my_table")
        True
        >>> spark.table("my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.dropTempView(viewName)

    def dropGlobalTempView(self, viewName: str) -> None:
        """Drops the global temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        .. versionadded:: 2.1.0

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")
        >>> spark.table("global_temp.my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropGlobalTempView("my_table")
        True
        >>> spark.table("global_temp.my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.dropGlobalTempView(viewName)

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional["DataType"] = None
    ) -> "UserDefinedFunctionLike":
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 2.0.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.register` instead.
        """
        warnings.warn("Deprecated in 2.3.0. Use spark.udf.register instead.", FutureWarning)
        return self._sparkSession.udf.register(name, f, returnType)

    @since(2.0)
    def isCached(self, tableName: str) -> bool:
        """Returns true if the table is currently cached in-memory.

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name.
        """
        return self._jcatalog.isCached(tableName)

    @since(2.0)
    def cacheTable(self, tableName: str) -> None:
        """Caches the specified table in-memory.

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name.
        """
        self._jcatalog.cacheTable(tableName)

    @since(2.0)
    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache.

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name.
        """
        self._jcatalog.uncacheTable(tableName)

    @since(2.0)
    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache."""
        self._jcatalog.clearCache()

    @since(2.0)
    def refreshTable(self, tableName: str) -> None:
        """Invalidates and refreshes all the cached data and metadata of the given table.

        .. versionchanged:: 3.4
           Allowed ``tableName`` to be qualified with catalog name.
        """
        self._jcatalog.refreshTable(tableName)

    @since("2.1.1")
    def recoverPartitions(self, tableName: str) -> None:
        """Recovers all the partitions of the given table and update the catalog.

        Only works with a partitioned table, and not a view.
        """
        self._jcatalog.recoverPartitions(tableName)

    @since("2.2.0")
    def refreshByPath(self, path: str) -> None:
        """Invalidates and refreshes all the cached data (and the associated metadata) for any
        DataFrame that contains the given data source path.
        """
        self._jcatalog.refreshByPath(path)

    def _reset(self) -> None:
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        self._jsparkSession.sessionState().catalog().reset()


def _test() -> None:
    import os
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.catalog tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.catalog,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
