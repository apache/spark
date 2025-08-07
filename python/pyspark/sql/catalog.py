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

from pyspark.errors import PySparkTypeError
from pyspark.storagelevel import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql._typing import UserDefinedFunctionLike
    from pyspark.sql._typing import DataTypeOrString


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
    isCluster: bool


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

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, sparkSession: SparkSession) -> None:
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession
        self._jsparkSession = sparkSession._jsparkSession
        self._sc = sparkSession._sc
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

        Examples
        --------
        >>> spark.catalog.setCurrentCatalog("spark_catalog")
        """
        return self._jcatalog.setCurrentCatalog(catalogName)

    def listCatalogs(self, pattern: Optional[str] = None) -> List[CatalogMetadata]:
        """Returns a list of catalogs in this session.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        pattern : str, optional
            The pattern that the catalog name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`CatalogMetadata`.

        Examples
        --------
        >>> spark.catalog.listCatalogs()
        [CatalogMetadata(name='spark_catalog', description=None)]

        >>> spark.catalog.listCatalogs("spark*")
        [CatalogMetadata(name='spark_catalog', description=None)]

        >>> spark.catalog.listCatalogs("hive*")
        []
        """
        if pattern is None:
            iter = self._jcatalog.listCatalogs().toLocalIterator()
        else:
            iter = self._jcatalog.listCatalogs(pattern).toLocalIterator()
        catalogs = []
        while iter.hasNext():
            jcatalog = iter.next()
            catalogs.append(
                CatalogMetadata(name=jcatalog.name(), description=jcatalog.description())
            )
        return catalogs

    def currentDatabase(self) -> str:
        """
        Returns the current default database in this session.

        .. versionadded:: 2.0.0

        Returns
        -------
        str
            The current default database name.

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        return self._jcatalog.currentDatabase()

    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        return self._jcatalog.setCurrentDatabase(dbName)

    def listDatabases(self, pattern: Optional[str] = None) -> List[Database]:
        """
        Returns a list of databases available across all sessions.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        pattern : str, optional
            The pattern that the database name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Database`.

        Examples
        --------
        >>> spark.catalog.listDatabases()
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.listDatabases("def*")
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.listDatabases("def2*")
        []
        """
        if pattern is None:
            iter = self._jcatalog.listDatabases().toLocalIterator()
        else:
            iter = self._jcatalog.listDatabases(pattern).toLocalIterator()
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
             name of the database to get.

        Returns
        -------
        :class:`Database`
            The database found by the name.

        Examples
        --------
        >>> spark.catalog.getDatabase("default")
        Database(name='default', catalog='spark_catalog', description='default database', ...

        Using the fully qualified name with the catalog name.

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

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        Returns
        -------
        bool
            Indicating whether the database exists

        Examples
        --------
        Check if 'test_new_database' database exists

        >>> spark.catalog.databaseExists("test_new_database")
        False
        >>> _ = spark.sql("CREATE DATABASE test_new_database")
        >>> spark.catalog.databaseExists("test_new_database")
        True

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.databaseExists("spark_catalog.test_new_database")
        True
        >>> _ = spark.sql("DROP DATABASE test_new_database")
        """
        return self._jcatalog.databaseExists(dbName)

    def listTables(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Table]:
        """Returns a list of tables/views in the specified database.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        dbName : str, optional
            name of the database to list the tables.

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        pattern : str, optional
            The pattern that the database name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Table`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> spark.range(1).createTempView("test_view")
        >>> spark.catalog.listTables()
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.listTables(pattern="test*")
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.listTables(pattern="table*")
        []

        >>> _ = spark.catalog.dropTempView("test_view")
        >>> spark.catalog.listTables()
        []
        """
        if dbName is None:
            dbName = self.currentDatabase()

        if pattern is None:
            iter = self._jcatalog.listTables(dbName).toLocalIterator()
        else:
            iter = self._jcatalog.listTables(dbName, pattern).toLocalIterator()
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
            name of the table to get.

            .. versionchanged:: 3.4.0
               Allow `tableName` to be qualified with catalog name.

        Returns
        -------
        :class:`Table`
            The table found by the name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.getTable("tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.getTable("default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getTable("spark_catalog.default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> _ = spark.sql("DROP TABLE tbl1")

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.getTable("tbl1")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
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

    def listFunctions(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Function]:
        """
        Returns a list of functions registered in the specified database.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str, optional
            name of the database to list the functions.
            ``dbName`` can be qualified with catalog name.
        pattern : str, optional
            The pattern that the function name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Function`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.listFunctions()
        [Function(name=...

        >>> spark.catalog.listFunctions(pattern="to_*")
        [Function(name=...

        >>> spark.catalog.listFunctions(pattern="*not_existing_func*")
        []
        """
        if dbName is None:
            dbName = self.currentDatabase()
        if pattern is None:
            iter = self._jcatalog.listFunctions(dbName).toLocalIterator()
        else:
            iter = self._jcatalog.listFunctions(dbName, pattern).toLocalIterator()
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

            .. versionchanged:: 3.4.0
               Allow ``functionName`` to be qualified with catalog name

        dbName : str, optional
            name of the database to check function existence in.

        Returns
        -------
        bool
            Indicating whether the function exists

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.functionExists("count")
        True

        Using the fully qualified name for function name.

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
        functionName : str
            name of the function to check existence.

        Returns
        -------
        :class:`Function`
            The function found by the name.

        Examples
        --------
        >>> _ = spark.sql(
        ...     "CREATE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
        >>> spark.catalog.getFunction("my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name for function name.

        >>> spark.catalog.getFunction("default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getFunction("spark_catalog.default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Throw an analysis exception when the function does not exists.

        >>> spark.catalog.getFunction("my_func2")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
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

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to list columns.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, optional
            name of the database to find the table to list columns.

        Returns
        -------
        list
            A list of :class:`Column`.

        Notes
        -----
        The order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.

        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tblA (name STRING, age INT) USING parquet")
        >>> spark.catalog.listColumns("tblA")
        [Column(name='name', description=None, dataType='string', nullable=True, ...
        >>> _ = spark.sql("DROP TABLE tblA")
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
                    isCluster=jcolumn.isCluster(),
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
            name of the table to check existence.
            If no database is specified, first try to treat ``tableName`` as a
            multi-layer-namespace identifier, then try ``tableName`` as a normal table
            name in the current database if necessary.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, optional
            name of the database to check table existence in.

        Returns
        -------
        bool
            Indicating whether the table/view exists

        Examples
        --------
        This function can check if a table is defined or not:

        >>> spark.catalog.tableExists("unexisting_table")
        False
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.tableExists("tbl1")
        True

        Using the fully qualified names for tables.

        >>> spark.catalog.tableExists("default.tbl1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.tableExists("tbl1", "default")
        True
        >>> _ = spark.sql("DROP TABLE tbl1")

        Check if views exist:

        >>> spark.catalog.tableExists("view1")
        False
        >>> _ = spark.sql("CREATE VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True

        Using the fully qualified names for views.

        >>> spark.catalog.tableExists("default.view1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.view1")
        True
        >>> spark.catalog.tableExists("view1", "default")
        True
        >>> _ = spark.sql("DROP VIEW view1")

        Check if temporary views exist:

        >>> _ = spark.sql("CREATE TEMPORARY VIEW view1 AS SELECT 1")
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
    ) -> "DataFrame":
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
    ) -> "DataFrame":
        """Creates a table based on the dataset in a data source.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        tableName : str
            name of the table to create.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name.

        path : str, optional
            the path in which the data for this table exists.
            When ``path`` is specified, an external table is
            created from the data at the given path. Otherwise a managed table is created.
        source : str, optional
            the source of this table such as 'parquet, 'orc', etc.
            If ``source`` is not specified, the default data source configured by
            ``spark.sql.sources.default`` will be used.
        schema : class:`StructType`, optional
            the schema for this table.
        description : str, optional
            the description of this table.

            .. versionchanged:: 3.1.0
                Added the ``description`` parameter.

        **options : dict, optional
            extra options to specify in the table.

        Returns
        -------
        :class:`DataFrame`
            The DataFrame associated with the table.

        Examples
        --------
        Creating a managed table.

        >>> _ = spark.catalog.createTable("tbl1", schema=spark.range(1).schema, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl1")

        Creating an external table

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="createTable") as d:
        ...     _ = spark.catalog.createTable(
        ...         "tbl2", schema=spark.range(1).schema, path=d, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl2")
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
                raise PySparkTypeError(
                    errorClass="NOT_STRUCT",
                    messageParameters={
                        "arg_name": "schema",
                        "arg_type": type(schema).__name__,
                    },
                )
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jcatalog.createTable(tableName, source, scala_datatype, description, options)
        return DataFrame(df, self._sparkSession)

    def dropTempView(self, viewName: str) -> bool:
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        viewName : str
            name of the temporary view to drop.

        Returns
        -------
        bool
            If the temporary view was successfully dropped or not.

            .. versionadded:: 2.1.0
                The return type of this method was ``None`` in Spark 2.0, but changed to ``bool``
                in Spark 2.1.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")

        Dropping the temporary view.

        >>> spark.catalog.dropTempView("my_table")
        True

        Throw an exception if the temporary view does not exists.

        >>> spark.table("my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.dropTempView(viewName)

    def dropGlobalTempView(self, viewName: str) -> bool:
        """Drops the global temporary view with the given view name in the catalog.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        viewName : str
            name of the global view to drop.

        Returns
        -------
        bool
            If the global view was successfully dropped or not.

        Notes
        -----
        If the view has been cached before, then it will also be uncached.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")

        Dropping the global view.

        >>> spark.catalog.dropGlobalTempView("my_table")
        True

        Throw an exception if the global view does not exists.

        >>> spark.table("global_temp.my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self._jcatalog.dropGlobalTempView(viewName)

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional["DataTypeOrString"] = None
    ) -> "UserDefinedFunctionLike":
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 2.0.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.register` instead.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.
        """
        warnings.warn("Deprecated in 2.3.0. Use spark.udf.register instead.", FutureWarning)
        return self._sparkSession.udf.register(name, f, returnType)

    def isCached(self, tableName: str) -> bool:
        """
        Returns true if the table is currently cached in-memory.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Returns
        -------
        bool

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        True

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.isCached("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.isCached("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        return self._jcatalog.isCached(tableName)

    def cacheTable(self, tableName: str, storageLevel: Optional[StorageLevel] = None) -> None:
        """Caches the specified table in-memory or with given storage level.
        Default MEMORY_AND_DISK.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        storageLevel : :class:`StorageLevel`, optional
            storage level to set for persistence.

            .. versionchanged:: 3.5.0
                Allow to specify storage level.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")

        or

        >>> spark.catalog.cacheTable("tbl1", StorageLevel.OFF_HEAP)

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.cacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.cacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        if storageLevel:
            javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
            self._jcatalog.cacheTable(tableName, javaStorageLevel)
        else:
            self._jcatalog.cacheTable(tableName)

    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        False

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.uncacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.uncacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._jcatalog.uncacheTable(tableName)

    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.clearCache()
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._jcatalog.clearCache()

    def refreshTable(self, tableName: str) -> None:
        """Invalidates and refreshes all the cached data and metadata of the given table.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="refreshTable") as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.table("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.table("tbl1").count()
        1

        After refreshing the table, it shows 0 because the data does not exist anymore.

        >>> spark.catalog.refreshTable("tbl1")
        >>> spark.table("tbl1").count()
        0

        Using the fully qualified name for the table.

        >>> spark.catalog.refreshTable("spark_catalog.default.tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._jcatalog.refreshTable(tableName)

    def recoverPartitions(self, tableName: str) -> None:
        """Recovers all the partitions of the given table and updates the catalog.

        .. versionadded:: 2.1.1

        Parameters
        ----------
        tableName : str
            name of the table to get.

        Notes
        -----
        Only works with a partitioned table, and not a view.

        Examples
        --------
        The example below creates a partitioned table against the existing directory of
        the partitioned table. After that, it recovers the partitions.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="recoverPartitions") as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     spark.range(1).selectExpr(
        ...         "id as key", "id as value").write.partitionBy("key").mode("overwrite").save(d)
        ...     _ = spark.sql(
        ...          "CREATE TABLE tbl1 (key LONG, value LONG)"
        ...          "USING parquet OPTIONS (path '{}') PARTITIONED BY (key)".format(d))
        ...     spark.table("tbl1").show()
        ...     spark.catalog.recoverPartitions("tbl1")
        ...     spark.table("tbl1").show()
        +-----+---+
        |value|key|
        +-----+---+
        +-----+---+
        +-----+---+
        |value|key|
        +-----+---+
        |    0|  0|
        +-----+---+
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._jcatalog.recoverPartitions(tableName)

    def refreshByPath(self, path: str) -> None:
        """Invalidates and refreshes all the cached data (and the associated metadata) for any
        DataFrame that contains the given data source path.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        path : str
            the path to refresh the cache.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="refreshByPath") as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.table("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.table("tbl1").count()
        1

        After refreshing the table by path, it shows 0 because the data does not exist anymore.

        >>> spark.catalog.refreshByPath(d)
        >>> spark.table("tbl1").count()
        0

        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._jcatalog.refreshByPath(path)


def _test() -> None:
    import os
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    globs["spark"] = (
        SparkSession.builder.master("local[4]").appName("sql.catalog tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.catalog,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )
    globs["spark"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
