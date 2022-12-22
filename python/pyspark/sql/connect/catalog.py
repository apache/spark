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
import regex
from typing import Optional, TYPE_CHECKING, List, Callable, Any

from pyspark.pandas import DataFrame
from pyspark.sql.catalog import (
    Database,
    CatalogMetadata,
    Table,
    Function,
    Column,
    Catalog as PySparkCatalog,
)
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


class Catalog:
    """
    User-facing catalog API, accessible through `SparkSession.catalog`.

    .. versionchanged:: 3.4.0
        Support Spark Connect.
    """

    def _quote_if_needed(self, part: str):
        """
        Copied from `org.apache.spark.sql.catalyst.util.quoteIfNeeded`.
        """
        if regex.match(r"[a-zA-Z0-9_]+", part) and not regex.match(r"\d+", part):
            return part
        else:
            quoted = part.replace("`", "``")
            return f"`{quoted}`"

    def __init__(self, sparkSession: "SparkSession") -> None:
        self._sparkSession = sparkSession

    def currentCatalog(self) -> str:
        (catalog, _) = self._sparkSession.sql("SHOW CURRENT NAMESPACE").first()
        return catalog

    currentCatalog.__doc__ = PySparkCatalog.currentCatalog.__doc__

    def setCurrentCatalog(self, catalogName: str) -> None:
        self._sparkSession.sql(f"SET CATALOG {self._quote_if_needed(catalogName)}").count()

    setCurrentCatalog.__doc__ = PySparkCatalog.setCurrentCatalog.__doc__

    def listCatalogs(self) -> List[CatalogMetadata]:
        rows = self._sparkSession.sql("SHOW CATALOGS").collect()
        return [CatalogMetadata(name=row.catalog, description=None) for row in rows]

    listCatalogs.__doc__ = PySparkCatalog.listCatalogs.__doc__

    def currentDatabase(self) -> str:
        (catalog, database) = self._sparkSession.sql("SHOW CURRENT DATABASE").first()
        if catalog == "spark_catalog":
            # Default Spark session
            return database
        else:
            return f"`{self._quote_if_needed(catalog)}.{self._quote_if_needed(database)}`"

    currentDatabase.__doc__ = PySparkCatalog.currentDatabase.__doc__

    def setCurrentDatabase(self, dbName: str) -> None:
        self._sparkSession.sql(f"SET DATABASE {self._quote_if_needed(dbName)}").count()

    setCurrentDatabase.__doc__ = PySparkCatalog.setCurrentDatabase.__doc__

    def listDatabases(self) -> List[Database]:
        rows = self._sparkSession.sql("SHOW DATABASES").collect()
        return [self.getDatabase(row["namespace"]) for row in rows]

    listDatabases.__doc__ = PySparkCatalog.listDatabases.__doc__

    def getDatabase(self, dbName: str) -> Database:
        # The difference compared to the regular Catalog API: if `dbName` has
        # a dot in its name, it cannot be supported as a single database name for now.

        rows = self._sparkSession.sql(f"DESCRIBE DATABASE {dbName}").collect()
        return Database(
            name=rows[1].info_value,
            catalog=rows[0].info_value,
            description=rows[2].info_value,
            locationUri=rows[3].info_value,
        )

    getDatabase.__doc__ = PySparkCatalog.getDatabase.__doc__

    def databaseExists(self, dbName: str) -> bool:
        try:
            self._sparkSession.sql(f"DESCRIBE DATABASE {dbName}").count()
            return True
        except AnalysisException as e:
            if e.getErrorClass() == "SCHEMA_NOT_FOUND":
                return False
            else:
                raise

    databaseExists.__doc__ = PySparkCatalog.databaseExists.__doc__

    def listTables(self, dbName: Optional[str] = None) -> List[Table]:
        results = []
        qualified_dbname = dbName
        if qualified_dbname is None:
            qualified_dbname = self.currentDatabase()
        rows = self._sparkSession.sql(f"SHOW TABLES IN {qualified_dbname}").collect()
        for row in rows:
            namespace = row.namespace
            table_name = self._quote_if_needed(row.tableName)
            if namespace == "":
                self.getTable(table_name)
            else:
                self.getTable(f"{namespace}.{table_name}")
        return results

    listTables.__doc__ = PySparkCatalog.listTables.__doc__

    def getTable(self, tableName: str) -> Table:
        desc = self._sparkSession.sql(f"DESCRIBE TABLE {tableName}").collect()

        catalog = None
        table_name = None
        description = desc[0].comment
        dbname = ""
        table_type = "TEMPORARY"
        for r in desc:
            if r.col_name == "Catalog":
                catalog = r.data_type
            elif r.col_name == "Type":
                table_type = r.data_type
            elif r.col_name == "Database":
                dbname = r.data_type
            elif r.col_name == "Table":
                table_name = r.data_type

        if catalog == "spark_catalog":
            namespace = [dbname]
        else:
            namespace = [catalog, dbname]

        return Table(
            name=table_name,
            catalog=catalog,
            namespace=namespace,
            description=description,
            tableType=table_type,
            isTemporary=table_type == "TEMPORARY",
        )

    getTable.__doc__ = PySparkCatalog.getTable.__doc__

    def listFunctions(self, dbName: Optional[str] = None) -> List[Function]:
        # catalog, namespace unknown. 'description' have to be manually parsed.
        # Cannot specify Database name to list functions in SQL syntax.
        raise NotImplementedError()

    listFunctions.__doc__ = PySparkCatalog.listFunctions.__doc__

    def functionExists(self, functionName: str, dbName: Optional[str] = None) -> bool:
        # catalog, namespace unknown. 'description' have to be manually parsed.
        # Cannot specify Database name to list functions in SQL syntax.
        raise NotImplementedError()

    functionExists.__doc__ = PySparkCatalog.functionExists.__doc__

    def getFunction(self, functionName: str) -> Function:
        # catalog, namespace unknown. 'description' have to be manually parsed.
        # Cannot specify Database name to list functions in SQL syntax.
        raise NotImplementedError()

    getFunction.__doc__ = PySparkCatalog.getFunction.__doc__

    def listColumns(self, tableName: str) -> List[Column]:
        # nullable, isPartition and isBucket missing.
        raise NotImplementedError()

    listColumns.__doc__ = PySparkCatalog.listColumns.__doc__

    def tableExists(self, tableName: str) -> bool:
        try:
            self._sparkSession.sql(f"DESCRIBE TABLE {tableName}").count()
            return True
        except AnalysisException as e:
            if e.getErrorClass() == "TABLE_NOT_FOUND":
                return False
            else:
                raise

    tableExists.__doc__ = PySparkCatalog.tableExists.__doc__

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str,
    ) -> DataFrame:
        # Feasible
        pass

    createExternalTable.__doc__ = PySparkCatalog.createExternalTable.__doc__

    def createTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        description: Optional[str] = None,
        **options: str,
    ) -> DataFrame:
        # Feasible
        pass

    createTable.__doc__ = PySparkCatalog.createTable.__doc__

    def dropTempView(self, viewName: str) -> bool:
        # Feasible
        pass

    dropTempView.__doc__ = PySparkCatalog.dropTempView.__doc__

    def dropGlobalTempView(self, viewName: str) -> bool:
        # Feasible
        pass

    dropGlobalTempView.__doc__ = PySparkCatalog.dropGlobalTempView.__doc__

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional["DataType"] = None
    ) -> "UserDefinedFunctionLike":
        # Feasible
        pass

    registerFunction.__doc__ = PySparkCatalog.registerFunction.__doc__

    def isCached(self, tableName: str) -> bool:
        # No such feature in SQL
        raise NotImplementedError()

    isCached.__doc__ = PySparkCatalog.isCached.__doc__

    def cacheTable(self, tableName: str) -> None:
        # Feasible
        pass

    cacheTable.__doc__ = PySparkCatalog.cacheTable.__doc__

    def uncacheTable(self, tableName: str) -> None:
        # Feasible
        pass

    uncacheTable.__doc__ = PySparkCatalog.uncacheTable.__doc__

    def clearCache(self) -> None:
        # Feasible
        pass

    clearCache.__doc__ = PySparkCatalog.clearCache.__doc__

    def refreshTable(self, tableName: str) -> None:
        # Feasible
        pass

    refreshTable.__doc__ = PySparkCatalog.refreshTable.__doc__

    def recoverPartitions(self, tableName: str) -> None:
        # Feasible
        pass

    recoverPartitions.__doc__ = PySparkCatalog.recoverPartitions.__doc__

    def refreshByPath(self, path: str) -> None:
        # Feasible
        pass

    refreshByPath.__doc__ = PySparkCatalog.refreshByPath.__doc__
