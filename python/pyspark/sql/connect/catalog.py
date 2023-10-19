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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Any, Callable, List, Optional, TYPE_CHECKING

import warnings
import pandas as pd

from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StructType
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.catalog import (
    Catalog as PySparkCatalog,
    CatalogMetadata,
    Database,
    Table,
    Function,
    Column,
)
from pyspark.sql.connect import plan

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect._typing import DataTypeOrString, UserDefinedFunctionLike


class Catalog:
    def __init__(self, sparkSession: "SparkSession") -> None:
        self._sparkSession = sparkSession

    def _execute_and_fetch(self, catalog: plan.LogicalPlan) -> pd.DataFrame:
        pdf = DataFrame.withPlan(catalog, session=self._sparkSession).toPandas()
        assert pdf is not None
        return pdf

    def currentCatalog(self) -> str:
        pdf = self._execute_and_fetch(plan.CurrentCatalog())
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    currentCatalog.__doc__ = PySparkCatalog.currentCatalog.__doc__

    def setCurrentCatalog(self, catalogName: str) -> None:
        self._execute_and_fetch(plan.SetCurrentCatalog(catalog_name=catalogName))

    setCurrentCatalog.__doc__ = PySparkCatalog.setCurrentCatalog.__doc__

    def listCatalogs(self, pattern: Optional[str] = None) -> List[CatalogMetadata]:
        pdf = self._execute_and_fetch(plan.ListCatalogs(pattern=pattern))
        return [
            CatalogMetadata(name=row.iloc[0], description=row.iloc[1]) for _, row in pdf.iterrows()
        ]

    listCatalogs.__doc__ = PySparkCatalog.listCatalogs.__doc__

    def currentDatabase(self) -> str:
        pdf = self._execute_and_fetch(plan.CurrentDatabase())
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    currentDatabase.__doc__ = PySparkCatalog.currentDatabase.__doc__

    def setCurrentDatabase(self, dbName: str) -> None:
        self._execute_and_fetch(plan.SetCurrentDatabase(db_name=dbName))

    setCurrentDatabase.__doc__ = PySparkCatalog.setCurrentDatabase.__doc__

    def listDatabases(self, pattern: Optional[str] = None) -> List[Database]:
        pdf = self._execute_and_fetch(plan.ListDatabases(pattern=pattern))
        return [
            Database(
                name=row.iloc[0],
                catalog=row.iloc[1],
                description=row.iloc[2],
                locationUri=row.iloc[3],
            )
            for _, row in pdf.iterrows()
        ]

    listDatabases.__doc__ = PySparkCatalog.listDatabases.__doc__

    def getDatabase(self, dbName: str) -> Database:
        pdf = self._execute_and_fetch(plan.GetDatabase(db_name=dbName))
        assert pdf is not None
        row = pdf.iloc[0]
        return Database(
            name=row[0],
            catalog=row[1],
            description=row[2],
            locationUri=row[3],
        )

    getDatabase.__doc__ = PySparkCatalog.getDatabase.__doc__

    def databaseExists(self, dbName: str) -> bool:
        pdf = self._execute_and_fetch(plan.DatabaseExists(db_name=dbName))
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    databaseExists.__doc__ = PySparkCatalog.databaseExists.__doc__

    def listTables(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Table]:
        pdf = self._execute_and_fetch(plan.ListTables(db_name=dbName, pattern=pattern))
        return [
            Table(
                name=row.iloc[0],
                catalog=row.iloc[1],
                # If None, returns None.
                namespace=None if row.iloc[2] is None else list(row.iloc[2]),
                description=row.iloc[3],
                tableType=row.iloc[4],
                isTemporary=row.iloc[5],
            )
            for _, row in pdf.iterrows()
        ]

    listTables.__doc__ = PySparkCatalog.listTables.__doc__

    def getTable(self, tableName: str) -> Table:
        pdf = self._execute_and_fetch(plan.GetTable(table_name=tableName))
        assert pdf is not None
        row = pdf.iloc[0]
        return Table(
            name=row.iloc[0],
            catalog=row.iloc[1],
            # If None, returns None.
            namespace=None if row.iloc[2] is None else list(row.iloc[2]),
            description=row.iloc[3],
            tableType=row.iloc[4],
            isTemporary=row.iloc[5],
        )

    getTable.__doc__ = PySparkCatalog.getTable.__doc__

    def listFunctions(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Function]:
        pdf = self._execute_and_fetch(plan.ListFunctions(db_name=dbName, pattern=pattern))
        return [
            Function(
                name=row.iloc[0],
                catalog=row.iloc[1],
                # If None, returns None.
                namespace=None if row.iloc[2] is None else list(row.iloc[2]),
                description=row.iloc[3],
                className=row.iloc[4],
                isTemporary=row.iloc[5],
            )
            for _, row in pdf.iterrows()
        ]

    listFunctions.__doc__ = PySparkCatalog.listFunctions.__doc__

    def functionExists(self, functionName: str, dbName: Optional[str] = None) -> bool:
        pdf = self._execute_and_fetch(
            plan.FunctionExists(function_name=functionName, db_name=dbName)
        )
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    functionExists.__doc__ = PySparkCatalog.functionExists.__doc__

    def getFunction(self, functionName: str) -> Function:
        pdf = self._execute_and_fetch(plan.GetFunction(function_name=functionName))
        assert pdf is not None
        row = pdf.iloc[0]
        return Function(
            name=row.iloc[0],
            catalog=row.iloc[1],
            # If None, returns None.
            namespace=None if row.iloc[2] is None else list(row.iloc[2]),
            description=row.iloc[3],
            className=row.iloc[4],
            isTemporary=row.iloc[5],
        )

    getFunction.__doc__ = PySparkCatalog.getFunction.__doc__

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[Column]:
        pdf = self._execute_and_fetch(plan.ListColumns(table_name=tableName, db_name=dbName))
        return [
            Column(
                name=row.iloc[0],
                description=row.iloc[1],
                dataType=row.iloc[2],
                nullable=row.iloc[3],
                isPartition=row.iloc[4],
                isBucket=row.iloc[5],
            )
            for _, row in pdf.iterrows()
        ]

    listColumns.__doc__ = PySparkCatalog.listColumns.__doc__

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        pdf = self._execute_and_fetch(plan.TableExists(table_name=tableName, db_name=dbName))
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    tableExists.__doc__ = PySparkCatalog.tableExists.__doc__

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str,
    ) -> DataFrame:
        catalog = plan.CreateExternalTable(
            table_name=tableName,
            path=path,  # type: ignore[arg-type]
            source=source,
            schema=schema,
            options=options,
        )
        df = DataFrame.withPlan(catalog, session=self._sparkSession)
        df.toPandas()  # Eager execution.
        return df

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
        catalog = plan.CreateTable(
            table_name=tableName,
            path=path,  # type: ignore[arg-type]
            source=source,
            schema=schema,
            description=description,
            options=options,
        )
        df = DataFrame.withPlan(catalog, session=self._sparkSession)
        df.toPandas()  # Eager execution.
        return df

    createTable.__doc__ = PySparkCatalog.createTable.__doc__

    def dropTempView(self, viewName: str) -> bool:
        pdf = self._execute_and_fetch(plan.DropTempView(view_name=viewName))
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    dropTempView.__doc__ = PySparkCatalog.dropTempView.__doc__

    def dropGlobalTempView(self, viewName: str) -> bool:
        pdf = self._execute_and_fetch(plan.DropGlobalTempView(view_name=viewName))
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    dropGlobalTempView.__doc__ = PySparkCatalog.dropGlobalTempView.__doc__

    def isCached(self, tableName: str) -> bool:
        pdf = self._execute_and_fetch(plan.IsCached(table_name=tableName))
        assert pdf is not None
        return pdf.iloc[0].iloc[0]

    isCached.__doc__ = PySparkCatalog.isCached.__doc__

    def cacheTable(self, tableName: str, storageLevel: Optional[StorageLevel] = None) -> None:
        self._execute_and_fetch(plan.CacheTable(table_name=tableName, storage_level=storageLevel))

    cacheTable.__doc__ = PySparkCatalog.cacheTable.__doc__

    def uncacheTable(self, tableName: str) -> None:
        self._execute_and_fetch(plan.UncacheTable(table_name=tableName))

    uncacheTable.__doc__ = PySparkCatalog.uncacheTable.__doc__

    def clearCache(self) -> None:
        self._execute_and_fetch(plan.ClearCache())

    clearCache.__doc__ = PySparkCatalog.clearCache.__doc__

    def refreshTable(self, tableName: str) -> None:
        self._execute_and_fetch(plan.RefreshTable(table_name=tableName))

    refreshTable.__doc__ = PySparkCatalog.refreshTable.__doc__

    def recoverPartitions(self, tableName: str) -> None:
        self._execute_and_fetch(plan.RecoverPartitions(table_name=tableName))

    recoverPartitions.__doc__ = PySparkCatalog.recoverPartitions.__doc__

    def refreshByPath(self, path: str) -> None:
        self._execute_and_fetch(plan.RefreshByPath(path=path))

    refreshByPath.__doc__ = PySparkCatalog.refreshByPath.__doc__

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional["DataTypeOrString"] = None
    ) -> "UserDefinedFunctionLike":
        warnings.warn("Deprecated in 2.3.0. Use spark.udf.register instead.", FutureWarning)
        return self._sparkSession.udf.register(name, f, returnType)

    registerFunction.__doc__ = PySparkCatalog.registerFunction.__doc__


Catalog.__doc__ = PySparkCatalog.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.catalog

    globs = pyspark.sql.connect.catalog.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.catalog tests").remote("local[4]").getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.catalog,
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
