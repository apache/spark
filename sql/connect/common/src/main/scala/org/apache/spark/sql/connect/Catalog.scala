/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog
import org.apache.spark.sql.catalog.{CatalogMetadata, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{PrimitiveBooleanEncoder, StringEncoder}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, StorageLevelProtoConverter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

class Catalog(sparkSession: SparkSession) extends catalog.Catalog {

  /**
   * Returns the current default database in this session.
   *
   * @since 3.5.0
   */
  override def currentDatabase: String =
    sparkSession
      .newDataset(StringEncoder) { builder =>
        builder.getCatalogBuilder.getCurrentDatabaseBuilder
      }
      .head()

  /**
   * Sets the current database (namespace) in this session.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  override def setCurrentDatabase(dbName: String): Unit = {
    // we assume `dbName` will not include the catalog name. e.g. if you call
    // `setCurrentDatabase("catalog.db")`, it will search for a database 'catalog.db' in the current
    // catalog.
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getSetCurrentDatabaseBuilder.setDbName(dbName)
    }
  }

  /**
   * Returns a list of databases (namespaces) available within the current catalog.
   *
   * @since 3.5.0
   */
  override def listDatabases(): Dataset[Database] = {
    sparkSession.newDataset(Catalog.databaseEncoder) { builder =>
      builder.getCatalogBuilder.getListDatabasesBuilder
    }
  }

  /**
   * Returns a list of databases (namespaces) which name match the specify pattern and available
   * within the current catalog.
   *
   * @since 3.5.0
   */
  override def listDatabases(pattern: String): Dataset[Database] = {
    sparkSession.newDataset(Catalog.databaseEncoder) { builder =>
      builder.getCatalogBuilder.getListDatabasesBuilder.setPattern(pattern)
    }
  }

  /**
   * Returns a list of tables/views in the current database (namespace). This includes all
   * temporary views.
   *
   * @since 3.5.0
   */
  override def listTables(): Dataset[Table] = {
    listTables(currentDatabase)
  }

  /**
   * Returns a list of tables/views in the specified database (namespace) (the name can be
   * qualified with catalog). This includes all temporary views.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String): Dataset[Table] = {
    sparkSession.newDataset(Catalog.tableEncoder) { builder =>
      builder.getCatalogBuilder.getListTablesBuilder.setDbName(dbName)
    }
  }

  /**
   * Returns a list of tables/views in the specified database (namespace) which name match the
   * specify pattern (the name can be qualified with catalog). This includes all temporary views.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String, pattern: String): Dataset[Table] = {
    sparkSession.newDataset(Catalog.tableEncoder) { builder =>
      builder.getCatalogBuilder.getListTablesBuilder.setDbName(dbName).setPattern(pattern)
    }
  }

  /**
   * Returns a list of functions registered in the current database (namespace). This includes all
   * temporary functions.
   *
   * @since 3.5.0
   */
  override def listFunctions(): Dataset[Function] = {
    listFunctions(currentDatabase)
  }

  /**
   * Returns a list of functions registered in the specified database (namespace) (the name can be
   * qualified with catalog). This includes all built-in and temporary functions.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String): Dataset[Function] = {
    sparkSession.newDataset(Catalog.functionEncoder) { builder =>
      builder.getCatalogBuilder.getListFunctionsBuilder.setDbName(dbName)
    }
  }

  /**
   * Returns a list of functions registered in the specified database (namespace) which name match
   * the specify pattern (the name can be qualified with catalog). This includes all built-in and
   * temporary functions.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String, pattern: String): Dataset[Function] = {
    sparkSession.newDataset(Catalog.functionEncoder) { builder =>
      builder.getCatalogBuilder.getListFunctionsBuilder.setDbName(dbName).setPattern(pattern)
    }
  }

  /**
   * Returns a list of columns for the given table/view or temporary view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 3.5.0
   */
  @throws[AnalysisException]("database or table does not exist")
  override def listColumns(tableName: String): Dataset[Column] = {
    sparkSession.newDataset(Catalog.columnEncoder) { builder =>
      builder.getCatalogBuilder.getListColumnsBuilder.setTableName(tableName)
    }
  }

  /**
   * Returns a list of columns for the given table/view in the specified database under the Hive
   * Metastore.
   *
   * To list columns for table/view in other catalogs, please use `listColumns(tableName)` with
   * qualified table/view name instead.
   *
   * @param dbName
   *   is an unqualified name that designates a database.
   * @param tableName
   *   is an unqualified name that designates a table/view.
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  override def listColumns(dbName: String, tableName: String): Dataset[Column] = {
    sparkSession.newDataset(Catalog.columnEncoder) { builder =>
      builder.getCatalogBuilder.getListColumnsBuilder
        .setTableName(tableName)
        .setDbName(dbName)
    }
  }

  /**
   * Get the database (namespace) with the specified name (can be qualified with catalog). This
   * throws an AnalysisException when the database (namespace) cannot be found.
   *
   * @since 3.5.0
   */
  override def getDatabase(dbName: String): Database = {
    sparkSession
      .newDataset(Catalog.databaseEncoder) { builder =>
        builder.getCatalogBuilder.getGetDatabaseBuilder.setDbName(dbName)
      }
      .head()
  }

  /**
   * Get the table or view with the specified name. This table can be a temporary view or a
   * table/view. This throws an AnalysisException when no Table can be found.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 3.5.0
   */
  override def getTable(tableName: String): Table = {
    sparkSession
      .newDataset(Catalog.tableEncoder) { builder =>
        builder.getCatalogBuilder.getGetTableBuilder.setTableName(tableName)
      }
      .head()
  }

  /**
   * Get the table or view with the specified name in the specified database under the Hive
   * Metastore. This throws an AnalysisException when no Table can be found.
   *
   * To get table/view in other catalogs, please use `getTable(tableName)` with qualified
   * table/view name instead.
   *
   * @since 3.5.0
   */
  override def getTable(dbName: String, tableName: String): Table = {
    sparkSession
      .newDataset(Catalog.tableEncoder) { builder =>
        builder.getCatalogBuilder.getGetTableBuilder
          .setTableName(tableName)
          .setDbName(dbName)
      }
      .head()
  }

  /**
   * Get the function with the specified name. This function can be a temporary function or a
   * function. This throws an AnalysisException when the function cannot be found.
   *
   * @param functionName
   *   is either a qualified or unqualified name that designates a function. It follows the same
   *   resolution rule with SQL: search for built-in/temp functions first then functions in the
   *   current database (namespace).
   * @since 3.5.0
   */
  override def getFunction(functionName: String): Function = {
    sparkSession
      .newDataset(Catalog.functionEncoder) { builder =>
        builder.getCatalogBuilder.getGetFunctionBuilder.setFunctionName(functionName)
      }
      .head()
  }

  /**
   * Get the function with the specified name in the specified database under the Hive Metastore.
   * This throws an AnalysisException when the function cannot be found.
   *
   * To get functions in other catalogs, please use `getFunction(functionName)` with qualified
   * function name instead.
   *
   * @param dbName
   *   is an unqualified name that designates a database.
   * @param functionName
   *   is an unqualified name that designates a function in the specified database
   * @since 3.5.0
   */
  override def getFunction(dbName: String, functionName: String): Function = {
    sparkSession
      .newDataset(Catalog.functionEncoder) { builder =>
        builder.getCatalogBuilder.getGetFunctionBuilder
          .setFunctionName(functionName)
          .setDbName(dbName)
      }
      .head()
  }

  /**
   * Check if the database (namespace) with the specified name exists (the name can be qualified
   * with catalog).
   *
   * @since 3.5.0
   */
  override def databaseExists(dbName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getDatabaseExistsBuilder.setDbName(dbName)
      }
      .head()
  }

  /**
   * Check if the table or view with the specified name exists. This can either be a temporary
   * view or a table/view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 3.5.0
   */
  override def tableExists(tableName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getTableExistsBuilder.setTableName(tableName)
      }
      .head()
  }

  /**
   * Check if the table or view with the specified name exists in the specified database under the
   * Hive Metastore.
   *
   * To check existence of table/view in other catalogs, please use `tableExists(tableName)` with
   * qualified table/view name instead.
   *
   * @param dbName
   *   is an unqualified name that designates a database.
   * @param tableName
   *   is an unqualified name that designates a table.
   * @since 3.5.0
   */
  override def tableExists(dbName: String, tableName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getTableExistsBuilder
          .setTableName(tableName)
          .setDbName(dbName)
      }
      .head()
  }

  /**
   * Check if the function with the specified name exists. This can either be a temporary function
   * or a function.
   *
   * @param functionName
   *   is either a qualified or unqualified name that designates a function. It follows the same
   *   resolution rule with SQL: search for built-in/temp functions first then functions in the
   *   current database (namespace).
   * @since 3.5.0
   */
  override def functionExists(functionName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getFunctionExistsBuilder.setFunctionName(functionName)
      }
      .head()
  }

  /**
   * Check if the function with the specified name exists in the specified database under the Hive
   * Metastore.
   *
   * To check existence of functions in other catalogs, please use `functionExists(functionName)`
   * with qualified function name instead.
   *
   * @param dbName
   *   is an unqualified name that designates a database.
   * @param functionName
   *   is an unqualified name that designates a function.
   * @since 3.5.0
   */
  override def functionExists(dbName: String, functionName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getFunctionExistsBuilder
          .setFunctionName(functionName)
          .setDbName(dbName)
      }
      .head()
  }

  /**
   * Creates a table from the given path and returns the corresponding DataFrame. It will use the
   * default data source configured by spark.sql.sources.default.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(tableName: String, path: String): DataFrame = {
    sparkSession.newDataFrame { builder =>
      builder.getCatalogBuilder.getCreateTableBuilder
        .setTableName(tableName)
        .setSchema(DataTypeProtoConverter.toConnectProtoType(new StructType))
        .setDescription("")
        .putOptions("path", path)
    }
  }

  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(tableName: String, path: String, source: String): DataFrame = {
    createTable(tableName, source, Map("path" -> path))
  }

  /**
   * (Scala-specific) Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, options)
  }

  /**
   * (Scala-specific) Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, description, options)
  }

  /**
   * (Scala-specific) Create a table based on the dataset in a data source, a schema and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    createTable(
      tableName = tableName,
      source = source,
      schema = schema,
      description = "",
      options = options)
  }

  /**
   * (Scala-specific) Create a table based on the dataset in a data source, a schema and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val createTableBuilder = builder.getCatalogBuilder.getCreateTableBuilder
        .setTableName(tableName)
        .setSource(source)
        .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
        .setDescription(description)
      options.foreach { case (k, v) =>
        createTableBuilder.putOptions(k, v)
      }
    }
  }

  /**
   * Drops the local temporary view with the given view name in the catalog. If the view has been
   * cached before, then it will also be uncached.
   *
   * Local temporary view is session-scoped. Its lifetime is the lifetime of the session that
   * created it, i.e. it will be automatically dropped when the session terminates. It's not tied
   * to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
   *
   * Note that, the return type of this method was Unit in Spark 2.0, but changed to Boolean in
   * Spark 2.1.
   *
   * @param viewName
   *   the name of the temporary view to be dropped.
   * @return
   *   true if the view is dropped successfully, false otherwise.
   * @since 3.5.0
   */
  override def dropTempView(viewName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getDropTempViewBuilder.setViewName(viewName)
      }
      .head()
  }

  /**
   * Drops the global temporary view with the given view name in the catalog. If the view has been
   * cached before, then it will also be uncached.
   *
   * Global temporary view is cross-session. Its lifetime is the lifetime of the Spark
   * application, i.e. it will be automatically dropped when the application terminates. It's tied
   * to a system preserved database `global_temp`, and we must use the qualified name to refer a
   * global temp view, e.g. `SELECT * FROM global_temp.view1`.
   *
   * @param viewName
   *   the unqualified name of the temporary view to be dropped.
   * @return
   *   true if the view is dropped successfully, false otherwise.
   * @since 3.5.0
   */
  override def dropGlobalTempView(viewName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getDropGlobalTempViewBuilder.setViewName(viewName)
      }
      .head()
  }

  /**
   * Recovers all the partitions in the directory of a table and update the catalog. Only works
   * with a partitioned table, and not a view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.5.0
   */
  override def recoverPartitions(tableName: String): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getRecoverPartitionsBuilder.setTableName(tableName)
    }
  }

  /**
   * Returns true if the table is currently cached in-memory.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 3.5.0
   */
  override def isCached(tableName: String): Boolean = {
    sparkSession
      .newDataset(PrimitiveBooleanEncoder) { builder =>
        builder.getCatalogBuilder.getIsCachedBuilder.setTableName(tableName)
      }
      .head()
  }

  /**
   * Caches the specified table in-memory.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 3.5.0
   */
  override def cacheTable(tableName: String): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getCacheTableBuilder.setTableName(tableName)
    }
  }

  /**
   * Caches the specified table or view with the given storage level.
   *
   * @group cachemgmt
   * @since 3.4.0
   */
  override def cacheTable(tableName: String, storageLevel: StorageLevel): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getCacheTableBuilder
        .setTableName(tableName)
        .setStorageLevel(StorageLevelProtoConverter.toConnectProtoType(storageLevel))
    }
  }

  /**
   * Removes the specified table from the in-memory cache.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 3.5.0
   */
  override def uncacheTable(tableName: String): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getUncacheTableBuilder.setTableName(tableName)
    }
  }

  /**
   * Removes all cached tables from the in-memory cache.
   *
   * @since 3.5.0
   */
  override def clearCache(): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getClearCacheBuilder
    }
  }

  /**
   * Invalidates and refreshes all the cached data and metadata of the given table. For
   * performance reasons, Spark SQL or the external data source library it uses might cache
   * certain metadata about a table, such as the location of blocks. When those change outside of
   * Spark SQL, users should call this function to invalidate the cache.
   *
   * If this table is cached as an InMemoryRelation, drop the original cached version and make the
   * new version cached lazily.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 3.5.0
   */
  override def refreshTable(tableName: String): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getRefreshTableBuilder.setTableName(tableName)
    }
  }

  /**
   * Invalidates and refreshes all the cached data (and the associated metadata) for any `Dataset`
   * that contains the given data source path. Path matching is by prefix, i.e. "/" would
   * invalidate everything that is cached.
   *
   * @since 3.5.0
   */
  override def refreshByPath(path: String): Unit = {
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getRefreshByPathBuilder.setPath(path)
    }
  }

  /**
   * Returns the current catalog in this session.
   *
   * @since 3.5.0
   */
  override def currentCatalog(): String = sparkSession
    .newDataset(StringEncoder) { builder =>
      builder.getCatalogBuilder.getCurrentCatalogBuilder
    }
    .head()

  /**
   * Sets the current catalog in this session.
   *
   * @since 3.5.0
   */
  override def setCurrentCatalog(catalogName: String): Unit =
    sparkSession.execute { builder =>
      builder.getCatalogBuilder.getSetCurrentCatalogBuilder.setCatalogName(catalogName)
    }

  /**
   * Returns a list of catalogs available in this session.
   *
   * @since 3.5.0
   */
  override def listCatalogs(): Dataset[CatalogMetadata] =
    sparkSession
      .newDataset(Catalog.catalogEncoder) { builder =>
        builder.getCatalogBuilder.getListCatalogsBuilder
      }

  /**
   * Returns a list of catalogs which name match the specify pattern and available in this
   * session.
   *
   * @since 3.5.0
   */
  override def listCatalogs(pattern: String): Dataset[CatalogMetadata] =
    sparkSession
      .newDataset(Catalog.catalogEncoder) { builder =>
        builder.getCatalogBuilder.getListCatalogsBuilder.setPattern(pattern)
      }
}

private object Catalog {
  private val databaseEncoder: AgnosticEncoder[Database] = ScalaReflection
    .encoderFor(ScalaReflection.localTypeOf[Database])
    .asInstanceOf[AgnosticEncoder[Database]]

  private val catalogEncoder: AgnosticEncoder[CatalogMetadata] = ScalaReflection
    .encoderFor(ScalaReflection.localTypeOf[CatalogMetadata])
    .asInstanceOf[AgnosticEncoder[CatalogMetadata]]

  private val tableEncoder: AgnosticEncoder[Table] = ScalaReflection
    .encoderFor(ScalaReflection.localTypeOf[Table])
    .asInstanceOf[AgnosticEncoder[Table]]

  private val functionEncoder: AgnosticEncoder[Function] = ScalaReflection
    .encoderFor(ScalaReflection.localTypeOf[Function])
    .asInstanceOf[AgnosticEncoder[Function]]

  private val columnEncoder: AgnosticEncoder[Column] = ScalaReflection
    .encoderFor(ScalaReflection.localTypeOf[Column])
    .asInstanceOf[AgnosticEncoder[Column]]
}
