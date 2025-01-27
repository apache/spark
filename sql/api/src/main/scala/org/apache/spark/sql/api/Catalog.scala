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

package org.apache.spark.sql.api

import scala.jdk.CollectionConverters._

import _root_.java.util

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalog.{CatalogMetadata, Column, Database, Function, Table}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * Catalog interface for Spark. To access this, use `SparkSession.catalog`.
 *
 * @since 2.0.0
 */
@Stable
abstract class Catalog {

  /**
   * Returns the current database (namespace) in this session.
   *
   * @since 2.0.0
   */
  def currentDatabase: String

  /**
   * Sets the current database (namespace) in this session.
   *
   * @since 2.0.0
   */
  def setCurrentDatabase(dbName: String): Unit

  /**
   * Returns a list of databases (namespaces) available within the current catalog.
   *
   * @since 2.0.0
   */
  def listDatabases(): Dataset[Database]

  /**
   * Returns a list of databases (namespaces) which name match the specify pattern and available
   * within the current catalog.
   *
   * @since 3.5.0
   */
  def listDatabases(pattern: String): Dataset[Database]

  /**
   * Returns a list of tables/views in the current database (namespace). This includes all
   * temporary views.
   *
   * @since 2.0.0
   */
  def listTables(): Dataset[Table]

  /**
   * Returns a list of tables/views in the specified database (namespace) (the name can be
   * qualified with catalog). This includes all temporary views.
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String): Dataset[Table]

  /**
   * Returns a list of tables/views in the specified database (namespace) which name match the
   * specify pattern (the name can be qualified with catalog). This includes all temporary views.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String, pattern: String): Dataset[Table]

  /**
   * Returns a list of functions registered in the current database (namespace). This includes all
   * temporary functions.
   *
   * @since 2.0.0
   */
  def listFunctions(): Dataset[Function]

  /**
   * Returns a list of functions registered in the specified database (namespace) (the name can be
   * qualified with catalog). This includes all built-in and temporary functions.
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String): Dataset[Function]

  /**
   * Returns a list of functions registered in the specified database (namespace) which name match
   * the specify pattern (the name can be qualified with catalog). This includes all built-in and
   * temporary functions.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String, pattern: String): Dataset[Function]

  /**
   * Returns a list of columns for the given table/view or temporary view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 2.0.0
   */
  @throws[AnalysisException]("table does not exist")
  def listColumns(tableName: String): Dataset[Column]

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
   * @since 2.0.0
   */
  @throws[AnalysisException]("database or table does not exist")
  def listColumns(dbName: String, tableName: String): Dataset[Column]

  /**
   * Get the database (namespace) with the specified name (can be qualified with catalog). This
   * throws an AnalysisException when the database (namespace) cannot be found.
   *
   * @since 2.1.0
   */
  @throws[AnalysisException]("database does not exist")
  def getDatabase(dbName: String): Database

  /**
   * Get the table or view with the specified name. This table can be a temporary view or a
   * table/view. This throws an AnalysisException when no Table can be found.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 2.1.0
   */
  @throws[AnalysisException]("table does not exist")
  def getTable(tableName: String): Table

  /**
   * Get the table or view with the specified name in the specified database under the Hive
   * Metastore. This throws an AnalysisException when no Table can be found.
   *
   * To get table/view in other catalogs, please use `getTable(tableName)` with qualified
   * table/view name instead.
   *
   * @since 2.1.0
   */
  @throws[AnalysisException]("database or table does not exist")
  def getTable(dbName: String, tableName: String): Table

  /**
   * Get the function with the specified name. This function can be a temporary function or a
   * function. This throws an AnalysisException when the function cannot be found.
   *
   * @param functionName
   *   is either a qualified or unqualified name that designates a function. It follows the same
   *   resolution rule with SQL: search for built-in/temp functions first then functions in the
   *   current database (namespace).
   * @since 2.1.0
   */
  @throws[AnalysisException]("function does not exist")
  def getFunction(functionName: String): Function

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
   * @since 2.1.0
   */
  @throws[AnalysisException]("database or function does not exist")
  def getFunction(dbName: String, functionName: String): Function

  /**
   * Check if the database (namespace) with the specified name exists (the name can be qualified
   * with catalog).
   *
   * @since 2.1.0
   */
  def databaseExists(dbName: String): Boolean

  /**
   * Check if the table or view with the specified name exists. This can either be a temporary
   * view or a table/view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. It follows the same
   *   resolution rule with SQL: search for temp views first then table/views in the current
   *   database (namespace).
   * @since 2.1.0
   */
  def tableExists(tableName: String): Boolean

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
   * @since 2.1.0
   */
  def tableExists(dbName: String, tableName: String): Boolean

  /**
   * Check if the function with the specified name exists. This can either be a temporary function
   * or a function.
   *
   * @param functionName
   *   is either a qualified or unqualified name that designates a function. It follows the same
   *   resolution rule with SQL: search for built-in/temp functions first then functions in the
   *   current database (namespace).
   * @since 2.1.0
   */
  def functionExists(functionName: String): Boolean

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
   * @since 2.1.0
   */
  def functionExists(dbName: String, functionName: String): Boolean

  /**
   * Creates a table from the given path and returns the corresponding DataFrame. It will use the
   * default data source configured by spark.sql.sources.default.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String): Dataset[Row] = {
    createTable(tableName, path)
  }

  /**
   * Creates a table from the given path and returns the corresponding DataFrame. It will use the
   * default data source configured by spark.sql.sources.default.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(tableName: String, path: String): Dataset[Row]

  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String, source: String): Dataset[Row] = {
    createTable(tableName, path, source)
  }

  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(tableName: String, path: String, source: String): Dataset[Row]

  /**
   * Creates a table from the given path based on a data source and a set of options. Then,
   * returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, options)
  }

  /**
   * Creates a table based on the dataset in a data source and a set of options. Then, returns the
   * corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(
      tableName: String,
      source: String,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, options.asScala.toMap)
  }

  /**
   * (Scala-specific) Creates a table from the given path based on a data source and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, options)
  }

  /**
   * (Scala-specific) Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(tableName: String, source: String, options: Map[String, String]): Dataset[Row]

  /**
   * Create a table from the given path based on a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, schema, options)
  }

  /**
   * Creates a table based on the dataset in a data source and a set of options. Then, returns the
   * corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.1.0
   */
  def createTable(
      tableName: String,
      source: String,
      description: String,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(
      tableName,
      source = source,
      description = description,
      options = options.asScala.toMap)
  }

  /**
   * (Scala-specific) Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.1.0
   */
  def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]): Dataset[Row]

  /**
   * Create a table based on the dataset in a data source, a schema and a set of options. Then,
   * returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, schema, options.asScala.toMap)
  }

  /**
   * (Scala-specific) Create a table from the given path based on a data source, a schema and a
   * set of options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): Dataset[Row] = {
    createTable(tableName, source, schema, options)
  }

  /**
   * (Scala-specific) Create a table based on the dataset in a data source, a schema and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.2.0
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): Dataset[Row]

  /**
   * Create a table based on the dataset in a data source, a schema and a set of options. Then,
   * returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.1.0
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: util.Map[String, String]): Dataset[Row] = {
    createTable(
      tableName,
      source = source,
      schema = schema,
      description = description,
      options = options.asScala.toMap)
  }

  /**
   * (Scala-specific) Create a table based on the dataset in a data source, a schema and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 3.1.0
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]): Dataset[Row]

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
   * @since 2.0.0
   */
  def dropTempView(viewName: String): Boolean

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
   * @since 2.1.0
   */
  def dropGlobalTempView(viewName: String): Boolean

  /**
   * Recovers all the partitions in the directory of a table and update the catalog. Only works
   * with a partitioned table, and not a view.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table. If no database
   *   identifier is provided, it refers to a table in the current database.
   * @since 2.1.1
   */
  def recoverPartitions(tableName: String): Unit

  /**
   * Returns true if the table is currently cached in-memory.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 2.0.0
   */
  def isCached(tableName: String): Boolean

  /**
   * Caches the specified table in-memory.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 2.0.0
   */
  def cacheTable(tableName: String): Unit

  /**
   * Caches the specified table with the given storage level.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @param storageLevel
   *   storage level to cache table.
   * @since 2.3.0
   */
  def cacheTable(tableName: String, storageLevel: StorageLevel): Unit

  /**
   * Removes the specified table from the in-memory cache.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table/view. If no database
   *   identifier is provided, it refers to a temporary view or a table/view in the current
   *   database.
   * @since 2.0.0
   */
  def uncacheTable(tableName: String): Unit

  /**
   * Removes all cached tables from the in-memory cache.
   *
   * @since 2.0.0
   */
  def clearCache(): Unit

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
   * @since 2.0.0
   */
  def refreshTable(tableName: String): Unit

  /**
   * Invalidates and refreshes all the cached data (and the associated metadata) for any `Dataset`
   * that contains the given data source path. Path matching is by checking for sub-directories,
   * i.e. "/" would invalidate everything that is cached and "/test/parent" would invalidate
   * everything that is a subdirectory of "/test/parent".
   *
   * @since 2.0.0
   */
  def refreshByPath(path: String): Unit

  /**
   * Returns the current catalog in this session.
   *
   * @since 3.4.0
   */
  def currentCatalog(): String

  /**
   * Sets the current catalog in this session.
   *
   * @since 3.4.0
   */
  def setCurrentCatalog(catalogName: String): Unit

  /**
   * Returns a list of catalogs available in this session.
   *
   * @since 3.4.0
   */
  def listCatalogs(): Dataset[CatalogMetadata]

  /**
   * Returns a list of catalogs which name match the specify pattern and available in this
   * session.
   *
   * @since 3.5.0
   */
  def listCatalogs(pattern: String): Dataset[CatalogMetadata]
}
