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

package org.apache.spark.sql.catalog

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset}
import org.apache.spark.sql.types.StructType


/**
 * Catalog interface for Spark. To access this, use `SparkSession.catalog`.
 */
abstract class Catalog {

  /**
   * Returns the current default database in this session.
   *
   * @since 2.0.0
   */
  def currentDatabase: String

  /**
   * Sets the current default database in this session.
   *
   * @since 2.0.0
   */
  def setCurrentDatabase(dbName: String): Unit

  /**
   * Returns a list of databases available across all sessions.
   *
   * @since 2.0.0
   */
  def listDatabases(): Dataset[Database]

  /**
   * Returns a list of tables in the current database.
   * This includes all temporary tables.
   *
   * @since 2.0.0
   */
  def listTables(): Dataset[Table]

  /**
   * Returns a list of tables in the specified database.
   * This includes all temporary tables.
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String): Dataset[Table]

  /**
   * Returns a list of functions registered in the current database.
   * This includes all temporary functions
   *
   * @since 2.0.0
   */
  def listFunctions(): Dataset[Function]

  /**
   * Returns a list of functions registered in the specified database.
   * This includes all temporary functions
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String): Dataset[Function]

  /**
   * Returns a list of columns for the given table in the current database.
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("table does not exist")
  def listColumns(tableName: String): Dataset[Column]

  /**
   * Returns a list of columns for the given table in the specified database.
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database or table does not exist")
  def listColumns(dbName: String, tableName: String): Dataset[Column]

  /**
   * :: Experimental ::
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(tableName: String, path: String): DataFrame

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(tableName: String, path: String, source: String): DataFrame

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame

  /**
   * :: Experimental ::
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @since 2.0.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame

  /**
   * Drops the temporary view with the given view name in the catalog.
   * If the view has been cached before, then it will also be uncached.
   *
   * @param viewName the name of the view to be dropped.
   * @since 2.0.0
   */
  def dropTempView(viewName: String): Unit

  /**
   * Returns true if the table is currently cached in-memory.
   *
   * @since 2.0.0
   */
  def isCached(tableName: String): Boolean

  /**
   * Caches the specified table in-memory.
   *
   * @since 2.0.0
   */
  def cacheTable(tableName: String): Unit

  /**
   * Removes the specified table from the in-memory cache.
   *
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
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   *
   * If this table is cached as an InMemoryRelation, drop the original cached version and make the
   * new version cached lazily.
   *
   * @since 2.0.0
   */
  def refreshTable(tableName: String): Unit

}
