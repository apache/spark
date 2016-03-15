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

package org.apache.spark.sql.catalyst.catalog

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


/**
 * An internal catalog that is used by a Spark Session. This internal catalog serves as a
 * proxy to the underlying metastore (e.g. Hive Metastore) and it also manages temporary
 * tables and functions of the Spark Session that it belongs to.
 */
abstract class SessionCatalog(catalog: ExternalCatalog) {
  import ExternalCatalog._

  private[this] val tempTables = new ConcurrentHashMap[String, LogicalPlan]

  private[this] val tempFunctions = new ConcurrentHashMap[String, CatalogFunction]

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit

  def alterDatabase(dbDefinition: CatalogDatabase): Unit

  def getDatabase(db: String): CatalogDatabase

  def databaseExists(db: String): Boolean

  def listDatabases(): Seq[String]

  def listDatabases(pattern: String): Seq[String]

  // ----------------------------------------------------------------------------
  // Tables
  // ----------------------------------------------------------------------------
  // There are two kinds of tables, temporary tables and metastore tables.
  // Temporary tables are isolated across sessions and do not belong to any
  // particular database. Metastore tables can be used across multiple
  // sessions as their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // | Methods that interact with metastore tables only |
  // ----------------------------------------------------

  /**
   * Create a metastore table in the database specified in `tableDefinition`.
   * If no such database is specified, create it in the current database.
   */
  def createTable(
      currentDb: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit

  /**
   * Alter the metadata of an existing metastore table identified by `tableDefinition`.
   *
   * If no database is specified in `tableDefinition`, assume the table is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(currentDb: String, tableDefinition: CatalogTable): Unit

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   */
  def getTable(currentDb: String, name: TableIdentifier): CatalogTable

  // -------------------------------------------------------------
  // | Methods that interact with temporary and metastore tables |
  // -------------------------------------------------------------

  /**
   * Create a temporary table.
   * If a temporary table with the same name already exists, this throws an exception.
   */
  def createTempTable(name: String, tableDefinition: LogicalPlan): Unit

  /**
   * Rename a table.
   *
   * If a database is specified in `oldName`, this will rename the table in that database.
   * If no database is specified, this will first attempt to rename a temporary table with
   * the same name, then, if that does not exist, rename the table in the current database.
   *
   * This assumes the database specified in `oldName` matches the one specified in `newName`.
   */
  def renameTable(
      currentDb: String,
      oldName: TableIdentifier,
      newName: TableIdentifier): Unit

  /**
   * Drop a table.
   *
   * If a database is specified in `name`, this will drop the table from that database.
   * If no database is specified, this will first attempt to drop a temporary table with
   * the same name, then, if that does not exist, drop the table from the current database.
   */
  def dropTable(
      currentDb: String,
      name: TableIdentifier,
      ignoreIfNotExists: Boolean): Unit

  /**
   * Return a [[LogicalPlan]] that represents the given table.
   *
   * If a database is specified in `name`, this will return the table from that database.
   * If no database is specified, this will first attempt to return a temporary table with
   * the same name, then, if that does not exist, return the table from the current database.
   */
  def lookupRelation(
      currentDb: String,
      name: TableIdentifier,
      alias: Option[String] = None): LogicalPlan

  /**
   * List all tables in the current database, including temporary tables.
   */
  def listTables(currentDb: String): Seq[TableIdentifier]

  /**
   * List all matching tables in the current database, including temporary tables.
   */
  def listTables(currentDb: String, pattern: String): Seq[TableIdentifier]

  // ----------------------------------------------------------------------------
  // Partitions
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // These methods are concerned with only metastore tables.
  // ----------------------------------------------------------------------------

  // TODO: We need to figure out how these methods interact with our data source
  // tables. For such tables, we do not store values of partitioning columns in
  // the metastore. For now, partition values of a data source table will be
  // automatically discovered when we load the table.

  def createPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit

  def dropPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   */
  def renamePartitions(
      currentDb: String,
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit

  /**
   * Alter one or many table partitions whose specs that match those specified in `parts`,
   * assuming the partitions exist.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit

  def getPartition(
      currentDb: String,
      tableName: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition

  def listPartitions(
      currentDb: String,
      tableName: TableIdentifier): Seq[CatalogTablePartition]

  // ----------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------
  // There are two kinds of functions, temporary functions and metastore
  // functions (permanent UDFs). Temporary functions are isolated across
  // sessions. Metastore functions can be used across multiple sessions as
  // their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // | Methods that interact with metastore functions only |
  // -------------------------------------------------------

  /**
   * Create a metastore function in the database specified in `funcDefinition`.
   * If no such database is specified, create it in the current database.
   */
  def createFunction(currentDb: String, funcDefinition: CatalogFunction): Unit

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(currentDb: String, funcName: FunctionIdentifier): Unit

  /**
   * Alter a function whose name that matches the one specified in `funcDefinition`.
   *
   * If no database is specified in `funcDefinition`, assume the function is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterFunction(currentDb: String, funcDefinition: CatalogFunction): Unit

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Create a temporary function.
   * This assumes no database is specified in `funcDefinition`.
   */
  def createTempFunction(funcDefinition: CatalogFunction): Unit

  // TODO: The reason that we distinguish dropFunction and dropTempFunction is that
  // Hive has DROP FUNCTION and DROP TEMPORARY FUNCTION. We may want to consolidate
  // dropFunction and dropTempFunction.
  def dropTempFunction(name: String): Unit

  /**
   * Rename a function.
   *
   * If a database is specified in `oldName`, this will rename the function in that database.
   * If no database is specified, this will first attempt to rename a temporary function with
   * the same name, then, if that does not exist, rename the function in the current database.
   *
   * This assumes the database specified in `oldName` matches the one specified in `newName`.
   */
  def renameFunction(
      currentDb: String,
      oldName: FunctionIdentifier,
      newName: FunctionIdentifier): Unit

  /**
   * Retrieve the metadata of an existing function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will first attempt to return a temporary function with
   * the same name, then, if that does not exist, return the function in the current database.
   */
  def getFunction(currentDb: String, name: FunctionIdentifier): CatalogFunction

  /**
   * List all matching functions in the current database, including temporary functions.
   */
  def listFunctions(currentDb: String, pattern: String): Seq[FunctionIdentifier]

}
