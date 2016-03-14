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

import org.apache.spark.sql.catalyst.TableIdentifier
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

  /**
   * Create a metastore table in the database specified in `tableDefinition`.
   * If no such table is specified, create it in the current database.
   */
  def createTable(
      currentDb: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit

  /**
   * Alter the metadata of an existing metastore table identified by `tableDefinition`.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(tableDefinition: CatalogTable): Unit

  /**
   * Retrieve the metadata of an existing metastore table.
   */
  def getTableMetadata(name: TableIdentifier): CatalogTable

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

  // --------------------------------------------------------------------------
  // Partitions
  // All methods in this category interact directly with the underlying catalog.
  // --------------------------------------------------------------------------
  // TODO: We need to figure out how these methods interact with our data source tables.
  // For data source tables, we do not store values of partitioning columns in the metastore.
  // For now, partition values of a data source table will be automatically discovered
  // when we load the table.

  def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit

  def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   */
  def renamePartitions(
      db: String,
      table: String,
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
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit

  def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition

  def listPartitions(db: String, table: String): Seq[CatalogTablePartition]

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  // --------------------------------------------------------------------------
  // Functions: Methods for metastore functions (permanent UDFs).
  // --------------------------------------------------------------------------

  def createFunction(db: String, funcDefinition: CatalogFunction): Unit

  /**
   * Drops a permanent function with the given name from the given database.
   */
  def dropFunction(db: String, funcName: String): Unit

  // --------------------------------------------------------------------------
  // Functions: Methods for metastore functions (permanent UDFs) or temp functions.
  // --------------------------------------------------------------------------

  def createTempFunction(funcDefinition: CatalogFunction): Unit

  /**
   * Drops a temporary function with the given name.
   */
  // TODO: The reason that we distinguish dropFunction and dropTempFunction is that
  // Hive has DROP FUNCTION and DROP TEMPORARY FUNCTION. We may want to consolidate
  // dropFunction and dropTempFunction.
  def dropTempFunction(funcName: String): Unit

  def renameFunction(
      specifiedDB: Option[String],
      currentDB: String,
      oldName: String,
      newName: String): Unit

  /**
   * Alter a function whose name that matches the one specified in `funcDefinition`,
   * assuming the function exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterFunction(
      specifiedDB: Option[String],
      currentDB: String,
      funcDefinition: CatalogFunction): Unit

  def getFunction(
      specifiedDB: Option[String],
      currentDB: String,
      funcName: String): CatalogFunction

  def listFunctions(
      specifiedDB: Option[String],
      currentDB: String,
      pattern: String): Seq[String]

}
