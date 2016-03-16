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

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}


/**
 * An internal catalog that is used by a Spark Session. This internal catalog serves as a
 * proxy to the underlying metastore (e.g. Hive Metastore) and it also manages temporary
 * tables and functions of the Spark Session that it belongs to.
 */
class SessionCatalog(externalCatalog: ExternalCatalog) {
  import ExternalCatalog._

  private[this] val tempTables = new ConcurrentHashMap[String, LogicalPlan]

  private[this] val tempFunctions = new ConcurrentHashMap[String, CatalogFunction]

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    externalCatalog.createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    externalCatalog.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    externalCatalog.alterDatabase(dbDefinition)
  }

  def getDatabase(db: String): CatalogDatabase = {
    externalCatalog.getDatabase(db)
  }

  def databaseExists(db: String): Boolean = {
    externalCatalog.databaseExists(db)
  }

  def listDatabases(): Seq[String] = {
    externalCatalog.listDatabases()
  }

  def listDatabases(pattern: String): Seq[String] = {
    externalCatalog.listDatabases(pattern)
  }

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
      ignoreIfExists: Boolean): Unit = {
    val db = tableDefinition.name.database.getOrElse(currentDb)
    val newTableDefinition = tableDefinition.copy(
      name = TableIdentifier(tableDefinition.name.table, Some(db)))
    externalCatalog.createTable(db, newTableDefinition, ignoreIfExists)
  }

  /**
   * Alter the metadata of an existing metastore table identified by `tableDefinition`.
   *
   * If no database is specified in `tableDefinition`, assume the table is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(currentDb: String, tableDefinition: CatalogTable): Unit = {
    val db = tableDefinition.name.database.getOrElse(currentDb)
    val newTableDefinition = tableDefinition.copy(
      name = TableIdentifier(tableDefinition.name.table, Some(db)))
    externalCatalog.alterTable(db, newTableDefinition)
  }

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   */
  def getTable(currentDb: String, name: TableIdentifier): CatalogTable = {
    val db = name.database.getOrElse(currentDb)
    externalCatalog.getTable(db, name.table)
  }

  // -------------------------------------------------------------
  // | Methods that interact with temporary and metastore tables |
  // -------------------------------------------------------------

  /**
   * Create a temporary table.
   */
  def createTempTable(
      name: String,
      tableDefinition: LogicalPlan,
      ignoreIfExists: Boolean): Unit = {
    if (tempTables.containsKey(name) && !ignoreIfExists) {
      throw new AnalysisException(s"Temporary table '$name' already exists.")
    }
    tempTables.put(name, tableDefinition)
  }

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
      newName: TableIdentifier): Unit = {
    if (oldName.database != newName.database) {
      throw new AnalysisException("rename does not support moving tables across databases")
    }
    val db = oldName.database.getOrElse(currentDb)
    if (oldName.database.isDefined || !tempTables.containsKey(oldName.table)) {
      externalCatalog.renameTable(db, oldName.table, newName.table)
    } else {
      val table = tempTables.remove(oldName.table)
      tempTables.put(newName.table, table)
    }
  }

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
      ignoreIfNotExists: Boolean): Unit = {
    val db = name.database.getOrElse(currentDb)
    if (name.database.isDefined || !tempTables.containsKey(name.table)) {
      externalCatalog.dropTable(db, name.table, ignoreIfNotExists)
    } else {
      tempTables.remove(name.table)
    }
  }

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
      alias: Option[String] = None): LogicalPlan = {
    val db = name.database.getOrElse(currentDb)
    val relation =
      if (name.database.isDefined || !tempTables.containsKey(name.table)) {
        val metadata = externalCatalog.getTable(db, name.table)
        CatalogRelation(db, metadata, alias)
      } else {
        tempTables.get(name.table)
      }
    val tableWithQualifiers = SubqueryAlias(name.table, relation)
    // If an alias was specified by the lookup, wrap the plan in a subquery so that
    // attributes are properly qualified with this alias.
    alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  /**
   * List all tables in the current database, including temporary tables.
   */
  def listTables(currentDb: String): Seq[TableIdentifier] = {
    val tablesInCurrentDb = externalCatalog.listTables(currentDb).map { t =>
      TableIdentifier(t, Some(currentDb))
    }
    val _tempTables = tempTables.keys().asScala.map { t => TableIdentifier(t) }
    tablesInCurrentDb ++ _tempTables
  }

  /**
   * List all matching tables in the current database, including temporary tables.
   */
  def listTables(currentDb: String, pattern: String): Seq[TableIdentifier] = {
    val tablesInCurrentDb = externalCatalog.listTables(currentDb, pattern).map { t =>
      TableIdentifier(t, Some(currentDb))
    }
    val regex = pattern.replaceAll("\\*", ".*").r
    val _tempTables = tempTables.keys().asScala
      .filter { t => regex.pattern.matcher(t).matches() }
      .map { t => TableIdentifier(t) }
    tablesInCurrentDb ++ _tempTables
  }

  /**
   * Return a temporary table exactly as it was stored.
   * For testing only.
   */
  private[catalog] def getTempTable(name: String): Option[LogicalPlan] = {
    Option(tempTables.get(name))
  }

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

  /**
   * Create partitions in an existing table, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def createPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.createPartitions(db, tableName.table, parts, ignoreIfExists)
  }

  /**
   * Drop partitions from a table, assuming they exist.
   * If no database is specified, assume the table is in the current database.
   */
  def dropPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.dropPartitions(db, tableName.table, parts, ignoreIfNotExists)
  }

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   *
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   * If no database is specified, assume the table is in the current database.
   */
  def renamePartitions(
      currentDb: String,
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.renamePartitions(db, tableName.table, specs, newSpecs)
  }

  /**
   * Alter one or many table partitions whose specs that match those specified in `parts`,
   * assuming the partitions exist.
   *
   * If no database is specified, assume the table is in the current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterPartitions(
      currentDb: String,
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.alterPartitions(db, tableName.table, parts)
  }

  /**
   * Retrieve the metadata of a table partition, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def getPartition(
      currentDb: String,
      tableName: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.getPartition(db, tableName.table, spec)
  }

  /**
   * List all partitions in a table, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def listPartitions(
      currentDb: String,
      tableName: TableIdentifier): Seq[CatalogTablePartition] = {
    val db = tableName.database.getOrElse(currentDb)
    externalCatalog.listPartitions(db, tableName.table)
  }

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
  def createFunction(currentDb: String, funcDefinition: CatalogFunction): Unit = {
    val db = funcDefinition.name.database.getOrElse(currentDb)
    val newFuncDefinition = funcDefinition.copy(
      name = FunctionIdentifier(funcDefinition.name.funcName, Some(db)))
    externalCatalog.createFunction(db, newFuncDefinition)
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(currentDb: String, name: FunctionIdentifier): Unit = {
    val db = name.database.getOrElse(currentDb)
    externalCatalog.dropFunction(db, name.funcName)
  }

  /**
   * Alter a metastore function whose name that matches the one specified in `funcDefinition`.
   *
   * If no database is specified in `funcDefinition`, assume the function is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterFunction(currentDb: String, funcDefinition: CatalogFunction): Unit = {
    val db = funcDefinition.name.database.getOrElse(currentDb)
    val newFuncDefinition = funcDefinition.copy(
      name = FunctionIdentifier(funcDefinition.name.funcName, Some(db)))
    externalCatalog.alterFunction(db, newFuncDefinition)
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Create a temporary function.
   * This assumes no database is specified in `funcDefinition`.
   */
  def createTempFunction(
      funcDefinition: CatalogFunction,
      ignoreIfExists: Boolean): Unit = {
    require(funcDefinition.name.database.isEmpty,
      "attempted to create a temporary function while specifying a database")
    val name = funcDefinition.name.funcName
    if (tempFunctions.containsKey(name) && !ignoreIfExists) {
      throw new AnalysisException(s"Temporary function '$name' already exists.")
    }
    tempFunctions.put(name, funcDefinition)
  }

  /**
   * Drop a temporary function.
   */
  // TODO: The reason that we distinguish dropFunction and dropTempFunction is that
  // Hive has DROP FUNCTION and DROP TEMPORARY FUNCTION. We may want to consolidate
  // dropFunction and dropTempFunction.
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!tempFunctions.containsKey(name) && !ignoreIfNotExists) {
      throw new AnalysisException(
        s"Temporary function '$name' cannot be dropped because it does not exist!")
    }
    tempFunctions.remove(name)
  }

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
      newName: FunctionIdentifier): Unit = {
    val db = oldName.database.getOrElse(currentDb)
    if (oldName.database.isDefined || !tempFunctions.containsKey(oldName.funcName)) {
      externalCatalog.renameFunction(db, oldName.funcName, newName.funcName)
    } else {
      val func = tempFunctions.remove(oldName.funcName)
      val newFunc = func.copy(name = func.name.copy(funcName = newName.funcName))
      tempFunctions.put(newName.funcName, newFunc)
    }
  }

  /**
   * Retrieve the metadata of an existing function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will first attempt to return a temporary function with
   * the same name, then, if that does not exist, return the function in the current database.
   */
  def getFunction(currentDb: String, name: FunctionIdentifier): CatalogFunction = {
    val db = name.database.getOrElse(currentDb)
    if (name.database.isDefined || !tempFunctions.containsKey(name.funcName)) {
      externalCatalog.getFunction(db, name.funcName)
    } else {
      tempFunctions.get(name.funcName)
    }
  }

  /**
   * List all matching functions in the current database, including temporary functions.
   */
  def listFunctions(currentDb: String, pattern: String): Seq[FunctionIdentifier] = {
    val functionsInCurrentDb = externalCatalog.listFunctions(currentDb, pattern).map { f =>
      FunctionIdentifier(f, Some(currentDb))
    }
    functionsInCurrentDb ++ tempFunctions.keys.asScala.map { f => FunctionIdentifier(f) }
  }

}
