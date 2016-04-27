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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.util.StringUtils

/**
 * An in-memory (ephemeral) implementation of the system catalog.
 *
 * This is a dummy implementation that does not require setting up external systems.
 * It is intended for testing or exploration purposes only and should not be used
 * in production.
 *
 * All public methods should be synchronized for thread-safety.
 */
class InMemoryCatalog extends ExternalCatalog {
  import CatalogTypes.TablePartitionSpec

  private class TableDesc(var table: CatalogTable) {
    val partitions = new mutable.HashMap[TablePartitionSpec, CatalogTablePartition]
  }

  private class DatabaseDesc(var db: CatalogDatabase) {
    val tables = new mutable.HashMap[String, TableDesc]
    val functions = new mutable.HashMap[String, CatalogFunction]
  }

  // Database name -> description
  private val catalog = new scala.collection.mutable.HashMap[String, DatabaseDesc]

  private def partitionExists(db: String, table: String, spec: TablePartitionSpec): Boolean = {
    requireTableExists(db, table)
    catalog(db).tables(table).partitions.contains(spec)
  }

  private def requireFunctionExists(db: String, funcName: String): Unit = {
    if (!functionExists(db, funcName)) {
      throw new AnalysisException(
        s"Function not found: '$funcName' does not exist in database '$db'")
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    if (!tableExists(db, table)) {
      throw new AnalysisException(
        s"Table or view not found: '$table' does not exist in database '$db'")
    }
  }

  private def requirePartitionExists(db: String, table: String, spec: TablePartitionSpec): Unit = {
    if (!partitionExists(db, table, spec)) {
      throw new AnalysisException(
        s"Partition not found: database '$db' table '$table' does not contain: '$spec'")
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = synchronized {
    if (catalog.contains(dbDefinition.name)) {
      if (!ignoreIfExists) {
        throw new AnalysisException(s"Database '${dbDefinition.name}' already exists.")
      }
    } else {
      catalog.put(dbDefinition.name, new DatabaseDesc(dbDefinition))
    }
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = synchronized {
    if (catalog.contains(db)) {
      if (!cascade) {
        // If cascade is false, make sure the database is empty.
        if (catalog(db).tables.nonEmpty) {
          throw new AnalysisException(s"Database '$db' is not empty. One or more tables exist.")
        }
        if (catalog(db).functions.nonEmpty) {
          throw new AnalysisException(s"Database '$db' is not empty. One or more functions exist.")
        }
      }
      // Remove the database.
      catalog.remove(db)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Database '$db' does not exist")
      }
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = synchronized {
    requireDbExists(dbDefinition.name)
    catalog(dbDefinition.name).db = dbDefinition
  }

  override def getDatabase(db: String): CatalogDatabase = synchronized {
    requireDbExists(db)
    catalog(db).db
  }

  override def databaseExists(db: String): Boolean = synchronized {
    catalog.contains(db)
  }

  override def listDatabases(): Seq[String] = synchronized {
    catalog.keySet.toSeq
  }

  override def listDatabases(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(listDatabases(), pattern)
  }

  override def setCurrentDatabase(db: String): Unit = { /* no-op */ }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      db: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = synchronized {
    requireDbExists(db)
    val table = tableDefinition.identifier.table
    if (tableExists(db, table)) {
      if (!ignoreIfExists) {
        throw new AnalysisException(s"Table '$table' already exists in database '$db'")
      }
    } else {
      catalog(db).tables.put(table, new TableDesc(tableDefinition))
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    requireDbExists(db)
    if (tableExists(db, table)) {
      catalog(db).tables.remove(table)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Table or View '$table' does not exist in database '$db'")
      }
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = synchronized {
    requireTableExists(db, oldName)
    val oldDesc = catalog(db).tables(oldName)
    oldDesc.table = oldDesc.table.copy(identifier = TableIdentifier(newName, Some(db)))
    catalog(db).tables.put(newName, oldDesc)
    catalog(db).tables.remove(oldName)
  }

  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = synchronized {
    requireTableExists(db, tableDefinition.identifier.table)
    catalog(db).tables(tableDefinition.identifier.table).table = tableDefinition
  }

  override def getTable(db: String, table: String): CatalogTable = synchronized {
    requireTableExists(db, table)
    catalog(db).tables(table).table
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = synchronized {
    if (!tableExists(db, table)) None else Option(catalog(db).tables(table).table)
  }

  override def tableExists(db: String, table: String): Boolean = synchronized {
    requireDbExists(db)
    catalog(db).tables.contains(table)
  }

  override def listTables(db: String): Seq[String] = synchronized {
    requireDbExists(db)
    catalog(db).tables.keySet.toSeq
  }

  override def listTables(db: String, pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(listTables(db), pattern)
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = {
    throw new AnalysisException("loadTable is not implemented for InMemoryCatalog.")
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = {
    throw new AnalysisException("loadPartition is not implemented for InMemoryCatalog.")
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = synchronized {
    requireTableExists(db, table)
    val existingParts = catalog(db).tables(table).partitions
    if (!ignoreIfExists) {
      val dupSpecs = parts.collect { case p if existingParts.contains(p.spec) => p.spec }
      if (dupSpecs.nonEmpty) {
        val dupSpecsStr = dupSpecs.mkString("\n===\n")
        throw new AnalysisException("The following partitions already exist in database " +
          s"'$db' table '$table':\n$dupSpecsStr")
      }
    }
    parts.foreach { p => existingParts.put(p.spec, p) }
  }

  override def dropPartitions(
      db: String,
      table: String,
      partSpecs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = synchronized {
    requireTableExists(db, table)
    val existingParts = catalog(db).tables(table).partitions
    if (!ignoreIfNotExists) {
      val missingSpecs = partSpecs.collect { case s if !existingParts.contains(s) => s }
      if (missingSpecs.nonEmpty) {
        val missingSpecsStr = missingSpecs.mkString("\n===\n")
        throw new AnalysisException("The following partitions do not exist in database " +
          s"'$db' table '$table':\n$missingSpecsStr")
      }
    }
    partSpecs.foreach(existingParts.remove)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = synchronized {
    require(specs.size == newSpecs.size, "number of old and new partition specs differ")
    specs.zip(newSpecs).foreach { case (oldSpec, newSpec) =>
      val newPart = getPartition(db, table, oldSpec).copy(spec = newSpec)
      val existingParts = catalog(db).tables(table).partitions
      existingParts.remove(oldSpec)
      existingParts.put(newSpec, newPart)
    }
  }

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = synchronized {
    parts.foreach { p =>
      requirePartitionExists(db, table, p.spec)
      catalog(db).tables(table).partitions.put(p.spec, p)
    }
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = synchronized {
    requirePartitionExists(db, table, spec)
    catalog(db).tables(table).partitions(spec)
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = synchronized {
    requireTableExists(db, table)
    if (partialSpec.nonEmpty) {
      throw new AnalysisException("listPartition does not support partition spec in " +
        "InMemoryCatalog.")
    }
    catalog(db).tables(table).partitions.values.toSeq
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String, func: CatalogFunction): Unit = synchronized {
    requireDbExists(db)
    if (functionExists(db, func.identifier.funcName)) {
      throw new AnalysisException(s"Function '$func' already exists in '$db' database")
    } else {
      catalog(db).functions.put(func.identifier.funcName, func)
    }
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions.remove(funcName)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = synchronized {
    requireFunctionExists(db, oldName)
    val newFunc = getFunction(db, oldName).copy(identifier = FunctionIdentifier(newName, Some(db)))
    catalog(db).functions.remove(oldName)
    catalog(db).functions.put(newName, newFunc)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions(funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    requireDbExists(db)
    catalog(db).functions.contains(funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = synchronized {
    requireDbExists(db)
    StringUtils.filterPattern(catalog(db).functions.keysIterator.toSeq, pattern)
  }

}
