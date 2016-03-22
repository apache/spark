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
  import ExternalCatalog._

  private class TableDesc(var table: CatalogTable) {
    val partitions = new mutable.HashMap[TablePartitionSpec, CatalogTablePartition]
  }

  private class DatabaseDesc(var db: CatalogDatabase) {
    val tables = new mutable.HashMap[String, TableDesc]
    val functions = new mutable.HashMap[String, CatalogFunction]
  }

  // Database name -> description
  private val catalog = new scala.collection.mutable.HashMap[String, DatabaseDesc]

  private def filterPattern(names: Seq[String], pattern: String): Seq[String] = {
    val regex = pattern.replaceAll("\\*", ".*").r
    names.filter { funcName => regex.pattern.matcher(funcName).matches() }
  }

  private def existsFunction(db: String, funcName: String): Boolean = {
    requireDbExists(db)
    catalog(db).functions.contains(funcName)
  }

  private def existsTable(db: String, table: String): Boolean = {
    requireDbExists(db)
    catalog(db).tables.contains(table)
  }

  private def existsPartition(db: String, table: String, spec: TablePartitionSpec): Boolean = {
    requireTableExists(db, table)
    catalog(db).tables(table).partitions.contains(spec)
  }

  private def requireFunctionExists(db: String, funcName: String): Unit = {
    if (!existsFunction(db, funcName)) {
      throw new AnalysisException(s"Function $funcName does not exist in $db database")
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    if (!existsTable(db, table)) {
      throw new AnalysisException(s"Table $table does not exist in $db database")
    }
  }

  private def requirePartitionExists(db: String, table: String, spec: TablePartitionSpec): Unit = {
    if (!existsPartition(db, table, spec)) {
      throw new AnalysisException(s"Partition does not exist in database $db table $table: $spec")
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
        throw new AnalysisException(s"Database ${dbDefinition.name} already exists.")
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
          throw new AnalysisException(s"Database $db is not empty. One or more tables exist.")
        }
        if (catalog(db).functions.nonEmpty) {
          throw new AnalysisException(s"Database $db is not empty. One or more functions exist.")
        }
      }
      // Remove the database.
      catalog.remove(db)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Database $db does not exist")
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
    filterPattern(listDatabases(), pattern)
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
    if (existsTable(db, tableDefinition.name)) {
      if (!ignoreIfExists) {
        throw new AnalysisException(s"Table ${tableDefinition.name} already exists in $db database")
      }
    } else {
      catalog(db).tables.put(tableDefinition.name, new TableDesc(tableDefinition))
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    requireDbExists(db)
    if (existsTable(db, table)) {
      catalog(db).tables.remove(table)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Table $table does not exist in $db database")
      }
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = synchronized {
    requireTableExists(db, oldName)
    val oldDesc = catalog(db).tables(oldName)
    oldDesc.table = oldDesc.table.copy(name = newName)
    catalog(db).tables.put(newName, oldDesc)
    catalog(db).tables.remove(oldName)
  }

  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = synchronized {
    requireTableExists(db, tableDefinition.name)
    catalog(db).tables(tableDefinition.name).table = tableDefinition
  }

  override def getTable(db: String, table: String): CatalogTable = synchronized {
    requireTableExists(db, table)
    catalog(db).tables(table).table
  }

  override def listTables(db: String): Seq[String] = synchronized {
    requireDbExists(db)
    catalog(db).tables.keySet.toSeq
  }

  override def listTables(db: String, pattern: String): Seq[String] = synchronized {
    requireDbExists(db)
    filterPattern(listTables(db), pattern)
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
        throw new AnalysisException(
          s"The following partitions already exist in database $db table $table:\n$dupSpecsStr")
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
        throw new AnalysisException(
          s"The following partitions do not exist in database $db table $table:\n$missingSpecsStr")
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
      table: String): Seq[CatalogTablePartition] = synchronized {
    requireTableExists(db, table)
    catalog(db).tables(table).partitions.values.toSeq
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String, func: CatalogFunction): Unit = synchronized {
    requireDbExists(db)
    if (existsFunction(db, func.name)) {
      throw new AnalysisException(s"Function $func already exists in $db database")
    } else {
      catalog(db).functions.put(func.name, func)
    }
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions.remove(funcName)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = synchronized {
    requireFunctionExists(db, oldName)
    val newFunc = getFunction(db, oldName).copy(name = newName)
    catalog(db).functions.remove(oldName)
    catalog(db).functions.put(newName, newFunc)
  }

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = synchronized {
    requireFunctionExists(db, funcDefinition.name)
    catalog(db).functions.put(funcDefinition.name, funcDefinition)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions(funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = synchronized {
    requireDbExists(db)
    filterPattern(catalog(db).functions.keysIterator.toSeq, pattern)
  }

}
