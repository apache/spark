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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ListenerBus

/**
 * Wraps an ExternalCatalog to provide listener events.
 */
class ExternalCatalogWithListener(delegate: ExternalCatalog)
  extends ExternalCatalog
    with ListenerBus[ExternalCatalogEventListener, ExternalCatalogEvent] {
  import CatalogTypes.TablePartitionSpec

  def unwrapped: ExternalCatalog = delegate

  override protected def doPostEvent(
      listener: ExternalCatalogEventListener,
      event: ExternalCatalogEvent): Unit = {
    listener.onEvent(event)
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = dbDefinition.name
    postToAll(CreateDatabasePreEvent(db))
    delegate.createDatabase(dbDefinition, ignoreIfExists)
    postToAll(CreateDatabaseEvent(db))
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    postToAll(DropDatabasePreEvent(db))
    delegate.dropDatabase(db, ignoreIfNotExists, cascade)
    postToAll(DropDatabaseEvent(db))
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val db = dbDefinition.name
    postToAll(AlterDatabasePreEvent(db))
    delegate.alterDatabase(dbDefinition)
    postToAll(AlterDatabaseEvent(db))
  }

  override def getDatabase(db: String): CatalogDatabase = {
    delegate.getDatabase(db)
  }

  override def databaseExists(db: String): Boolean = {
    delegate.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = {
    delegate.listDatabases()
  }

  override def listDatabases(pattern: String): Seq[String] = {
    delegate.listDatabases(pattern)
  }

  override def setCurrentDatabase(db: String): Unit = {
    delegate.setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    val tableDefinitionWithVersion =
      tableDefinition.copy(createVersion = org.apache.spark.SPARK_VERSION)
    postToAll(CreateTablePreEvent(db, name))
    delegate.createTable(tableDefinitionWithVersion, ignoreIfExists)
    postToAll(CreateTableEvent(db, name))
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    postToAll(DropTablePreEvent(db, table))
    delegate.dropTable(db, table, ignoreIfNotExists, purge)
    postToAll(DropTableEvent(db, table))
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameTablePreEvent(db, oldName, newName))
    delegate.renameTable(db, oldName, newName)
    postToAll(RenameTableEvent(db, oldName, newName))
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    postToAll(AlterTablePreEvent(db, name, AlterTableKind.TABLE))
    delegate.alterTable(tableDefinition)
    postToAll(AlterTableEvent(db, name, AlterTableKind.TABLE))
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.DATASCHEMA))
    delegate.alterTableDataSchema(db, table, newDataSchema)
    postToAll(AlterTableEvent(db, table, AlterTableKind.DATASCHEMA))
  }

  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.STATS))
    delegate.alterTableStats(db, table, stats)
    postToAll(AlterTableEvent(db, table, AlterTableKind.STATS))
  }

  override def getTable(db: String, table: String): CatalogTable = {
    delegate.getTable(db, table)
  }

  override def tableExists(db: String, table: String): Boolean = {
    delegate.tableExists(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    delegate.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    delegate.listTables(db, pattern)
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadPartition(
      db, table, loadPath, partition, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = {
    delegate.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    delegate.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      partSpecs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    delegate.dropPartitions(db, table, partSpecs, ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    delegate.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    delegate.alterPartitions(db, table, parts)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    delegate.getPartition(db, table, spec)
  }

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    delegate.getPartitionOption(db, table, spec)
  }

  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    delegate.listPartitionNames(db, table, partialSpec)
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    delegate.listPartitions(db, table, partialSpec)
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    delegate.listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    postToAll(CreateFunctionPreEvent(db, name))
    delegate.createFunction(db, funcDefinition)
    postToAll(CreateFunctionEvent(db, name))
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    postToAll(DropFunctionPreEvent(db, funcName))
    delegate.dropFunction(db, funcName)
    postToAll(DropFunctionEvent(db, funcName))
  }

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    postToAll(AlterFunctionPreEvent(db, name))
    delegate.alterFunction(db, funcDefinition)
    postToAll(AlterFunctionEvent(db, name))
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameFunctionPreEvent(db, oldName, newName))
    delegate.renameFunction(db, oldName, newName)
    postToAll(RenameFunctionEvent(db, oldName, newName))
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    delegate.getFunction(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    delegate.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    delegate.listFunctions(db, pattern)
  }
}
