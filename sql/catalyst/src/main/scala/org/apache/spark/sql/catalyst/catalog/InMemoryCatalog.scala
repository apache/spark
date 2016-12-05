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

import java.io.IOException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.Expression
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
class InMemoryCatalog(
    conf: SparkConf = new SparkConf,
    hadoopConfig: Configuration = new Configuration)
  extends ExternalCatalog {

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

  private def requireTableNotExists(db: String, table: String): Unit = {
    if (tableExists(db, table)) {
      throw new TableAlreadyExistsException(db = db, table = table)
    }
  }

  private def requirePartitionsExist(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (!partitionExists(db, table, s)) {
        throw new NoSuchPartitionException(db = db, table = table, spec = s)
      }
    }
  }

  private def requirePartitionsNotExist(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (partitionExists(db, table, s)) {
        throw new PartitionAlreadyExistsException(db = db, table = table, spec = s)
      }
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
        throw new DatabaseAlreadyExistsException(dbDefinition.name)
      }
    } else {
      try {
        val location = new Path(dbDefinition.locationUri)
        val fs = location.getFileSystem(hadoopConfig)
        fs.mkdirs(location)
      } catch {
        case e: IOException =>
          throw new SparkException(s"Unable to create database ${dbDefinition.name} as failed " +
            s"to create its directory ${dbDefinition.locationUri}", e)
      }
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
      val dbDefinition = catalog(db).db
      try {
        val location = new Path(dbDefinition.locationUri)
        val fs = location.getFileSystem(hadoopConfig)
        fs.delete(location, true)
      } catch {
        case e: IOException =>
          throw new SparkException(s"Unable to drop database ${dbDefinition.name} as failed " +
            s"to delete its directory ${dbDefinition.locationUri}", e)
      }
      catalog.remove(db)
    } else {
      if (!ignoreIfNotExists) {
        throw new NoSuchDatabaseException(db)
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
    catalog.keySet.toSeq.sorted
  }

  override def listDatabases(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(listDatabases(), pattern)
  }

  override def setCurrentDatabase(db: String): Unit = { /* no-op */ }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = synchronized {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    requireDbExists(db)
    val table = tableDefinition.identifier.table
    if (tableExists(db, table)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else {
      // Set the default table location if this is a managed table and its location is not
      // specified.
      // Ideally we should not create a managed table with location, but Hive serde table can
      // specify location for managed table. And in [[CreateDataSourceTableAsSelectCommand]] we have
      // to create the table directory and write out data before we create this table, to avoid
      // exposing a partial written table.
      val needDefaultTableLocation =
        tableDefinition.tableType == CatalogTableType.MANAGED &&
          tableDefinition.storage.locationUri.isEmpty

      val tableWithLocation = if (needDefaultTableLocation) {
        val defaultTableLocation = new Path(catalog(db).db.locationUri, table)
        try {
          val fs = defaultTableLocation.getFileSystem(hadoopConfig)
          fs.mkdirs(defaultTableLocation)
        } catch {
          case e: IOException =>
            throw new SparkException(s"Unable to create table $table as failed " +
              s"to create its directory $defaultTableLocation", e)
        }
        tableDefinition.withNewStorage(locationUri = Some(defaultTableLocation.toUri.toString))
      } else {
        tableDefinition
      }

      catalog(db).tables.put(table, new TableDesc(tableWithLocation))
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    requireDbExists(db)
    if (tableExists(db, table)) {
      val tableMeta = getTable(db, table)
      if (tableMeta.tableType == CatalogTableType.MANAGED) {
        assert(tableMeta.storage.locationUri.isDefined,
          "Managed table should always have table location, as we will assign a default location " +
            "to it if it doesn't have one.")
        val dir = new Path(tableMeta.location)
        try {
          val fs = dir.getFileSystem(hadoopConfig)
          fs.delete(dir, true)
        } catch {
          case e: IOException =>
            throw new SparkException(s"Unable to drop table $table as failed " +
              s"to delete its directory $dir", e)
        }
      }
      catalog(db).tables.remove(table)
    } else {
      if (!ignoreIfNotExists) {
        throw new NoSuchTableException(db = db, table = table)
      }
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = synchronized {
    requireTableExists(db, oldName)
    requireTableNotExists(db, newName)
    val oldDesc = catalog(db).tables(oldName)
    oldDesc.table = oldDesc.table.copy(identifier = TableIdentifier(newName, Some(db)))

    if (oldDesc.table.tableType == CatalogTableType.MANAGED) {
      assert(oldDesc.table.storage.locationUri.isDefined,
        "Managed table should always have table location, as we will assign a default location " +
          "to it if it doesn't have one.")
      val oldDir = new Path(oldDesc.table.location)
      val newDir = new Path(catalog(db).db.locationUri, newName)
      try {
        val fs = oldDir.getFileSystem(hadoopConfig)
        fs.rename(oldDir, newDir)
      } catch {
        case e: IOException =>
          throw new SparkException(s"Unable to rename table $oldName to $newName as failed " +
            s"to rename its directory $oldDir", e)
      }
      oldDesc.table = oldDesc.table.withNewStorage(locationUri = Some(newDir.toUri.toString))
    }

    catalog(db).tables.put(newName, oldDesc)
    catalog(db).tables.remove(oldName)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = synchronized {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
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
    catalog(db).tables.keySet.toSeq.sorted
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
    throw new UnsupportedOperationException("loadTable is not implemented")
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    throw new UnsupportedOperationException("loadPartition is not implemented.")
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean): Unit = {
    throw new UnsupportedOperationException("loadDynamicPartitions is not implemented.")
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
        throw new PartitionsAlreadyExistException(db = db, table = table, specs = dupSpecs)
      }
    }

    val tableMeta = getTable(db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    val tablePath = new Path(tableMeta.location)
    // TODO: we should follow hive to roll back if one partition path failed to create.
    parts.foreach { p =>
      val partitionPath = p.storage.locationUri.map(new Path(_)).getOrElse {
        ExternalCatalogUtils.generatePartitionPath(p.spec, partitionColumnNames, tablePath)
      }

      try {
        val fs = tablePath.getFileSystem(hadoopConfig)
        if (!fs.exists(partitionPath)) {
          fs.mkdirs(partitionPath)
        }
      } catch {
        case e: IOException =>
          throw new SparkException(s"Unable to create partition path $partitionPath", e)
      }

      existingParts.put(
        p.spec,
        p.copy(storage = p.storage.copy(locationUri = Some(partitionPath.toString))))
    }
  }

  override def dropPartitions(
      db: String,
      table: String,
      partSpecs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = synchronized {
    requireTableExists(db, table)
    val existingParts = catalog(db).tables(table).partitions
    if (!ignoreIfNotExists) {
      val missingSpecs = partSpecs.collect { case s if !existingParts.contains(s) => s }
      if (missingSpecs.nonEmpty) {
        throw new NoSuchPartitionsException(db = db, table = table, specs = missingSpecs)
      }
    }

    val shouldRemovePartitionLocation = if (retainData) {
      false
    } else {
      getTable(db, table).tableType == CatalogTableType.MANAGED
    }

    // TODO: we should follow hive to roll back if one partition path failed to delete, and support
    // partial partition spec.
    partSpecs.foreach { p =>
      if (existingParts.contains(p) && shouldRemovePartitionLocation) {
        val partitionPath = new Path(existingParts(p).location)
        try {
          val fs = partitionPath.getFileSystem(hadoopConfig)
          fs.delete(partitionPath, true)
        } catch {
          case e: IOException =>
            throw new SparkException(s"Unable to delete partition path $partitionPath", e)
        }
      }
      existingParts.remove(p)
    }
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = synchronized {
    require(specs.size == newSpecs.size, "number of old and new partition specs differ")
    requirePartitionsExist(db, table, specs)
    requirePartitionsNotExist(db, table, newSpecs)

    val tableMeta = getTable(db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    val tablePath = new Path(tableMeta.location)
    val shouldUpdatePartitionLocation = getTable(db, table).tableType == CatalogTableType.MANAGED
    val existingParts = catalog(db).tables(table).partitions
    // TODO: we should follow hive to roll back if one partition path failed to rename.
    specs.zip(newSpecs).foreach { case (oldSpec, newSpec) =>
      val oldPartition = getPartition(db, table, oldSpec)
      val newPartition = if (shouldUpdatePartitionLocation) {
        val oldPartPath = new Path(oldPartition.location)
        val newPartPath = ExternalCatalogUtils.generatePartitionPath(
          newSpec, partitionColumnNames, tablePath)
        try {
          val fs = tablePath.getFileSystem(hadoopConfig)
          fs.rename(oldPartPath, newPartPath)
        } catch {
          case e: IOException =>
            throw new SparkException(s"Unable to rename partition path $oldPartPath", e)
        }
        oldPartition.copy(
          spec = newSpec,
          storage = oldPartition.storage.copy(locationUri = Some(newPartPath.toString)))
      } else {
        oldPartition.copy(spec = newSpec)
      }

      existingParts.remove(oldSpec)
      existingParts.put(newSpec, newPartition)
    }
  }

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = synchronized {
    requirePartitionsExist(db, table, parts.map(p => p.spec))
    parts.foreach { p =>
      catalog(db).tables(table).partitions.put(p.spec, p)
    }
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = synchronized {
    requirePartitionsExist(db, table, Seq(spec))
    catalog(db).tables(table).partitions(spec)
  }

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = synchronized {
    if (!partitionExists(db, table, spec)) {
      None
    } else {
      Option(catalog(db).tables(table).partitions(spec))
    }
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = synchronized {
    requireTableExists(db, table)

    partialSpec match {
      case None => catalog(db).tables(table).partitions.values.toSeq
      case Some(partial) =>
        catalog(db).tables(table).partitions.toSeq.collect {
          case (spec, partition) if isPartialPartitionSpec(partial, spec) => partition
        }
    }
  }

  /**
   * Returns true if `spec1` is a partial partition spec w.r.t. `spec2`, e.g. PARTITION (a=1) is a
   * partial partition spec w.r.t. PARTITION (a=1,b=2).
   */
  private def isPartialPartitionSpec(
      spec1: TablePartitionSpec,
      spec2: TablePartitionSpec): Boolean = {
    spec1.forall {
      case (partitionColumn, value) => spec2(partitionColumn) == value
    }
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    // TODO: Provide an implementation
    throw new UnsupportedOperationException(
      "listPartitionsByFilter is not implemented for InMemoryCatalog")
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String, func: CatalogFunction): Unit = synchronized {
    requireDbExists(db)
    requireFunctionNotExists(db, func.identifier.funcName)
    catalog(db).functions.put(func.identifier.funcName, func)
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions.remove(funcName)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = synchronized {
    requireFunctionExists(db, oldName)
    requireFunctionNotExists(db, newName)
    val newFunc = getFunction(db, oldName).copy(identifier = FunctionIdentifier(newName, Some(db)))
    catalog(db).functions.remove(oldName)
    catalog(db).functions.put(newName, newFunc)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = synchronized {
    requireFunctionExists(db, funcName)
    catalog(db).functions(funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = synchronized {
    requireDbExists(db)
    catalog(db).functions.contains(funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = synchronized {
    requireDbExists(db)
    StringUtils.filterPattern(catalog(db).functions.keysIterator.toSeq, pattern)
  }

}
