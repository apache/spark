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

import javax.annotation.Nullable

import org.apache.spark.sql.AnalysisException


/**
 * Interface for the system catalog (of columns, partitions, tables, and databases).
 *
 * This is only used for non-temporary items, and implementations must be thread-safe as they
 * can be accessed in multiple threads. This is an external catalog because it is expected to
 * interact with external systems.
 *
 * Implementations should throw [[AnalysisException]] when table or database don't exist.
 */
abstract class ExternalCatalog {
  import ExternalCatalog._

  protected def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new AnalysisException(s"Database $db does not exist")
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterDatabase(dbDefinition: CatalogDatabase): Unit

  def getDatabase(db: String): CatalogDatabase

  def databaseExists(db: String): Boolean

  def listDatabases(): Seq[String]

  def listDatabases(pattern: String): Seq[String]

  def setCurrentDatabase(db: String): Unit

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  def createTable(db: String, tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit

  def dropTable(db: String, table: String, ignoreIfNotExists: Boolean): Unit

  def renameTable(db: String, oldName: String, newName: String): Unit

  /**
   * Alter a table whose name that matches the one specified in `tableDefinition`,
   * assuming the table exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(db: String, tableDefinition: CatalogTable): Unit

  def getTable(db: String, table: String): CatalogTable

  def listTables(db: String): Seq[String]

  def listTables(db: String, pattern: String): Seq[String]

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

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

  // TODO: support listing by pattern
  def listPartitions(db: String, table: String): Seq[CatalogTablePartition]

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  def createFunction(db: String, funcDefinition: CatalogFunction): Unit

  def dropFunction(db: String, funcName: String): Unit

  def renameFunction(db: String, oldName: String, newName: String): Unit

  /**
   * Alter a function whose name that matches the one specified in `funcDefinition`,
   * assuming the function exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterFunction(db: String, funcDefinition: CatalogFunction): Unit

  def getFunction(db: String, funcName: String): CatalogFunction

  def listFunctions(db: String, pattern: String): Seq[String]

}


/**
 * A function defined in the catalog.
 *
 * @param name name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 */
case class CatalogFunction(name: String, className: String)


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class CatalogStorageFormat(
    locationUri: Option[String],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    serdeProperties: Map[String, String])


/**
 * A column in a table.
 */
case class CatalogColumn(
    name: String,
    // This may be null when used to create views. TODO: make this type-safe; this is left
    // as a string due to issues in converting Hive varchars to and from SparkSQL strings.
    @Nullable dataType: String,
    nullable: Boolean = true,
    comment: Option[String] = None)


/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 */
case class CatalogTablePartition(
    spec: ExternalCatalog.TablePartitionSpec,
    storage: CatalogStorageFormat)


/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 */
case class CatalogTable(
    specifiedDatabase: Option[String],
    name: String,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: Seq[CatalogColumn],
    partitionColumns: Seq[CatalogColumn] = Seq.empty,
    sortColumns: Seq[CatalogColumn] = Seq.empty,
    numBuckets: Int = 0,
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = System.currentTimeMillis,
    properties: Map[String, String] = Map.empty,
    viewOriginalText: Option[String] = None,
    viewText: Option[String] = None) {

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = specifiedDatabase.getOrElse {
    throw new AnalysisException(s"table $name did not specify database")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = s"$database.$name"

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[String] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      serde: Option[String] = storage.serde,
      serdeProperties: Map[String, String] = storage.serdeProperties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, serdeProperties))
  }

}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL_TABLE = new CatalogTableType("EXTERNAL_TABLE")
  val MANAGED_TABLE = new CatalogTableType("MANAGED_TABLE")
  val INDEX_TABLE = new CatalogTableType("INDEX_TABLE")
  val VIRTUAL_VIEW = new CatalogTableType("VIRTUAL_VIEW")
}


/**
 * A database defined in the catalog.
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: String,
    properties: Map[String, String])


object ExternalCatalog {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]
}
