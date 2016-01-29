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


/**
 * Interface for the system catalog (of columns, partitions, tables, and databases).
 *
 * This is only used for non-temporary items.
 */
abstract class Catalog {

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  def createFunction(db: String, funcDefinition: Function, ifNotExists: Boolean): Unit

  def dropFunction(db: String, funcName: String): Unit

  def alterFunction(db: String, funcName: String, funcDefinition: Function): Unit

  def getFunction(db: String, name: String): Function

  def listFunctions(db: String, pattern: String): Seq[String]

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  // TODO: need more functions for partitioning.

  def alterPartition(db: String, table: String, part: TablePartition): Unit

  def alterPartitions(db: String, table: String, parts: Seq[TablePartition]): Unit

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  def createTable(db: String, tableDefinition: Table, ifNotExists: Boolean): Unit

  def dropTable(
    db: String, table: String, deleteData: Boolean, ignoreIfNotExists: Boolean): Unit

  def alterTable(db: String, table: String, tableDefinition: Table)

  def getTable(db: String, table: String): Table

  def listTables(db: String): Seq[String]

  def listTables(db: String, pattern: String): Seq[String]

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  def createDatabase(dbDefinition: Database, ifNotExists: Boolean): Unit

  def dropDatabase(
      db: String,
      deleteData: Boolean,
      ignoreIfNotExists: Boolean,
      deleteTables: Boolean): Unit

  def alterDatabase(db: String, dbDefinition: Database)

  def getDatabase(db: String): Database

  def listDatabases(): Seq[String]

  def listDatabases(pattern: String): Seq[String]
}


/**
 * A function defined in the catalog.
 * @param name name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 */
class Function(
  val name: String,
  val className: String
)


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
class StorageFormat(
  locationUri: String,
  inputFormat: String,
  outputFormat: String,
  serde: String,
  serdeProperties: Map[String, String]
)


/**
 * A column in a table.
 */
class Column(
  val name: String,
  val dataType: String,
  val nullable: Boolean,
  val comment: String
)


/**
 * A partition (Hive style) defined in the catalog.
 * @param values values for the partition columns
 * @param storage storage format of the partition
 */
class TablePartition(
  val values: Seq[String],
  val storage: StorageFormat
)


/**
 * A table defined in the catalog.
 */
class Table(
  val name: String,
  val schema: Seq[Column],
  val partitionColumns: Seq[Column],
  val storage: StorageFormat,
  val numBuckets: Int,
  val properties: Map[String, String],
  val tableType: String,
  val createTime: Long,
  val lastAccessTime: Long,
  val viewOriginalText: Option[String],
  val viewText: Option[String]) {

  require(tableType == "EXTERNAL_TABLE" || tableType == "INDEX_TABLE" ||
    tableType == "MANAGED_TABLE" || tableType == "VIRTUAL_VIEW")
}


/**
 * A database defined in the catalog.
 */
class Database(
  val name: String,
  val description: String,
  val locationUri: String,
  val properties: Map[String, String]
)
