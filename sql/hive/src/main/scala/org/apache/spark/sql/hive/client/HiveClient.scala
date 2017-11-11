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

package org.apache.spark.sql.hive.client

import java.io.PrintStream

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType


/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
 */
private[hive] trait HiveClient {

  /** Returns the Hive Version of this client. */
  def version: HiveVersion

  /** Returns the configuration for the given key in the current session. */
  def getConf(key: String, defaultValue: String): String

  /**
   * Return the associated Hive SessionState of this [[HiveClientImpl]]
   * @return [[Any]] not SessionState to avoid linkage error
   */
  def getState: Any

  /**
   * Runs a HiveQL command using Hive, returning the results as a list of strings.  Each row will
   * result in one string.
   */
  def runSqlHive(sql: String): Seq[String]

  def setOut(stream: PrintStream): Unit
  def setInfo(stream: PrintStream): Unit
  def setError(stream: PrintStream): Unit

  /** Returns the names of all tables in the given database. */
  def listTables(dbName: String): Seq[String]

  /** Returns the names of tables in the given database that matches the given pattern. */
  def listTables(dbName: String, pattern: String): Seq[String]

  /** Sets the name of current database. */
  def setCurrentDatabase(databaseName: String): Unit

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist */
  def getDatabase(name: String): CatalogDatabase

  /** Return whether a table/view with the specified name exists. */
  def databaseExists(dbName: String): Boolean

  /** List the names of all the databases that match the specified pattern. */
  def listDatabases(pattern: String): Seq[String]

  /** Return whether a table/view with the specified name exists. */
  def tableExists(dbName: String, tableName: String): Boolean

  /** Returns the specified table, or throws [[NoSuchTableException]]. */
  final def getTable(dbName: String, tableName: String): CatalogTable = {
    getTableOption(dbName, tableName).getOrElse(throw new NoSuchTableException(dbName, tableName))
  }

  /** Returns the metadata for the specified table or None if it doesn't exist. */
  def getTableOption(dbName: String, tableName: String): Option[CatalogTable]

  /** Creates a table with the given metadata. */
  def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit

  /** Drop the specified table. */
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit

  /** Alter a table whose name matches the one specified in `table`, assuming it exists. */
  final def alterTable(table: CatalogTable): Unit = {
    alterTable(table.database, table.identifier.table, table)
  }

  /**
   * Updates the given table with new metadata, optionally renaming the table or
   * moving across different database.
   */
  def alterTable(dbName: String, tableName: String, table: CatalogTable): Unit

  /**
   * Updates the given table with a new data schema and table properties, and keep everything else
   * unchanged.
   *
   * TODO(cloud-fan): it's a little hacky to introduce the schema table properties here in
   * `HiveClient`, but we don't have a cleaner solution now.
   */
  def alterTableDataSchema(
    dbName: String, tableName: String, newDataSchema: StructType, schemaProps: Map[String, String])

  /** Creates a new database with the given name. */
  def createDatabase(database: CatalogDatabase, ignoreIfExists: Boolean): Unit

  /**
   * Drop the specified database, if it exists.
   *
   * @param name database to drop
   * @param ignoreIfNotExists if true, do not throw error if the database does not exist
   * @param cascade whether to remove all associated objects such as tables and functions
   */
  def dropDatabase(name: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit

  /**
   * Alter a database whose name matches the one specified in `database`, assuming it exists.
   */
  def alterDatabase(database: CatalogDatabase): Unit

  /**
   * Create one or many partitions in the given table.
   */
  def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit

  /**
   * Drop one or many partitions in the given table, assuming they exist.
   */
  def dropPartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit

  /**
   * Rename one or many existing table partitions, assuming they exist.
   */
  def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit

  /**
   * Alter one or more table partitions whose specs match the ones specified in `newParts`,
   * assuming the partitions exist.
   */
  def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit

  /** Returns the specified partition, or throws [[NoSuchPartitionException]]. */
  final def getPartition(
      dbName: String,
      tableName: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    getPartitionOption(dbName, tableName, spec).getOrElse {
      throw new NoSuchPartitionException(dbName, tableName, spec)
    }
  }

  /**
   * Returns the partition names for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   *
   * The returned sequence is sorted as strings.
   */
  def getPartitionNames(
      table: CatalogTable,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String]

  /** Returns the specified partition or None if it does not exist. */
  final def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    getPartitionOption(getTable(db, table), spec)
  }

  /** Returns the specified partition or None if it does not exist. */
  def getPartitionOption(
      table: CatalogTable,
      spec: TablePartitionSpec): Option[CatalogTablePartition]

  /**
   * Returns the partitions for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   */
  final def getPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    getPartitions(getTable(db, table), partialSpec)
  }

  /**
   * Returns the partitions for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   */
  def getPartitions(
      catalogTable: CatalogTable,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition]

  /** Returns partitions filtered by predicates for the given table. */
  def getPartitionsByFilter(
      catalogTable: CatalogTable,
      predicates: Seq[Expression]): Seq[CatalogTablePartition]

  /** Loads a static partition into an existing table. */
  def loadPartition(
      loadPath: String,
      dbName: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit

  /** Loads data into an existing table. */
  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit

  /** Loads new dynamic partitions into an existing table. */
  def loadDynamicPartitions(
      loadPath: String,
      dbName: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      numDP: Int): Unit

  /** Create a function in an existing database. */
  def createFunction(db: String, func: CatalogFunction): Unit

  /** Drop an existing function in the database. */
  def dropFunction(db: String, name: String): Unit

  /** Rename an existing function in the database. */
  def renameFunction(db: String, oldName: String, newName: String): Unit

  /** Alter a function whose name matches the one specified in `func`, assuming it exists. */
  def alterFunction(db: String, func: CatalogFunction): Unit

  /** Return an existing function in the database, assuming it exists. */
  final def getFunction(db: String, name: String): CatalogFunction = {
    getFunctionOption(db, name).getOrElse(throw new NoSuchPermanentFunctionException(db, name))
  }

  /** Return an existing function in the database, or None if it doesn't exist. */
  def getFunctionOption(db: String, name: String): Option[CatalogFunction]

  /** Return whether a function exists in the specified database. */
  final def functionExists(db: String, name: String): Boolean = {
    getFunctionOption(db, name).isDefined
  }

  /** Return the names of all functions that match the given pattern in the database. */
  def listFunctions(db: String, pattern: String): Seq[String]

  /** Add a jar into class loader */
  def addJar(path: String): Unit

  /** Return a [[HiveClient]] as new session, that will share the class loader and Hive client */
  def newSession(): HiveClient

  /** Run a function within Hive state (SessionState, HiveConf, Hive client and class loader) */
  def withHiveState[A](f: => A): A

  /** Used for testing only.  Removes all metadata from this instance of Hive. */
  def reset(): Unit

}
