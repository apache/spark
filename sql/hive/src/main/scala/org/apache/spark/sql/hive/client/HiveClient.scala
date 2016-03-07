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
import org.apache.spark.sql.catalyst.expressions.Expression


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

  /** Returns the name of the active database. */
  def currentDatabase: String

  /** Sets the name of current database. */
  def setCurrentDatabase(databaseName: String): Unit

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist */
  final def getDatabase(name: String): CatalogDatabase = {
    getDatabaseOption(name).getOrElse(throw new NoSuchDatabaseException(name))
  }

  /** Returns the metadata for a given database, or None if it doesn't exist. */
  def getDatabaseOption(name: String): Option[CatalogDatabase]

  /** List the names of all the databases that match the specified pattern. */
  def listDatabases(pattern: String): Seq[String]

  /** Returns the specified table, or throws [[NoSuchTableException]]. */
  final def getTable(dbName: String, tableName: String): CatalogTable = {
    getTableOption(dbName, tableName).getOrElse(throw new NoSuchTableException(dbName, tableName))
  }

  /** Returns the metadata for the specified table or None if it doesn't exist. */
  def getTableOption(dbName: String, tableName: String): Option[CatalogTable]

  /** Creates a view with the given metadata. */
  def createView(view: CatalogTable): Unit

  /** Updates the given view with new metadata. */
  def alertView(view: CatalogTable): Unit

  /** Creates a table with the given metadata. */
  def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit

  /** Drop the specified table. */
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit

  /** Alter a table whose name matches the one specified in `table`, assuming it exists. */
  final def alterTable(table: CatalogTable): Unit = alterTable(table.name, table)

  /** Updates the given table with new metadata, optionally renaming the table. */
  def alterTable(tableName: String, table: CatalogTable): Unit

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
   * Drop one or many partitions in the given table.
   *
   * Note: Unfortunately, Hive does not currently provide a way to ignore this call if the
   * partitions do not already exist. The seemingly relevant flag `ifExists` in
   * [[org.apache.hadoop.hive.metastore.PartitionDropOptions]] is not read anywhere.
   */
  def dropPartitions(
      db: String,
      table: String,
      specs: Seq[ExternalCatalog.TablePartitionSpec]): Unit

  /**
   * Rename one or many existing table partitions, assuming they exist.
   */
  def renamePartitions(
      db: String,
      table: String,
      specs: Seq[ExternalCatalog.TablePartitionSpec],
      newSpecs: Seq[ExternalCatalog.TablePartitionSpec]): Unit

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
      spec: ExternalCatalog.TablePartitionSpec): CatalogTablePartition = {
    getPartitionOption(dbName, tableName, spec).getOrElse {
      throw new NoSuchPartitionException(dbName, tableName, spec)
    }
  }

  /** Returns the specified partition or None if it does not exist. */
  final def getPartitionOption(
      db: String,
      table: String,
      spec: ExternalCatalog.TablePartitionSpec): Option[CatalogTablePartition] = {
    getPartitionOption(getTable(db, table), spec)
  }

  /** Returns the specified partition or None if it does not exist. */
  def getPartitionOption(
      table: CatalogTable,
      spec: ExternalCatalog.TablePartitionSpec): Option[CatalogTablePartition]

  /** Returns all partitions for the given table. */
  final def getAllPartitions(db: String, table: String): Seq[CatalogTablePartition] = {
    getAllPartitions(getTable(db, table))
  }

  /** Returns all partitions for the given table. */
  def getAllPartitions(table: CatalogTable): Seq[CatalogTablePartition]

  /** Returns partitions filtered by predicates for the given table. */
  def getPartitionsByFilter(
      table: CatalogTable,
      predicates: Seq[Expression]): Seq[CatalogTablePartition]

  /** Loads a static partition into an existing table. */
  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit

  /** Loads data into an existing table. */
  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit

  /** Loads new dynamic partitions into an existing table. */
  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit

  /** Create a function in an existing database. */
  def createFunction(db: String, func: CatalogFunction): Unit

  /** Drop an existing function an the database. */
  def dropFunction(db: String, name: String): Unit

  /** Rename an existing function in the database. */
  def renameFunction(db: String, oldName: String, newName: String): Unit

  /** Alter a function whose name matches the one specified in `func`, assuming it exists. */
  def alterFunction(db: String, func: CatalogFunction): Unit

  /** Return an existing function in the database, assuming it exists. */
  final def getFunction(db: String, name: String): CatalogFunction = {
    getFunctionOption(db, name).getOrElse(throw new NoSuchFunctionException(db, name))
  }

  /** Return an existing function in the database, or None if it doesn't exist. */
  def getFunctionOption(db: String, name: String): Option[CatalogFunction]

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
