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
import java.util.{Map => JMap}
import javax.annotation.Nullable

import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.Expression

private[hive] case class HiveDatabase(name: String, location: String)

private[hive] abstract class TableType { val name: String }
private[hive] case object ExternalTable extends TableType { override val name = "EXTERNAL_TABLE" }
private[hive] case object IndexTable extends TableType { override val name = "INDEX_TABLE" }
private[hive] case object ManagedTable extends TableType { override val name = "MANAGED_TABLE" }
private[hive] case object VirtualView extends TableType { override val name = "VIRTUAL_VIEW" }

// TODO: Use this for Tables and Partitions
private[hive] case class HiveStorageDescriptor(
    location: String,
    inputFormat: String,
    outputFormat: String,
    serde: String,
    serdeProperties: Map[String, String])

private[hive] case class HivePartition(
    values: Seq[String],
    storage: HiveStorageDescriptor)

private[hive] case class HiveColumn(name: String, @Nullable hiveType: String, comment: String)
private[hive] case class HiveTable(
    specifiedDatabase: Option[String],
    name: String,
    schema: Seq[HiveColumn],
    partitionColumns: Seq[HiveColumn],
    properties: Map[String, String],
    serdeProperties: Map[String, String],
    tableType: TableType,
    location: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    serde: Option[String] = None,
    viewText: Option[String] = None) {

  @transient
  private[client] var client: ClientInterface = _

  private[client] def withClient(ci: ClientInterface): this.type = {
    client = ci
    this
  }

  def database: String = specifiedDatabase.getOrElse(sys.error("database not resolved"))

  def isPartitioned: Boolean = partitionColumns.nonEmpty

  def getAllPartitions: Seq[HivePartition] = client.getAllPartitions(this)

  def getPartitions(predicates: Seq[Expression]): Seq[HivePartition] =
    client.getPartitionsByFilter(this, predicates)

  // Hive does not support backticks when passing names to the client.
  def qualifiedName: String = s"$database.$name"
}

/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
 */
private[hive] trait ClientInterface {

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

  /** Returns the name of the active database. */
  def currentDatabase: String

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist */
  def getDatabase(name: String): HiveDatabase = {
    getDatabaseOption(name).getOrElse(throw new NoSuchDatabaseException)
  }

  /** Returns the metadata for a given database, or None if it doesn't exist. */
  def getDatabaseOption(name: String): Option[HiveDatabase]

  /** Returns the specified table, or throws [[NoSuchTableException]]. */
  def getTable(dbName: String, tableName: String): HiveTable = {
    getTableOption(dbName, tableName).getOrElse(throw new NoSuchTableException)
  }

  /** Returns the metadata for the specified table or None if it doens't exist. */
  def getTableOption(dbName: String, tableName: String): Option[HiveTable]

  /** Creates a view with the given metadata. */
  def createView(view: HiveTable): Unit

  /** Updates the given view with new metadata. */
  def alertView(view: HiveTable): Unit

  /** Creates a table with the given metadata. */
  def createTable(table: HiveTable): Unit

  /** Updates the given table with new metadata. */
  def alterTable(table: HiveTable): Unit

  /** Creates a new database with the given name. */
  def createDatabase(database: HiveDatabase): Unit

  /** Returns the specified paritition or None if it does not exist. */
  def getPartitionOption(
      hTable: HiveTable,
      partitionSpec: JMap[String, String]): Option[HivePartition]

  /** Returns all partitions for the given table. */
  def getAllPartitions(hTable: HiveTable): Seq[HivePartition]

  /** Returns partitions filtered by predicates for the given table. */
  def getPartitionsByFilter(hTable: HiveTable, predicates: Seq[Expression]): Seq[HivePartition]

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

  /** Add a jar into class loader */
  def addJar(path: String): Unit

  /** Return a ClientInterface as new session, that will share the class loader and Hive client */
  def newSession(): ClientInterface

  /** Run a function within Hive state (SessionState, HiveConf, Hive client and class loader) */
  def withHiveState[A](f: => A): A

  /** Used for testing only.  Removes all metadata from this instance of Hive. */
  def reset(): Unit
}
