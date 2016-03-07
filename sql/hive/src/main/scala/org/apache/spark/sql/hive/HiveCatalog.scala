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

package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.thrift.TException

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchItemException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.client.HiveClient


/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveCatalog(client: HiveClient) extends ExternalCatalog with Logging {
  import ExternalCatalog._

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    classOf[HiveException].getCanonicalName,
    classOf[TException].getCanonicalName)

  /**
   * Whether this is an exception thrown by the hive client that should be wrapped.
   *
   * Due to classloader isolation issues, pattern matching won't work here so we need
   * to compare the canonical names of the exceptions, which we assume to be stable.
   */
  private def isClientException(e: Throwable): Boolean = {
    var temp: Class[_] = e.getClass
    var found = false
    while (temp != null && !found) {
      found = clientExceptions.contains(temp.getCanonicalName)
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Run some code involving `client` in a [[synchronized]] block and wrap certain
   * exceptions thrown in the process in [[AnalysisException]].
   */
  private def withClient[T](body: => T): T = synchronized {
    try {
      body
    } catch {
      case e: NoSuchItemException =>
        throw new AnalysisException(e.getMessage)
      case NonFatal(e) if isClientException(e) =>
        throw new AnalysisException(e.getClass.getCanonicalName + ": " + e.getMessage)
    }
  }

  private def requireDbMatches(db: String, table: CatalogTable): Unit = {
    if (table.specifiedDatabase != Some(db)) {
      throw new AnalysisException(
        s"Provided database $db does not much the one specified in the " +
        s"table definition (${table.specifiedDatabase.getOrElse("n/a")})")
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    withClient { getTable(db, table) }
  }


  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    client.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: As of now, this only supports altering database properties!
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = withClient {
    val existingDb = getDatabase(dbDefinition.name)
    if (existingDb.properties == dbDefinition.properties) {
      logWarning(s"Request to alter database ${dbDefinition.name} is a no-op because " +
        s"the provided database properties are the same as the old ones. Hive does not " +
        s"currently support altering other database fields.")
    }
    client.alterDatabase(dbDefinition)
  }

  override def getDatabase(db: String): CatalogDatabase = withClient {
    client.getDatabase(db)
  }

  override def databaseExists(db: String): Boolean = withClient {
    client.getDatabaseOption(db).isDefined
  }

  override def listDatabases(): Seq[String] = withClient {
    client.listDatabases("*")
  }

  override def listDatabases(pattern: String): Seq[String] = withClient {
    client.listDatabases(pattern)
  }

  override def setCurrentDatabase(db: String): Unit = withClient {
    client.setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      db: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = withClient {
    requireDbExists(db)
    requireDbMatches(db, tableDefinition)
    client.createTable(tableDefinition, ignoreIfExists)
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean): Unit = withClient {
    requireDbExists(db)
    client.dropTable(db, table, ignoreIfNotExists)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = withClient {
    val newTable = client.getTable(db, oldName).copy(name = newName)
    client.alterTable(oldName, newTable)
  }

  /**
   * Alter a table whose name that matches the one specified in `tableDefinition`,
   * assuming the table exists.
   *
   * Note: As of now, this only supports altering table properties, serde properties,
   * and num buckets!
   */
  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = withClient {
    requireDbMatches(db, tableDefinition)
    requireTableExists(db, tableDefinition.name)
    client.alterTable(tableDefinition)
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    client.getTable(db, table)
  }

  override def listTables(db: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listTables(db, pattern)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = withClient {
    requireTableExists(db, table)
    // Note: Unfortunately Hive does not currently support `ignoreIfNotExists` so we
    // need to implement it here ourselves. This is currently somewhat expensive because
    // we make multiple synchronous calls to Hive for each partition we want to drop.
    val partsToDrop =
      if (ignoreIfNotExists) {
        parts.filter { spec =>
          try {
            getPartition(db, table, spec)
            true
          } catch {
            // Filter out the partitions that do not actually exist
            case _: AnalysisException => false
          }
        }
      } else {
        parts
      }
    if (partsToDrop.nonEmpty) {
      client.dropPartitions(db, table, partsToDrop)
    }
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withClient {
    client.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    client.alterPartitions(db, table, newParts)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withClient {
    client.getPartition(db, table, spec)
  }

  override def listPartitions(
      db: String,
      table: String): Seq[CatalogTablePartition] = withClient {
    client.getAllPartitions(db, table)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(
      db: String,
      funcDefinition: CatalogFunction): Unit = withClient {
    client.createFunction(db, funcDefinition)
  }

  override def dropFunction(db: String, name: String): Unit = withClient {
    client.dropFunction(db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withClient {
    client.renameFunction(db, oldName, newName)
  }

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = withClient {
    client.alterFunction(db, funcDefinition)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = withClient {
    client.getFunction(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withClient {
    client.listFunctions(db, pattern)
  }

}
