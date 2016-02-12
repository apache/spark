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

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.client.HiveClient


/**
 * A persistent implementation of the system catalog using Hive.
 *
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveCatalog(client: HiveClient) extends Catalog {
  import Catalog._

  /**
   * Assert that the provided database matches the one specified in the table.
   */
  private def assertDbMatches(db: String, table: CatalogTable): Unit = {
    assert(table.specifiedDatabase == Some(db),
      "provided database does not much the one specified in the table definition")
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = synchronized {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = synchronized {
    client.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  /**
   * Alter an existing database. This operation does not support renaming.
   */
  override def alterDatabase(db: String, dbDefinition: CatalogDatabase): Unit = synchronized {
    client.alterDatabase(db, dbDefinition)
  }

  override def getDatabase(db: String): CatalogDatabase = synchronized {
    client.getDatabase(db)
  }

  override def databaseExists(db: String): Boolean = synchronized {
    client.getDatabaseOption(db).isDefined
  }

  override def listDatabases(): Seq[String] = synchronized {
    client.listDatabases("*")
  }

  override def listDatabases(pattern: String): Seq[String] = synchronized {
    client.listDatabases(pattern)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      db: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = synchronized {
    assertDbMatches(db, tableDefinition)
    client.createTable(tableDefinition, ignoreIfExists)
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    client.dropTable(db, table, ignoreIfNotExists)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = synchronized {
    val newTable = client.getTable(db, oldName).copy(name = newName)
    assertDbMatches(db, newTable)
    client.alterTable(oldName, newTable)
  }

  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = synchronized {
    assertDbMatches(db, tableDefinition)
    client.alterTable(tableDefinition)
  }

  override def getTable(db: String, table: String): CatalogTable = synchronized {
    client.getTable(db, table)
  }

  override def listTables(db: String): Seq[String] = synchronized {
    client.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = synchronized {
    client.listTables(db, pattern)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  /**
   * Alter an existing table partition and optionally override its spec.
   */
  override def alterPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec,
      newPart: CatalogTablePartition): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = synchronized {
    throw new UnsupportedOperationException
  }

  override def listPartitions(
      db: String,
      table: String): Seq[CatalogTablePartition] = synchronized {
    throw new UnsupportedOperationException
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(
      db: String,
      funcDefinition: CatalogFunction,
      ignoreIfExists: Boolean): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  /**
   * Alter an existing function and optionally override its name.
   */
  override def alterFunction(
      db: String,
      funcName: String,
      funcDefinition: CatalogFunction): Unit = synchronized {
    throw new UnsupportedOperationException
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = synchronized {
    throw new UnsupportedOperationException
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = synchronized {
    throw new UnsupportedOperationException
  }

}
