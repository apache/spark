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

import org.apache.spark.sql.catalyst.catalog.{Catalog, Database, Function, Table, TablePartition}
import org.apache.spark.sql.hive.client.HiveClient


/**
 * A persistent implementation of the system catalog using Hive.
 *
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveCatalog(client: HiveClient) extends Catalog {
  import Catalog._

  def createDatabase(dbDefinition: Database, ignoreIfExists: Boolean): Unit = synchronized {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

  /**
   * Alter an existing database. This operation does not support renaming.
   */
  def alterDatabase(db: String, dbDefinition: Database): Unit = ???

  def getDatabase(db: String): Database = ???

  def listDatabases(): Seq[String] = ???

  def listDatabases(pattern: String): Seq[String] = ???

  def createTable(db: String, tableDefinition: Table, ignoreIfExists: Boolean): Unit = ???

  def dropTable(db: String, table: String, ignoreIfNotExists: Boolean): Unit = ???

  def renameTable(db: String, oldName: String, newName: String): Unit = ???

  /**
   * Alter an existing table. This operation does not support renaming.
   */
  def alterTable(db: String, table: String, tableDefinition: Table): Unit = ???

  def getTable(db: String, table: String): Table = ???

  def listTables(db: String): Seq[String] = ???

  def listTables(db: String, pattern: String): Seq[String] = ???

  def createPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartition],
      ignoreIfExists: Boolean): Unit = ???

  def dropPartitions(
      db: String,
      table: String,
      parts: Seq[PartitionSpec],
      ignoreIfNotExists: Boolean): Unit = ???

  /**
   * Alter an existing table partition and optionally override its spec.
   */
  def alterPartition(
      db: String,
      table: String,
      spec: PartitionSpec,
      newPart: TablePartition): Unit = ???

  def getPartition(db: String, table: String, spec: PartitionSpec): TablePartition = ???

  def listPartitions(db: String, table: String): Seq[TablePartition] = ???

  def createFunction(db: String, funcDefinition: Function, ignoreIfExists: Boolean): Unit = ???

  def dropFunction(db: String, funcName: String): Unit = ???

  /**
   * Alter an existing function and optionally override its name.
   */
  def alterFunction(db: String, funcName: String, funcDefinition: Function): Unit = ???

  def getFunction(db: String, funcName: String): Function = ???

  def listFunctions(db: String, pattern: String): Seq[String] = ???

}
