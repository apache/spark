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

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Event emitted by the external catalog when it is modified. Events are either fired before or
 * after the modification (the event should document this).
 */
trait ExternalCatalogEvent extends SparkListenerEvent

/**
 * Listener interface for external catalog modification events.
 */
trait ExternalCatalogEventListener {
  def onEvent(event: ExternalCatalogEvent): Unit
}

/**
 * Event fired when a database is create or dropped.
 */
trait DatabaseEvent extends ExternalCatalogEvent {
  /**
   * Database of the object that was touched.
   */
  val database: String
}

/**
 * Event fired before a database is created.
 */
case class CreateDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * Event fired after a database has been created.
 */
case class CreateDatabaseEvent(database: String) extends DatabaseEvent

/**
 * Event fired before a database is dropped.
 */
case class DropDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * Event fired after a database has been dropped.
 */
case class DropDatabaseEvent(database: String) extends DatabaseEvent

/**
 * Event fired before a database is altered.
 */
case class AlterDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * Event fired after a database is altered.
 */
case class AlterDatabaseEvent(database: String) extends DatabaseEvent

/**
 * Event fired when a table is created, dropped or renamed.
 */
trait TableEvent extends DatabaseEvent {
  /**
   * Name of the table that was touched.
   */
  val name: String
}

/**
 * Event fired before a table is created.
 */
case class CreateTablePreEvent(database: String, name: String) extends TableEvent

/**
 * Event fired after a table has been created.
 */
case class CreateTableEvent(database: String, name: String) extends TableEvent

/**
 * Event fired before a table is dropped.
 */
case class DropTablePreEvent(database: String, name: String) extends TableEvent

/**
 * Event fired after a table has been dropped.
 */
case class DropTableEvent(database: String, name: String) extends TableEvent

/**
 * Event fired before a table is renamed.
 */
case class RenameTablePreEvent(
    database: String,
    name: String,
    newName: String)
  extends TableEvent

/**
 * Event fired after a table has been renamed.
 */
case class RenameTableEvent(
    database: String,
    name: String,
    newName: String)
  extends TableEvent

/**
 * String to indicate which part of table is altered. If a plain alterTable API is called, then
 * type will generally be Table.
 */
object AlterTableKind extends Enumeration {
  val TABLE = "table"
  val DATASCHEMA = "dataSchema"
  val STATS = "stats"
}

/**
 * Event fired before a table is altered.
 */
case class AlterTablePreEvent(
    database: String,
    name: String,
    kind: String) extends TableEvent

/**
 * Event fired after a table is altered.
 */
case class AlterTableEvent(
    database: String,
    name: String,
    kind: String) extends TableEvent

/**
 * Event fired when a function is created, dropped, altered or renamed.
 */
trait FunctionEvent extends DatabaseEvent {
  /**
   * Name of the function that was touched.
   */
  val name: String
}

/**
 * Event fired before a function is created.
 */
case class CreateFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired after a function has been created.
 */
case class CreateFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired before a function is dropped.
 */
case class DropFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired after a function has been dropped.
 */
case class DropFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired before a function is altered.
 */
case class AlterFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired after a function has been altered.
 */
case class AlterFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * Event fired before a function is renamed.
 */
case class RenameFunctionPreEvent(
    database: String,
    name: String,
    newName: String)
  extends FunctionEvent

/**
 * Event fired after a function has been renamed.
 */
case class RenameFunctionEvent(
    database: String,
    name: String,
    newName: String)
  extends FunctionEvent
