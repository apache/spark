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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException


/**
 * An in-memory (ephemeral) implementation of the system catalog.
 *
 * All public methods should be synchronized for thread-safety.
 */
class InMemoryCatalog extends Catalog {

  private class TableDesc(var table: Table) {
    val partitions = new mutable.HashMap[String, TablePartition]
  }

  private class DatabaseDesc(var db: Database) {
    val tables = new mutable.HashMap[String, TableDesc]
    val functions = new mutable.HashMap[String, Function]
  }

  private val catalog = new scala.collection.mutable.HashMap[String, DatabaseDesc]

  private def filterPattern(names: Seq[String], pattern: String): Seq[String] = {
    val regex = pattern.replaceAll("\\*", ".*").r
    names.filter { funcName => regex.pattern.matcher(funcName).matches() }
  }

  private def existsFunction(db: String, funcName: String): Boolean = {
    catalog(db).functions.contains(funcName)
  }

  private def existsTable(db: String, table: String): Boolean = {
    catalog(db).tables.contains(table)
  }

  private def assertDbExists(db: String): Unit = {
    if (!catalog.contains(db)) {
      throw new AnalysisException(s"Database $db does not exist")
    }
  }

  private def assertFunctionExists(db: String, funcName: String): Unit = {
    assertDbExists(db)
    if (!existsFunction(db, funcName)) {
      throw new AnalysisException(s"Function $funcName does not exists in $db database")
    }
  }

  private def assertTableExists(db: String, table: String): Unit = {
    assertDbExists(db)
    if (!existsTable(db, table)) {
      throw new AnalysisException(s"Table $table does not exists in $db database")
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(dbDefinition: Database, ifNotExists: Boolean): Unit = synchronized {
    if (catalog.contains(dbDefinition.name)) {
      if (!ifNotExists) {
        throw new AnalysisException(s"Database ${dbDefinition.name} already exists.")
      }
    } else {
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
          throw new AnalysisException(s"Database $db is not empty. One or more tables exist.")
        }
        if (catalog(db).functions.nonEmpty) {
          throw new AnalysisException(s"Database $db is not empty. One or more functions exist.")
        }
      }
      // Remove the database.
      catalog.remove(db)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Database $db does not exist")
      }
    }
  }

  override def alterDatabase(db: String, dbDefinition: Database): Unit = synchronized {
    assertDbExists(db)
    assert(db == dbDefinition.name)
    catalog(db).db = dbDefinition
  }

  override def getDatabase(db: String): Database = synchronized {
    assertDbExists(db)
    catalog(db).db
  }

  override def listDatabases(): Seq[String] = synchronized {
    catalog.keySet.toSeq
  }

  override def listDatabases(pattern: String): Seq[String] = synchronized {
    filterPattern(listDatabases(), pattern)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(db: String, tableDefinition: Table, ifNotExists: Boolean)
  : Unit = synchronized {
    assertDbExists(db)
    if (existsTable(db, tableDefinition.name)) {
      if (!ifNotExists) {
        throw new AnalysisException(s"Table ${tableDefinition.name} already exists in $db database")
      }
    } else {
      catalog(db).tables.put(tableDefinition.name, new TableDesc(tableDefinition))
    }
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean)
  : Unit = synchronized {
    assertDbExists(db)
    if (existsTable(db, table)) {
      catalog(db).tables.remove(table)
    } else {
      if (!ignoreIfNotExists) {
        throw new AnalysisException(s"Table $table does not exist in $db database")
      }
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = synchronized {
    assertTableExists(db, oldName)
    val oldDesc = catalog(db).tables(oldName)
    oldDesc.table = oldDesc.table.copy(name = newName)
    catalog(db).tables.put(newName, oldDesc)
    catalog(db).tables.remove(oldName)
  }

  override def alterTable(db: String, table: String, tableDefinition: Table): Unit = synchronized {
    assertTableExists(db, table)
    assert(table == tableDefinition.name)
    catalog(db).tables(table).table = tableDefinition
  }

  override def getTable(db: String, table: String): Table = synchronized {
    assertTableExists(db, table)
    catalog(db).tables(table).table
  }

  override def listTables(db: String): Seq[String] = synchronized {
    assertDbExists(db)
    catalog(db).tables.keySet.toSeq
  }

  override def listTables(db: String, pattern: String): Seq[String] = synchronized {
    assertDbExists(db)
    filterPattern(listTables(db), pattern)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def alterPartition(db: String, table: String, part: TablePartition)
  : Unit = synchronized {
    throw new UnsupportedOperationException
  }

  override def alterPartitions(db: String, table: String, parts: Seq[TablePartition])
  : Unit = synchronized {
    throw new UnsupportedOperationException
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(
      db: String, func: Function, ifNotExists: Boolean): Unit = synchronized {
    assertDbExists(db)

    if (existsFunction(db, func.name)) {
      if (!ifNotExists) {
        throw new AnalysisException(s"Function $func already exists in $db database")
      }
    } else {
      catalog(db).functions.put(func.name, func)
    }
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    assertFunctionExists(db, funcName)
    catalog(db).functions.remove(funcName)
  }

  override def alterFunction(db: String, funcName: String, funcDefinition: Function)
    : Unit = synchronized {
    assertFunctionExists(db, funcName)
    if (funcName != funcDefinition.name) {
      // Also a rename; remove the old one and add the new one back
      catalog(db).functions.remove(funcName)
    }
    catalog(db).functions.put(funcName, funcDefinition)
  }

  override def getFunction(db: String, funcName: String): Function = synchronized {
    assertFunctionExists(db, funcName)
    catalog(db).functions(funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = synchronized {
    assertDbExists(db)
    val regex = pattern.replaceAll("\\*", ".*").r
    filterPattern(catalog(db).functions.keysIterator.toSeq, pattern)
  }

}
