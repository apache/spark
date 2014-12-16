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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {

  def caseSensitive: Boolean

  def tableExists(db: Option[String], tableName: String): Boolean

  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan

  def registerTable(databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit

  def unregisterTable(databaseName: Option[String], tableName: String): Unit

  def unregisterAllTables(): Unit

  protected def processDatabaseAndTableName(
      databaseName: Option[String],
      tableName: String): (Option[String], String) = {
    if (!caseSensitive) {
      (databaseName.map(_.toLowerCase), tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: String,
      tableName: String): (String, String) = {
    if (!caseSensitive) {
      (databaseName.toLowerCase, tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }
}

class SimpleCatalog(val caseSensitive: Boolean) extends Catalog {
  val tables = new mutable.HashMap[String, LogicalPlan]()

  override def registerTable(
      databaseName: Option[String],
      tableName: String,
      plan: LogicalPlan): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    tables += ((tblName, plan))
  }

  override def unregisterTable(
      databaseName: Option[String],
      tableName: String) = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    tables -= tblName
  }

  override def unregisterAllTables() = {
    tables.clear()
  }

  override def tableExists(db: Option[String], tableName: String): Boolean = {
    val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
    tables.get(tblName) match {
      case Some(_) => true
      case None => false
    }
  }

  override def lookupRelation(
      databaseName: Option[String],
      tableName: String,
      alias: Option[String] = None): LogicalPlan = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val table = tables.getOrElse(tblName, sys.error(s"Table Not Found: $tableName"))
    val tableWithQualifiers = Subquery(tblName, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }
}

/**
 * A trait that can be mixed in with other Catalogs allowing specific tables to be overridden with
 * new logical plans.  This can be used to bind query result to virtual tables, or replace tables
 * with in-memory cached versions.  Note that the set of overrides is stored in memory and thus
 * lost when the JVM exits.
 */
trait OverrideCatalog extends Catalog {

  // TODO: This doesn't work when the database changes...
  val overrides = new mutable.HashMap[(Option[String],String), LogicalPlan]()

  abstract override def tableExists(db: Option[String], tableName: String): Boolean = {
    val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
    overrides.get((dbName, tblName)) match {
      case Some(_) => true
      case None => super.tableExists(db, tableName)
    }
  }

  abstract override def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val overriddenTable = overrides.get((dbName, tblName))
    val tableWithQualifers = overriddenTable.map(r => Subquery(tblName, r))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    val withAlias =
      tableWithQualifers.map(r => alias.map(a => Subquery(a, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(dbName, tblName, alias))
  }

  override def registerTable(
      databaseName: Option[String],
      tableName: String,
      plan: LogicalPlan): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    overrides.put((dbName, tblName), plan)
  }

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    overrides.remove((dbName, tblName))
  }

  override def unregisterAllTables(): Unit = {
    overrides.clear()
  }
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all
 * relations are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyCatalog extends Catalog {

  val caseSensitive: Boolean = true

  def tableExists(db: Option[String], tableName: String): Boolean = {
    throw new UnsupportedOperationException
  }

  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None) = {
    throw new UnsupportedOperationException
  }

  def registerTable(databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  def unregisterTable(databaseName: Option[String], tableName: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}
}
