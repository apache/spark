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
 * Thrown by a catalog when a table cannot be found.  The analyzer will rethrow the exception
 * as an AnalysisException with the correct position information.
 */
class NoSuchTableException extends Exception

class NoSuchDatabaseException extends Exception

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {

  def caseSensitive: Boolean

  def tableExists(tableIdentifier: Seq[String]): Boolean

  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   */
  def getTables(databaseName: Option[String]): Seq[(String, Boolean)]

  def refreshTable(databaseName: String, tableName: String): Unit

  def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit

  def unregisterTable(tableIdentifier: Seq[String]): Unit

  def unregisterAllTables(): Unit

  protected def processTableIdentifier(tableIdentifier: Seq[String]): Seq[String] = {
    if (!caseSensitive) {
      tableIdentifier.map(_.toLowerCase)
    } else {
      tableIdentifier
    }
  }

  protected def getDbTableName(tableIdent: Seq[String]): String = {
    val size = tableIdent.size
    if (size <= 2) {
      tableIdent.mkString(".")
    } else {
      tableIdent.slice(size - 2, size).mkString(".")
    }
  }

  protected def getDBTable(tableIdent: Seq[String]) : (Option[String], String) = {
    (tableIdent.lift(tableIdent.size - 2), tableIdent.last)
  }
}

class SimpleCatalog(val caseSensitive: Boolean) extends Catalog {
  val tables = new mutable.HashMap[String, LogicalPlan]()

  override def registerTable(
      tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables += ((getDbTableName(tableIdent), plan))
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables -= getDbTableName(tableIdent)
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables.get(getDbTableName(tableIdent)) match {
      case Some(_) => true
      case None => false
    }
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tableFullName = getDbTableName(tableIdent)
    val table = tables.getOrElse(tableFullName, sys.error(s"Table Not Found: $tableFullName"))
    val tableWithQualifiers = Subquery(tableIdent.last, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    tables.map {
      case (name, _) => (name, true)
    }.toSeq
  }

  override def refreshTable(databaseName: String, tableName: String): Unit = {
    throw new UnsupportedOperationException
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

  abstract override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    overrides.get(getDBTable(tableIdent)) match {
      case Some(_) => true
      case None => super.tableExists(tableIdentifier)
    }
  }

  abstract override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val overriddenTable = overrides.get(getDBTable(tableIdent))
    val tableWithQualifers = overriddenTable.map(r => Subquery(tableIdent.last, r))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    val withAlias =
      tableWithQualifers.map(r => alias.map(a => Subquery(a, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(tableIdentifier, alias))
  }

  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val dbName = if (!caseSensitive) {
      if (databaseName.isDefined) Some(databaseName.get.toLowerCase) else None
    } else {
      databaseName
    }

    val temporaryTables = overrides.filter {
      // If a temporary table does not have an associated database, we should return its name.
      case ((None, _), _) => true
      // If a temporary table does have an associated database, we should return it if the database
      // matches the given database name.
      case ((db: Some[String], _), _) if db == dbName => true
      case _ => false
    }.map {
      case ((_, tableName), _) => (tableName, true)
    }.toSeq

    temporaryTables ++ super.getTables(databaseName)
  }

  override def registerTable(
      tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    overrides.put(getDBTable(tableIdent), plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    overrides.remove(getDBTable(tableIdent))
  }

  override def unregisterAllTables(): Unit = {
    overrides.clear()
  }
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all
 * relations are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyCatalog extends Catalog {

  override val caseSensitive: Boolean = true

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    throw new UnsupportedOperationException
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    throw new UnsupportedOperationException
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}

  override def refreshTable(databaseName: String, tableName: String): Unit = {
    throw new UnsupportedOperationException
  }
}
