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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{TableIdentifier, CatalystConf, EmptyConf}
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

  val conf: CatalystConf

  def tableExists(tableIdent: TableIdentifier): Boolean

  def lookupRelation(tableIdent: TableIdentifier, alias: Option[String] = None): LogicalPlan

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   */
  def getTables(databaseName: Option[String]): Seq[(String, Boolean)]

  def refreshTable(tableIdent: TableIdentifier): Unit

  def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit

  def unregisterTable(tableIdent: TableIdentifier): Unit

  def unregisterAllTables(): Unit

  /**
   * Get the table name of TableIdentifier for temporary tables.
   */
  protected def getTableName(tableIdent: TableIdentifier): String = {
    // It is not allowed to specify database name for temporary tables.
    // We check it here and throw exception if database is defined.
    if (tableIdent.database.isDefined) {
      throw new AnalysisException("Specifying database name or other qualifiers are not allowed " +
        "for temporary tables. If the table name has dots (.) in it, please quote the " +
        "table name with backticks (`).")
    }
    if (conf.caseSensitiveAnalysis) {
      tableIdent.table
    } else {
      tableIdent.table.toLowerCase
    }
  }
}

class SimpleCatalog(val conf: CatalystConf) extends Catalog {
  private[this] val tables: ConcurrentMap[String, LogicalPlan] =
    new ConcurrentHashMap[String, LogicalPlan]

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    tables.put(getTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    tables.remove(getTableName(tableIdent))
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
  }

  override def tableExists(tableIdent: TableIdentifier): Boolean = {
    tables.containsKey(getTableName(tableIdent))
  }

  override def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    val tableName = getTableName(tableIdent)
    val table = tables.get(tableName)
    if (table == null) {
      throw new NoSuchTableException
    }
    val tableWithQualifiers = Subquery(tableName, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    tables.keySet().asScala.map(_ -> true).toSeq
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
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
  private[this] val overrides = new ConcurrentHashMap[String, LogicalPlan]

  private def getOverriddenTable(tableIdent: TableIdentifier): Option[LogicalPlan] = {
    if (tableIdent.database.isDefined) {
      None
    } else {
      Option(overrides.get(getTableName(tableIdent)))
    }
  }

  abstract override def tableExists(tableIdent: TableIdentifier): Boolean = {
    getOverriddenTable(tableIdent) match {
      case Some(_) => true
      case None => super.tableExists(tableIdent)
    }
  }

  abstract override def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    getOverriddenTable(tableIdent) match {
      case Some(table) =>
        val tableName = getTableName(tableIdent)
        val tableWithQualifiers = Subquery(tableName, table)

        // If an alias was specified by the lookup, wrap the plan in a sub-query so that attributes
        // are properly qualified with this alias.
        alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)

      case None => super.lookupRelation(tableIdent, alias)
    }
  }

  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    overrides.keySet().asScala.map(_ -> true).toSeq ++ super.getTables(databaseName)
  }

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    overrides.put(getTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    if (tableIdent.database.isEmpty) {
      overrides.remove(getTableName(tableIdent))
    }
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

  override val conf: CatalystConf = EmptyConf

  override def tableExists(tableIdent: TableIdentifier): Boolean = {
    throw new UnsupportedOperationException
  }

  override def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    throw new UnsupportedOperationException
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    throw new UnsupportedOperationException
  }

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {
    throw new UnsupportedOperationException
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }
}
