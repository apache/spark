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
  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan

  def registerTable(databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit

  def unregisterTable(databaseName: Option[String], tableName: String): Unit

  def unregisterAllTables(): Unit
}

class SimpleCatalog extends Catalog {
  val tables = new mutable.HashMap[String, LogicalPlan]()

  override def registerTable(
      databaseName: Option[String],
      tableName: String,
      plan: LogicalPlan): Unit = {
    tables += ((tableName, plan))
  }

  override def unregisterTable(
      databaseName: Option[String],
      tableName: String) = {
    tables -= tableName
  }

  override def unregisterAllTables() = {
    tables.clear()
  }

  override def lookupRelation(
      databaseName: Option[String],
      tableName: String,
      alias: Option[String] = None): LogicalPlan = {
    val table = tables.get(tableName).getOrElse(sys.error(s"Table Not Found: $tableName"))
    val tableWithQualifiers = Subquery(tableName, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a.toLowerCase, tableWithQualifiers)).getOrElse(tableWithQualifiers)
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

  abstract override def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan = {

    val overriddenTable = overrides.get((databaseName, tableName))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    val withAlias =
      overriddenTable.map(r => alias.map(a => Subquery(a.toLowerCase, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(databaseName, tableName, alias))
  }

  override def registerTable(
      databaseName: Option[String],
      tableName: String,
      plan: LogicalPlan): Unit = {
    overrides.put((databaseName, tableName), plan)
  }

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit = {
    overrides.remove((databaseName, tableName))
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
