package org.apache.spark.sql
package catalyst
package analysis

import plans.logical.LogicalPlan
import scala.collection.mutable

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {
  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan
}

trait OverrideCatalog extends Catalog {

  // TODO: This doesn't work when the database changes...
  val overrides = new mutable.HashMap[(Option[String],String), LogicalPlan]()

  abstract override def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan = {

    overrides.get((databaseName, tableName))
      .getOrElse(super.lookupRelation(databaseName, tableName, alias))
  }

  def overrideTable(databaseName: Option[String], tableName: String, plan: LogicalPlan) =
    overrides.put((databaseName, tableName), plan)
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
}
