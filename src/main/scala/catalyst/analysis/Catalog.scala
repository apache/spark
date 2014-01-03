package catalyst
package analysis

import plans.logical.LogicalPlan

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
abstract trait Catalog {
  def lookupRelation(name: String, alias: Option[String] = None): LogicalPlan
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all relations are
 * already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyCatalog extends Catalog {
  def lookupRelation(name: String, alias: Option[String] = None) = ???
}