package catalyst
package analysis

import plans.logical.LogicalPlan

abstract class Catalog {
  def lookupRelation(name: String, alias: Option[String] = None): LogicalPlan
}

object EmptyCatalog extends Catalog {
  def lookupRelation(name: String, alias: Option[String] = None) = ???
}