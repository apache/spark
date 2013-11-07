package catalyst
package analysis

import plans.logical.LogicalPlan

abstract class Catalog {
  def lookupRelation(name: String, alias: Option[String]): LogicalPlan
}