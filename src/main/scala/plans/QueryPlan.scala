package catalyst
package plans

import expressions.Attribute
import trees._

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>

  def output: Seq[Attribute]
}