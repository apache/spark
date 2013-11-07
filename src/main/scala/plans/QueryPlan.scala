package catalyst
package plans

import trees._

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>
}