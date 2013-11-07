package catalyst
package plans

import trees._

class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType]