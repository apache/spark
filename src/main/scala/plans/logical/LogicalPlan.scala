package catalyst
package plans
package logical

import trees._

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>
}

abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>
}

abstract class UnaryNode extends LogicalPlan with trees.UnaryNode[LogicalPlan] {
  self: Product =>
}