package catalyst
package planning


import plans.logical.LogicalPlan
import trees._

abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  def strategies: Seq[Strategy]

  abstract protected class Strategy {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan]

  }

  // TODO: Actually plan later.
  protected def planLater(plan: LogicalPlan) = apply(plan).next()

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}