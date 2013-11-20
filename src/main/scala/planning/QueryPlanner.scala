package catalyst
package planning


import plans.logical.LogicalPlan
import trees._

/**
 * Extended by classes that transform [[plans.logical.LogicalPlan]]s into Physical plans.  Child classes are
 * responsible for specifying a list of [[Strategy]] objects that each of which can return a list of possible
 * physical plan options.  If a given strategy is unable to plan all of the remaining operators in the tree, it can
 * call [[planLater]], which returns a placeholder object that will be filled in using other available strategies.
 *
 * NOTE: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER... PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[Strategy]

  /**
   * Given a [[plan.logical.LogicalPlan]], returns a list of [[PhysicalPlans]] that can be used for execution. If this
   * strategy does not apply to the give logical operation then an empty list should be returned.
   */
  abstract protected class Strategy {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan]

  }

  /**
   * Returns a placeholder for a physical plan that executes [[plan]].  This placeholder will be filled in automatically
   * by the QueryPlanner using the other execution strategies that are available.
   */
  protected def planLater(plan: LogicalPlan) = apply(plan).next()

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}