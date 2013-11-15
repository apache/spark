package catalyst
package planning

import catalyst.plans.logical.LogicalPlan
import catalyst.plans.physical.PhysicalPlan

abstract class QueryPlanner {
  def strategies: Seq[Strategy]

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}

object TrivalPlanner extends QueryPlanner {
  val strategies =
    HiveTableScans ::
    DataSinks :: Nil
}