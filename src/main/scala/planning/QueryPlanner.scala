package catalyst
package planning

import catalyst.plans.logical.LogicalPlan
import catalyst.plans.physical.PhysicalPlan

abstract class QueryPlanner {
  def strategies: Seq[Strategy]

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    strategies.head(plan).toIterator
  }
}

object TrivalPlanner extends QueryPlanner {
  val strategies = HiveTableScans :: Nil
}