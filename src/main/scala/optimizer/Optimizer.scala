package catalyst
package optimizer

import catalyst.plans.logical._
import catalyst.rules._

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries) :: Nil

}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}