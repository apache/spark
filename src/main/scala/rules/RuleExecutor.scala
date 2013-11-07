package catalyst
package rules

import plans._

abstract class RuleExecutor[PlanType <: QueryPlan[_]] {
  case class Batch(name: String, rules: Rule[PlanType]*)

  val batches: Seq[Batch]

  def apply(plan: PlanType): PlanType = {
    batches.foldLeft(plan) {
      case (curPlan, batch) => batch.rules.foldRight(curPlan)(_.apply(_))
    }
  }
}