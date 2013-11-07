package catalyst
package rules

import plans._

abstract class RuleExecutor[PlanType <: QueryPlan[_]] {
  protected case class Batch(name: String, rules: Rule[PlanType]*)

  protected val batches: Seq[Batch]

  def apply(plan: PlanType): PlanType = {
    batches.foldLeft(plan) {
      case (curPlan, batch) => batch.rules.foldRight(curPlan)(_.apply(_))
    }
  }
}