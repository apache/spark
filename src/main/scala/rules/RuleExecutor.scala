package catalyst
package rules

import trees._

abstract class RuleExecutor[TreeType <: TreeNode[_]] {
  abstract class Strategy { def maxIterations: Int }
  case object Once extends Strategy { val maxIterations = 1 }
  case class FixedPoint(maxIterations: Int) extends Strategy

  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  protected val batches: Seq[Batch]

  def apply(plan: TreeType): TreeType = {
    var curPlan = plan

    batches.foreach { batch =>
      var iteration = 1
      var lastPlan = curPlan
      curPlan = batch.rules.foldLeft(curPlan) { case (curPlan, rule) => rule(curPlan) }

      while(iteration < batch.strategy.maxIterations &&
            !(curPlan == lastPlan)) {
        lastPlan = curPlan
        curPlan = batch.rules.foldLeft(curPlan) { case (curPlan, rule) => rule(curPlan) }
        iteration += 1
      }

    }

    curPlan
  }
}