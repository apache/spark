package catalyst
package optimizer

import catalyst.expressions._
import catalyst.plans.logical._
import catalyst.rules._

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries) ::
    Batch("EvaluateLiterals", Once,
      EvaluateLiterals) :: Nil

}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

object EvaluateLiterals extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case project @ Project(projectList, child) => project transformExpressionsPostOrder {
      case b: BinaryExpression
        if b.left.isInstanceOf[Literal] && b.right.isInstanceOf[Literal] => {
        Literal(Evaluate(b, Nil))
      }
    }
  }
}