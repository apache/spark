package catalyst
package optimizer

import catalyst.expressions._
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types.BooleanType

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries) ::
    Batch("ConstantFolding", Once,
      ConstantFolding
    ) :: Nil
}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

// Do I really need post order traversal at here????
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsPostOrder {
      case a @ And(left, right) if !a.foldable => {
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => a
        }
      }
      case o @ Or(left, right) if !o.foldable => {
        (left, right) match {
          case (Literal(true, BooleanType), _) => Literal(true)
          case (_, Literal(true, BooleanType)) => Literal(true)
          case (Literal(false, BooleanType), r) => r
          case (l, Literal(false, BooleanType)) => l
          case (_, _) => o
        }
      }
      case e if e.foldable => {Literal(Evaluate(e, Nil))}
    }
  }
}