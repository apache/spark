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
      ConstantFolding,
      BooleanSimpliﬁcation
    ) :: Nil
}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case Subquery(_, child) => child
  }
}

object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case q: LogicalPlan => q transformExpressionsUp {
      case e if e.foldable => Literal(Evaluate(e, Nil), e.dataType)
    }
  }
}

object BooleanSimpliﬁcation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) => {
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => and
        }
      }
      case or @ Or(left, right) => {
        (left, right) match {
          case (Literal(true, BooleanType), _) => Literal(true)
          case (_, Literal(true, BooleanType)) => Literal(true)
          case (Literal(false, BooleanType), r) => r
          case (l, Literal(false, BooleanType)) => l
          case (_, _) => or
        }
      }
    }
  }
}