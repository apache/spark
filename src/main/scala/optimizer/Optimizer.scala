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

/*
* An optimization rule to evaluate literals appearing in expressions.
* It traverses the expressions in a post order to visit BinaryExpression.
* When it finds both the left child and right child of a node are literals,
* it evaluates the current visiting BinaryExpression.
* Because, currently, we evaluate literals based on the structure of the expression
* tree, key+1+1 will not be transformed to key+2.
* */
object EvaluateLiterals extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsPostOrder {
      case b: BinaryExpression
        if b.left.isInstanceOf[Literal] && b.right.isInstanceOf[Literal] => {
        Literal(Evaluate(b, Nil))
      }
    }
  }
}