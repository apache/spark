package catalyst
package plans

import expressions.{Attribute, Expression}
import trees._

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>

  def output: Seq[Attribute]

  /**
   * Runs [[transform]] with [[rule]] on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression) = {
      val newE = e.transform(rule)
      if(newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpression(e)
      case Some(e: Expression) => Some(transformExpression(e))
      case seq: Seq[_] => seq.map {
        case e: Expression => transformExpression(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if(changed) makeCopy(newArgs) else this
  }

  /**
   * Runs [[transformPostOrder]] with [[rule]] on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsPostOrder(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionPostOrder(e: Expression) = {
      val newE = e.transformPostOrder(rule)
      if(newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionPostOrder(e)
      case Some(e: Expression) => Some(transformExpressionPostOrder(e))
      case seq: Seq[_] => seq.map {
        case e: Expression => transformExpressionPostOrder(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if(changed) makeCopy(newArgs) else this
  }

  /** Returns the result of running [[transformExpressions]] on this node and all its children */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  def expressions: Seq[Expression] = {
    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Seq[_] => seq.flatMap {
        case e: Expression => e :: Nil
        case other => Nil
      }
      case other => Nil
    }.toSeq
  }
}