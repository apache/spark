package catalyst
package plans

import expressions.{Attribute, Expression}
import trees._

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: Set[Attribute] = output.toSet

  /**
   * Runs [[transformDown]] with [[rule]] on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression) = {
      val newE = e.transformDown(rule)
      if (newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case seq: Seq[_] => seq.map {
        case e: Expression => transformExpressionDown(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Runs [[transformUp]] with [[rule]] on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression) = {
      val newE = e.transformUp(rule)
      if (newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case seq: Seq[_] => seq.map {
        case e: Expression => transformExpressionUp(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /** Returns the result of running [[transformExpressionsDown]] on this node
    * and all its children. */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformDown {
      case q: QueryPlan[_] => q.transformExpressionsDown(rule).asInstanceOf[PlanType]
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