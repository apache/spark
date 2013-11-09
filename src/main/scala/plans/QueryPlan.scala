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
}