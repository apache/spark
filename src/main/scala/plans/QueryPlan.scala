package catalyst
package plans

import expressions.{Attribute, Expression}
import trees._

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>

  def output: Seq[Attribute]

  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false


    @inline def transformExpression(e: Expression) = {
      val newE = e.transform(rule)
      println("te")
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
      case seqE: Seq[Expression] if !seqE.isEmpty && classOf[Expression].isAssignableFrom(seqE.head.getClass) =>
        seqE.map(transformExpression)
      case other: AnyRef => other
    }.toArray

    if(changed) makeCopy(newArgs) else this
  }
}