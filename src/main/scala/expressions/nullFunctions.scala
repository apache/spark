package catalyst
package expressions

import catalyst.analysis.UnresolvedException

case class Coalesce(children: Seq[Expression]) extends Expression {
  def nullable = children.map(_.nullable).reduce(_&&_)
  def references = children.flatMap(_.references).toSet
  def foldable = children.map(_.foldable).reduce(_&&_)

  // Only resolved if all the children are of the same type.
  override lazy val resolved = childrenResolved && (children.map(_.dataType).distinct.size == 1)

  override def toString = s"Coalesce(${children.mkString(",")})"

  def dataType =
    if(resolved)
      children.head.dataType
    else
      throw new UnresolvedException(this, "Coalesce cannot have children of different types.")
}