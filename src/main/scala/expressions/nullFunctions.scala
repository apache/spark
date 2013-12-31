package catalyst
package expressions

import catalyst.analysis.UnresolvedException

case class Coalesce(children: Seq[Expression]) extends Expression {
  def nullable = children.map(_.nullable).reduce(_&&_)
  def references = children.flatMap(_.references).toSet

  // Only resolved if all the children are of the same type.
  override lazy val resolved = childrenResolved && (children.map(_.dataType).distinct.size == 1)

  def dataType =
    if(resolved)
      children.head.dataType
    else
      throw new UnresolvedException(this, "Coalesce cannot have children of different types.")
}