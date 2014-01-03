package catalyst
package expressions

import catalyst.analysis.UnresolvedException

case class Coalesce(children: Seq[Expression]) extends Expression {

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  def nullable = !children.exists(!_.nullable)

  def references = children.flatMap(_.references).toSet

  // Only resolved if all the children are of the same type.
  override lazy val resolved = childrenResolved && (children.map(_.dataType).distinct.size == 1)

  override def toString = s"Coalesce(${children.mkString(",")})"

  def dataType = if (resolved) {
    children.head.dataType
  } else {
    throw new UnresolvedException(this, "Coalesce cannot have children of different types.")
  }
}
