package catalyst
package expressions

import catalyst.types._

abstract class AggregateExpression extends Expression {
  self: Product =>
}

case class Count(child: Expression) extends AggregateExpression with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = IntegerType
  override def toString = s"COUNT($child)"
}

case class CountDistinct(expressions: Seq[Expression]) extends AggregateExpression {
  def children = expressions
  def references = expressions.flatMap(_.references).toSet
  def nullable = false
  def dataType = IntegerType
  override def toString = s"COUNT(DISTINCT ${expressions.mkString(",")}})"
}

case class Average(child: Expression) extends AggregateExpression with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = DoubleType
  override def toString = s"AVG($child)"
}

case class Sum(child: Expression) extends AggregateExpression with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = child.dataType
  override def toString = s"SUM($child)"
}