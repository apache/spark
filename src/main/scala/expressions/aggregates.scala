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

case class Average(child: Expression) extends AggregateExpression with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = DoubleType
  override def toString = s"AVG($child)"
}