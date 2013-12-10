package catalyst
package expressions

import catalyst.types._

abstract class AggregateExpression extends Expression {
  self: Product =>
}

/**
 * A specific implementation of an aggregate function. Used to wrap a generic [[AggregateExpression]] with an
 * algorithm that will be used to compute the result.
 */
abstract class AggregateFunction extends AggregateExpression with Serializable with trees.LeafNode[Expression] {
  self: Product =>

  /** Base should return the generic aggregate expression that this function is computing */
  val base: AggregateExpression
  def references = base.references
  def nullable = base.nullable
  def dataType = base.dataType

  def apply(input: Seq[Seq[Any]]): Unit
  def result: Any
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