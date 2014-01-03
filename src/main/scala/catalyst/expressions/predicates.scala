package catalyst
package expressions

import types._

trait Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType
}

abstract class BinaryPredicate extends BinaryExpression with Predicate {
  self: Product =>

  def nullable = left.nullable || right.nullable
}

case class Not(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = child.nullable
  override def toString = s"NOT $child"
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "&&"
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "||"
}

abstract class BinaryComparison extends BinaryPredicate {
  self: Product =>
}

case class Equals(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "="
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<"
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<="
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">"
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">="
}

case class IsNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
}

case class IsNotNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
}
