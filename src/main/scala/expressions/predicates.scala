package catalyst
package expressions

import types._

abstract trait Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType
}

abstract class BinaryPredicate extends BinaryExpression with Predicate {
  self: Product =>

  def nullable = left.nullable || right.nullable
}

case class Equals(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "="
}
case class LessThan(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "<"
}
case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "<="
}
case class GreaterThan(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = ">"
}
case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = ">="
}
