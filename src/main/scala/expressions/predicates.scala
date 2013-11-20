package catalyst
package expressions

import types._

abstract trait Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType
}

abstract class BinaryPredicate(symbol: String) extends BinaryExpression(symbol) with Predicate {
  self: Product =>

  def nullable = left.nullable || right.nullable
}

case class Equals(left: Expression, right: Expression) extends BinaryPredicate("=")
case class LessThan(left: Expression, right: Expression) extends BinaryPredicate("<")
case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate("<=")
case class GreaterThan(left: Expression, right: Expression) extends BinaryPredicate(">")
case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate(">=")
