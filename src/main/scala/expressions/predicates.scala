package catalyst
package expressions

import types._

abstract class Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType
}

abstract class BinaryPredicate(op: String) extends Predicate with trees.BinaryNode[Expression] {
  self: Product =>

  def nullable = left.nullable || right.nullable
  def references = left.references ++ right.references
  override def toString(): String = s"$left $op $right"
}

case class Equals(left: Expression, right: Expression) extends BinaryPredicate("=")
case class LessThan(left: Expression, right: Expression) extends BinaryPredicate("<")
case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate("<=")
case class GreaterThan(left: Expression, right: Expression) extends BinaryPredicate(">")
case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryPredicate(">=")
