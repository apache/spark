package catalyst
package expressions


case class UnaryMinus(child: Expression) extends UnaryExpression {
  def dataType = child.dataType
  def nullable = child.nullable
  override def toString = s"-$child"
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  def dataType = {
    require(left.dataType == right.dataType) // TODO(marmbrus): Figure out how to handle coersions.
    left.dataType
  }
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "+"
  def nullable = left.nullable || right.nullable
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"
  def nullable = left.nullable || right.nullable
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"
  def nullable = left.nullable || right.nullable
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"
  def nullable = left.nullable || right.nullable
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"
  def nullable = left.nullable || right.nullable
}