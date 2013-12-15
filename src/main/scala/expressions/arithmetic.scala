package catalyst
package expressions

import catalyst.analysis.UnresolvedException


case class UnaryMinus(child: Expression) extends UnaryExpression {
  def dataType = child.dataType
  def nullable = child.nullable
  override def toString = s"-$child"
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  override lazy val resolved = left.dataType == right.dataType

  def dataType = {
    if(!resolved)
      throw new UnresolvedException(
        this, s"datatype. Can not resolve due to  differing types ${left.dataType}, ${right.dataType}")
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