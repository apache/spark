package catalyst
package expressions

import catalyst.analysis.UnresolvedException


case class UnaryMinus(child: Expression) extends UnaryExpression {
  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == right.dataType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "+"
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"
}
