package catalyst
package expressions


case class UnaryMinus(child: Expression) extends UnaryExpression {
  def dataType = child.dataType
  def nullable = child.nullable
  override def toString = s"-$child"
}

case class Add(left: Expression, right: Expression) extends BinaryExpression {
  def symbol = "+"

  def dataType = {
    require(left.dataType == right.dataType) // TODO(marmbrus): Figure out rules for coersions.
    left.dataType
  }

  def nullable = left.nullable || right.nullable
}