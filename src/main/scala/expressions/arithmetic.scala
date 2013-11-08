package catalyst
package expressions

case class Add(left: Expression, right: Expression) extends BinaryExpression {
  def dataType = {
    require(left.dataType == right.dataType) // TODO(marmbrus): Figure out rules for coersions.
    left.dataType
  }

  def nullable = left.nullable || right.nullable

}