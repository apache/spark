package catalyst
package expressions

import types.DataType

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"CAST($child, $dataType)"
}
