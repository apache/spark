package org.apache.spark.sql
package catalyst
package expressions

import catalyst.types.BooleanType

case class Like(left: Expression, right: Expression) extends BinaryExpression {
  def dataType = BooleanType
  def nullable = left.nullable // Right cannot be null.
  def symbol = "LIKE"
}

