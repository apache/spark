package org.apache.spark.sql
package catalyst
package expressions

import types._

case class ScalaUdf(function: AnyRef, dataType: DataType, children: Seq[Expression])
  extends Expression with ImplementedUdf {

  def references = children.flatMap(_.references).toSet
  def nullable = true

  def evaluate(evaluatedChildren: Seq[Any]): Any = {
    children.size match {
      case 1 => function.asInstanceOf[(Any) => Any](evaluatedChildren(0))
      case 2 => function.asInstanceOf[(Any, Any) => Any](evaluatedChildren(0), evaluatedChildren(1))
    }
  }
}
