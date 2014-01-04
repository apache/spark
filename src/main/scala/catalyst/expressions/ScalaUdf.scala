package catalyst
package expressions

import types._

case class ScalaUdf(function: AnyRef, dataType: DataType, children: Seq[Expression])
    extends Expression with ImplementedUdf {

  def references = children.flatMap(_.references).toSet
  def foldable = false
  def nullable = true

  def evaluate(evaluatedChildren: Seq[Any]): Any = {
    children.size match {
      case 1 => function.asInstanceOf[Function1[Any, Any]](evaluatedChildren(0))
      case 2 => function.asInstanceOf[Function2[Any, Any, Any]](evaluatedChildren(0), evaluatedChildren(1))
    }
  }
}