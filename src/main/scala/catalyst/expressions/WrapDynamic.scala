package catalyst
package expressions

import scala.language.dynamics

import types._

case object DynamicType extends DataType

case class WrapDynamic(children: Seq[Attribute]) extends Expression with ImplementedUdf {
  def nullable = false
  def references = children.toSet
  def dataType = DynamicType

  def evaluate(evaluatedChildren: Seq[Any]): Any =
    new DynamicRow(children, evaluatedChildren)
}

class DynamicRow(val schema: Seq[Attribute], values: Seq[Any])
    extends GenericRow(values) with Dynamic {

  def selectDynamic(attributeName: String): String = {
    val ordinal = schema.indexWhere(_.name == attributeName)
    values(ordinal).toString
  }
}
