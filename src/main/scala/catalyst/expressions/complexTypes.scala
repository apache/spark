package catalyst
package expressions

import types._

/**
 * Returns the item at `ordinal` in the Array `child`.
 */
case class GetItem(child: Expression, ordinal: Expression) extends Expression {
  val children = child :: ordinal :: Nil
  /** `Null` is returned for invalid ordinals. */
  override def nullable = true
  override def references = children.flatMap(_.references).toSet
  def dataType = child.dataType match {
    case ArrayType(dt) => dt
  }
  override lazy val resolved = childrenResolved && child.dataType.isInstanceOf[ArrayType]
  override def toString = s"$child[$ordinal]"
}

/**
 * Returns the value of fields in the Struct `child`.
 */
case class GetField(child: Expression, fieldName: String) extends UnaryExpression {
  def dataType = field.dataType
  def nullable = field.nullable
  lazy val field = child.dataType match {
    case s: StructType =>
      s.fields
        .find(_.name == fieldName)
        .getOrElse(sys.error(s"No such field $fieldName in ${child.dataType}"))
  }
  override lazy val resolved = childrenResolved && child.dataType.isInstanceOf[StructType]
  override def toString = s"$child.$fieldName"
}