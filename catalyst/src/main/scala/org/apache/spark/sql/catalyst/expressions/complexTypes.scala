package org.apache.spark.sql
package catalyst
package expressions

import types._

/**
 * Returns the item at `ordinal` in the Array `child` or the Key `ordinal` in Map `child`.
 */
case class GetItem(child: Expression, ordinal: Expression) extends Expression {
  val children = child :: ordinal :: Nil
  /** `Null` is returned for invalid ordinals. */
  override def nullable = true
  override def references = children.flatMap(_.references).toSet
  def dataType = child.dataType match {
    case ArrayType(dt) => dt
    case MapType(_, vt) => vt
  }
  override lazy val resolved =
    childrenResolved &&
    (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType])

  override def toString = s"$child[$ordinal]"
}

/**
 * Returns the value of fields in the Struct `child`.
 */
case class GetField(child: Expression, fieldName: String) extends UnaryExpression {
  def dataType = field.dataType
  def nullable = field.nullable

  protected def structType = child.dataType match {
    case s: StructType => s
    case otherType => sys.error(s"GetField is not valid on fields of type $otherType")
  }

  lazy val field =
    structType.fields
        .find(_.name == fieldName)
        .getOrElse(sys.error(s"No such field $fieldName in ${child.dataType}"))

  lazy val ordinal = structType.fields.indexOf(field)

  override lazy val resolved = childrenResolved && child.dataType.isInstanceOf[StructType]
  override def toString = s"$child.$fieldName"
}
