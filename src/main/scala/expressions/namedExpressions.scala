package catalyst
package expressions

import types._

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  def newExprId = ExprId(curId.getAndIncrement())
}

/**
 * A globally (within this JVM) id for a given named expression.
 * Used to identify with attribute output by a relation is being
 * referenced in a subsuqent computation.
 */
case class ExprId(id: Long)

abstract class NamedExpression extends Expression {
  self: Product =>

  def name: String
  def exprId: ExprId
  def toAttribute: Attribute
}

abstract class Attribute extends NamedExpression {
  self: Product =>

  def toAttribute = this
}

case class Alias(child: Expression, name: String)
                (val exprId: ExprId = NamedExpression.newExprId)
  extends NamedExpression with trees.UnaryNode[Expression] {

  def dataType = child.dataType
  def nullable = child.nullable

  def toAttribute = AttributeReference(name, child.dataType, child.nullable)(exprId)
}

case class AttributeReference(name: String, dataType: DataType, nullable: Boolean = true)
                             (val exprId: ExprId = NamedExpression.newExprId)
  extends Attribute with trees.LeafNode[Expression] {

}