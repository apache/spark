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

  def references = Set(this)
  def toAttribute = this
}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1), "a")()
 *
 * @param child the computation being performed
 * @param name
 * @param exprId
 */
case class Alias(child: Expression, name: String)
                (val exprId: ExprId = NamedExpression.newExprId)
  extends NamedExpression with trees.UnaryNode[Expression] {

  def dataType = child.dataType
  def nullable = child.nullable
  def references = child.references

  def toAttribute = AttributeReference(name, child.dataType, child.nullable)(exprId)

  override def toString(): String = s"$child AS $name"
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the same attribute.
 */
case class AttributeReference(name: String, dataType: DataType, nullable: Boolean = true)
                             (val exprId: ExprId = NamedExpression.newExprId)
  extends Attribute with trees.LeafNode[Expression] {

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  def withNullability(newNullability: Boolean) =
    if(nullable == newNullability)
      this
    else
      AttributeReference(name, dataType, newNullability)(exprId)

  override def toString(): String = s"$name#${exprId.id}"
}