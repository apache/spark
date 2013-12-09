package catalyst
package expressions

import catalyst.analysis.{UnresolvedAttribute, UnresolvedException}
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

  /**
   * Returns true if this attribute has been resolved to a specific input value and false if it is still an unresolved
   * placeholder
   */
  def resolved: Boolean
}

abstract class Attribute extends NamedExpression {
  self: Product =>

  def qualifiers: Seq[String]

  def withQualifiers(newQualifiers: Seq[String]): Attribute

  def references = Set(this)
  def toAttribute = this
}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1), "a")()
 *
 * @param child the computation being performed
 * @param name the name to be associated with the result of computing [[child]].
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this alias.
 *               Auto-assigned if left blank.
 */
case class Alias(child: Expression, name: String)
                (val exprId: ExprId = NamedExpression.newExprId)
  extends NamedExpression with trees.UnaryNode[Expression] {

  def dataType = child.dataType
  def nullable = child.nullable
  def references = child.references

  // An alias is only resolved if all of its children are.
  def resolved = try { child.dataType; true } catch {
    case e: UnresolvedException[_] => false
  }

  def toAttribute =
    if(resolved)
      AttributeReference(name, child.dataType, child.nullable)(exprId)
    else
      UnresolvedAttribute(name)

  override def toString(): String = s"$child AS $name#${exprId.id}"

  override protected final def otherCopyArgs = exprId :: Nil
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the same attribute.
 * @param qualifiers a list of strings that can be used to refered to this attribute in a fully qualified way. Consider
 *                   the examples tableName.name, subQueryAlias.name. tableName and subQueryAlias are possible qualifers.
 */
case class AttributeReference(name: String, dataType: DataType, nullable: Boolean = true)
                             (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends Attribute with trees.LeafNode[Expression] {

  def resolved = true

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  def withNullability(newNullability: Boolean) =
    if(nullable == newNullability)
      this
    else
      AttributeReference(name, dataType, newNullability)(exprId, qualifiers)

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifiers.
   */
  def withQualifiers(newQualifiers: Seq[String]) =
    if(newQualifiers == qualifiers)
      this
    else
      AttributeReference(name, dataType, nullable)(exprId, newQualifiers)

  override def toString(): String = s"$name#${exprId.id}"
}