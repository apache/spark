package catalyst
package analysis

import expressions.{Attribute, Expression}
import plans.logical.LeafNode
import trees.TreeNode

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.OptimizationException(tree, "Invalid call to $function on unresolved object")

/**
 * Holds the name of a relation that has yet to be looked up in a [[Catalog]].
 */
case class UnresolvedRelation(name: String, alias: Option[String]) extends LeafNode {
  def output = Nil
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(name: String) extends Attribute with trees.LeafNode[Expression] {
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
  def resolved = false

  override def toString(): String = s"'$name"
}

/**
 * Represents all of the input attributes to a given relational operator, for example in "SELECT * FROM ..."
 */
case object Star extends Attribute with trees.LeafNode[Expression] {
  def name = throw new UnresolvedException(this, "exprId")
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
  def resolved = false

  override def toString = "*"
}