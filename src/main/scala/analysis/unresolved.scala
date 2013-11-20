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

  override def toString(): String = s"'$name"
}