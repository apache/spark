package catalyst
package analysis

import expressions.{Attribute, Expression}
import plans.logical.LeafNode
import trees.TreeNode

class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.OptimizationException(tree, "Invalid call to $function on unresolved object")

case class UnresolvedRelation(name: String, alias: Option[String]) extends LeafNode {
  def output = throw new UnresolvedException(this, "output")
}

case class UnresolvedAttribute(name: String) extends Attribute with trees.LeafNode[Expression] {
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
}