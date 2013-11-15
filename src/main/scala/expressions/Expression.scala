package catalyst
package expressions

import trees._
import types._

abstract class Expression extends TreeNode[Expression] {
  self: Product =>

  def dataType: DataType
  def nullable: Boolean
  def references: Set[Attribute]
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>
  def references = left.references ++ right.references
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>
  def references = child.references
}
