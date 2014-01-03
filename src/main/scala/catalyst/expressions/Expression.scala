package catalyst
package expressions

import trees._
import types._

abstract class Expression extends TreeNode[Expression] {
  self: Product =>

  def dataType: DataType
  def nullable: Boolean
  def references: Set[Attribute]

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema and false if it is
   * still contains any unresolved placeholders.
   */
  lazy val resolved: Boolean = childrenResolved

  def childrenResolved = children.map(_.resolved).reduceLeftOption(_&&_).getOrElse(true)
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>

  def symbol: String

  def references = left.references ++ right.references

  override def toString = s"($left $symbol $right)"
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>
  def references = child.references
}
