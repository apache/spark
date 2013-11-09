package catalyst
package plans
package logical

import expressions.Attribute
import trees._

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>

  /**
   * Returns the set of attributes that are referenced by this node
   * during evaluation.
   */
  def references: Set[Attribute] = ???

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: Set[Attribute] = ???

  /**
   * Returns the set of attributes that this node takes as
   * input from its children.
   */
  def inputSet: Set[Attribute] = ???
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>
}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan with trees.UnaryNode[LogicalPlan] {
  self: Product =>
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan with trees.BinaryNode[LogicalPlan] {
  self: Product =>
}