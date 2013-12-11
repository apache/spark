package catalyst
package plans
package logical

import expressions.Attribute
import errors._
import trees._

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>

  /**
   * Returns the set of attributes that are referenced by this node
   * during evaluation.
   */
  def references: Set[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: Set[Attribute] = output.toSet

  /**
   * Returns the set of attributes that this node takes as
   * input from its children.
   */
  def inputSet: Set[Attribute] = children.flatMap(_.output).toSet

  def resolve(name: String): Option[Attribute] = {
    val parts = name.split("\\.")
    val options = children.flatMap(_.output).filter {option =>
     // If the first part of the desired name matches a qualifier for this possible match, drop it.
     val remainingParts =
      if(option.qualifiers contains parts.head)
        parts.drop(1)
      else
        parts

      option.name == remainingParts.head
    }

    options match {
      case a :: Nil => Some(a) // One match, use it.
      case Nil => None         // No matches.
      case ambiguousReferences =>
        throw new OptimizationException(
          this, s"Ambiguous references to $name: ${ambiguousReferences.mkString(",")}")
    }
  }
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>

  // Leaf nodes by definition cannot reference any input attributes.
  def references = Set.empty
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