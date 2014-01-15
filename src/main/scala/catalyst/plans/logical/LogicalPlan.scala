package catalyst
package plans
package logical

import expressions.Attribute
import errors._

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>

  /**
   * Returns the set of attributes that are referenced by this node
   * during evaluation.
   */
  def references: Set[Attribute]

  /**
   * Returns the set of attributes that this node takes as
   * input from its children.
   */
  lazy val inputSet: Set[Attribute] = children.flatMap(_.output).toSet

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it is still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g. [[catalyst.analysis.UnresolvedRelation UnresolvedRelation]] should
   * return `false`).
   */
  lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved = !children.exists(!_.resolved)

  def resolve(name: String): Option[Attribute] = {
    val parts = name.split("\\.")
    val options = children.flatMap(_.output).filter { option =>
      // If the first part of the desired name matches a qualifier for this possible match, drop it.
      val remainingParts = if (option.qualifiers contains parts.head) parts.drop(1) else parts
      option.name == remainingParts.head
    }

    options.distinct match {
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