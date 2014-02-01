package catalyst
package plans
package logical

import catalyst.expressions._
import catalyst.errors._
import catalyst.types.StructType

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

  /**
   * Optionally resolves the given string to a
   * [[catalyst.expressions.NamedExpression NamedExpression]]. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(name: String): Option[NamedExpression] = {
    val parts = name.split("\\.")
    val options = children.flatMap(_.output).flatMap { option =>
      // If the first part of the desired name matches a qualifier for this possible match, drop it.
      val remainingParts = if (option.qualifiers contains parts.head) parts.drop(1) else parts
      if(option.name == remainingParts.head) (option, remainingParts.tail.toList) :: Nil else Nil
    }

    options.distinct match {
      case (a, Nil) :: Nil => Some(a) // One match, no nested fields, use it.
      // One match, but we also need to extract the requested nested field.
      case (a, nestedFields) :: Nil =>
        a.dataType match {
          case StructType(fields) =>
            Some(Alias(nestedFields.foldLeft(a: Expression)(GetField), nestedFields.last)())
          case _ => None // Don't know how to resolve these field references
        }
      case Nil => None         // No matches.
      case ambiguousReferences =>
        throw new TreeNodeException(
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
