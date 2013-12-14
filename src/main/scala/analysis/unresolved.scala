package catalyst
package analysis

import expressions.{Attribute, Expression}
import plans.logical.BaseRelation
import trees.TreeNode

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.OptimizationException(tree, s"Invalid call to $function on unresolved object")

/**
 * Holds the name of a relation that has yet to be looked up in a [[Catalog]].
 */
case class UnresolvedRelation(name: String, alias: Option[String]) extends BaseRelation {
  def output = Nil
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(name: String) extends Attribute with trees.LeafNode[Expression] {
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
  def qualifiers = throw new UnresolvedException(this, "qualifiers")
  override lazy val resolved = false

  def withQualifiers(newQualifiers: Seq[String]) = this

  override def toString(): String = s"'$name"
}

case class UnresolvedFunction(name: String, children: Seq[Expression]) extends Expression {
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
  def qualifiers = throw new UnresolvedException(this, "qualifiers")
  def references = children.flatMap(_.references).toSet
  override lazy val resolved = false
  override def toString = s"'$name(${children.mkString(",")})"
}

/**
 * Represents all of the input attributes to a given relational operator, for example in "SELECT * FROM ...".
 *
 * @param table an optional table that should be the target of the expansion.  If omitted all tables' columns are produced.
 */
case class Star(table: Option[String]) extends Attribute with trees.LeafNode[Expression] {
  def name = throw new UnresolvedException(this, "exprId")
  def exprId = throw new UnresolvedException(this, "exprId")
  def dataType = throw new UnresolvedException(this, "dataType")
  def nullable = throw new UnresolvedException(this, "nullable")
  def qualifiers = throw new UnresolvedException(this, "qualifiers")
  override lazy val resolved = false

  def withQualifiers(newQualifiers: Seq[String]) = this

  override def toString = table.map(_ + ".").getOrElse("") + "*"
}