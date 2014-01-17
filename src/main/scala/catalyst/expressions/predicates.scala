package catalyst
package expressions

import types._
import catalyst.analysis.UnresolvedException

trait Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType
}

abstract class BinaryPredicate extends BinaryExpression with Predicate {
  self: Product =>
  def nullable = left.nullable || right.nullable
}

case class Not(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"NOT $child"
}

/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate {
  def children = value +: list
  def references = children.flatMap(_.references).toSet
  def nullable = true // TODO: Figure out correct nullability semantics of IN.
  override def toString = s"$value IN ${list.mkString("(", ",", ")")}"
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "&&"
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "||"
}

abstract class BinaryComparison extends BinaryPredicate {
  self: Product =>
}

case class Equals(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "="
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<"
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<="
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">"
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">="
}

case class IsNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  override def foldable = child.foldable
  def nullable = false
}

case class IsNotNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  override def foldable = child.foldable
  def nullable = false
}

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    extends Expression {

  def children = predicate :: trueValue :: falseValue :: Nil
  def nullable = trueValue.nullable || falseValue.nullable
  def references = children.flatMap(_.references).toSet
  override lazy val resolved = childrenResolved && trueValue.dataType == falseValue.dataType
  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(
        this, s"Invalid types: ${trueValue.dataType}, ${falseValue.dataType}")
    }
    trueValue.dataType
  }
}
