package catalyst
package expressions

import types._

object Literal {
  def apply(v: Any): Literal = v match {
    // TODO(marmbrus): Use bigInt type for value?
    case i: Int => Literal(v, IntegerType)
  }
}

object IntegerLiteral {
  def unapply(a: Any): Option[Int] = a match {
    case Literal(a: Int, IntegerType) => Some(a)
    case _ => None
  }
}

case class Literal(value: Any, dataType: DataType) extends LeafExpression {
  def nullable = false
  def references = Set.empty
}