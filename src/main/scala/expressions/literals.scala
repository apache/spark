package catalyst
package expressions

import types._

object Literal {
  def apply(v: Any): Literal = v match {
    // TODO(marmbrus): Use bigInt type?
    case i: Int => Literal(v, IntegerType)
  }
}

case class Literal(value: Any, dataType: DataType) extends Expression