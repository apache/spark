package catalyst
package analysis

import expressions._

abstract trait FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression
}

object EmptyRegistry extends FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression = ???
}